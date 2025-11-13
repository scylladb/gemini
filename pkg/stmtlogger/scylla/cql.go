// Copyright 2025 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scylla

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/gocql/gocql"
	"github.com/samber/mo"
	"github.com/scylladb/gocqlx/v3/qb"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
)

type cqlStatements struct {
	mutationFragmentsSelect string
	oracleSelect            string
	testSelect              string
	insertStmt              string

	session       *gocql.Session
	oracleSession func() (*gocql.Session, error)
	testSession   func() (*gocql.Session, error)

	valuePool     sync.Pool
	partitionKeys typedef.Columns
}

type cqlData struct {
	partitionKeys     map[string][]any
	mutationFragments []json.RawMessage
	statements        []json.RawMessage
}

const (
	additionalColumns = "ts,ty,statement,values,host,attempt,gemini_attempt,error,dur"
)

var additionalColumnsArr = strings.Split(additionalColumns, ",")

func newStatements(
	session *gocql.Session,
	oracleSession, testSession func() (*gocql.Session, error),
	keyspace, table string,
	originalKeyspace, originalTable string,
	partitionKeys typedef.Columns,
	replication replication.Replication,
) (*cqlStatements, error) {
	createKeyspace, createTable := buildCreateTableQuery(keyspace, table, partitionKeys, replication)

	if err := session.Query(createKeyspace).Exec(); err != nil {
		return nil, err
	}

	if err := session.Query(createTable).Exec(); err != nil {
		return nil, err
	}

	mutationFragmentsSelectBuilder := qb.Select("MUTATION_FRAGMENTS(" + originalKeyspace + "." + originalTable + ")").
		Json()
	oracleSelectBuilder := qb.Select(keyspace+"."+table).
		Json().
		Where(qb.EqLit("ty", "'oracle'")).
		OrderBy("ts", qb.ASC)
	testSelectBuilder := qb.Select(keyspace+"."+table).
		Json().
		Where(qb.EqLit("ty", "'test'")).
		OrderBy("ts", qb.ASC)

	insertBuilder := qb.Insert(keyspace + "." + table)

	// Add partition key columns first to match the order of values in Insert()
	for _, col := range partitionKeys {
		oracleSelectBuilder.Where(qb.Eq(col.Name))
		testSelectBuilder.Where(qb.Eq(col.Name))
		mutationFragmentsSelectBuilder.Where(qb.Eq(col.Name))
		switch colType := col.Type.(type) {
		case *typedef.TupleType:
			insertBuilder.TupleColumn(col.Name, len(colType.ValueTypes))
		default:
			insertBuilder.Columns(col.Name)
		}
	}

	// Then add additional columns
	insertBuilder.Columns(additionalColumnsArr...)

	oracleSelect, _ := oracleSelectBuilder.ToCql()
	testSelect, _ := testSelectBuilder.ToCql()
	oracleInsert, _ := insertBuilder.ToCql()
	mutationFragmentsSelect, _ := mutationFragmentsSelectBuilder.ToCql()

	return &cqlStatements{
		oracleSelect:            oracleSelect,
		mutationFragmentsSelect: mutationFragmentsSelect,
		testSelect:              testSelect,
		insertStmt:              oracleInsert,
		session:                 session,
		oracleSession:           oracleSession,
		testSession:             testSession,
		valuePool: sync.Pool{
			New: func() any {
				slice := make([]any, 0, partitionKeys.LenValues()+len(additionalColumnsArr))
				return &slice
			},
		},
		partitionKeys: partitionKeys,
	}, nil
}

func (c *cqlStatements) Insert(ctx context.Context, item stmtlogger.Item) error {
	var itemErr string
	if item.Error.IsLeft() {
		if errVal := item.Error.MustLeft(); errVal != nil {
			itemErr = errVal.Error()
		}
	} else {
		itemErr = item.Error.MustRight()
	}

	valuesPtr := c.valuePool.Get().(*[]any)
	defer func() {
		*valuesPtr = (*valuesPtr)[:0]
		c.valuePool.Put(valuesPtr)
	}()

	*valuesPtr = append(*valuesPtr, item.PartitionKeys.ToCQLValues(c.partitionKeys)...)
	*valuesPtr = append(*valuesPtr,
		item.Start.Time,
		item.Type,
		item.Statement,
		prepareValuesOptimized(item.Values),
		item.Host,
		item.Attempt,
		item.GeminiAttempt,
		itemErr,
		item.Duration.Duration,
	)

	q := c.session.QueryWithContext(ctx, c.insertStmt, *valuesPtr...)
	defer q.Release()

	if err := q.Exec(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	return nil
}

func fetchPartitionKeys(ctx context.Context, session *gocql.Session, stmt string, values []any) ([]json.RawMessage, error) {
	q := session.QueryWithContext(ctx, stmt, values...)
	defer q.Release()

	iter := q.Iter()
	data := make([]json.RawMessage, 0, iter.NumRows())

	for range iter.NumRows() {
		var it json.RawMessage
		if !iter.Scan(&it) {
			break
		}

		data = append(data, it)
	}

	return data, iter.Close()
}

func fetchFragments(ctx context.Context, session *gocql.Session, stmt string, values []any) ([]json.RawMessage, error) {
	q := session.QueryWithContext(ctx, stmt, values...)
	defer q.Release()

	data := make([]json.RawMessage, 0)
	iter := q.Iter()

	for range iter.NumRows() {
		row := make(map[string]any, 10)
		if !iter.MapScan(row) {
			break
		}

		bs, _ := json.Marshal(row)
		data = append(data, bs)
	}

	return data, iter.Close()
}

func (c *cqlStatements) Fetch(ctx context.Context, ty stmtlogger.Type, item *joberror.JobError) (map[[32]byte]cqlData, error) {
	var (
		stmt    string
		session *gocql.Session
		err     error
	)

	switch ty {
	case stmtlogger.TypeOracle:
		stmt = c.oracleSelect
		session, err = c.oracleSession()
	case stmtlogger.TypeTest:
		stmt = c.testSelect
		session, err = c.testSession()
	default:
		panic("Invalid type for Error: " + string(ty))
	}

	if err != nil {
		return nil, err
	}

	switch item.StmtType {
	case typedef.SelectStatementType, typedef.SelectRangeStatementType,
		typedef.InsertStatementType, typedef.InsertJSONStatementType,
		typedef.UpdateStatementType, typedef.DeleteWholePartitionType,
		typedef.DeleteSingleRowType, typedef.DeleteSingleColumnType:
		//nolint:govet
		statements, err := fetchPartitionKeys(ctx, c.session, stmt, item.PartitionKeys.ToCQLValues(c.partitionKeys))
		if err != nil {
			return nil, err
		}
		fragments, err := fetchFragments(ctx, session, c.mutationFragmentsSelect, item.PartitionKeys.ToCQLValues(c.partitionKeys))
		if err != nil {
			return nil, err
		}

		return map[[32]byte]cqlData{
			item.Hash(): {
				mutationFragments: fragments,
				statements:        statements,
				partitionKeys:     item.PartitionKeys.ToMap(),
			},
		}, nil
	case typedef.SelectMultiPartitionType, typedef.SelectMultiPartitionRangeStatementType,
		typedef.DeleteMultiplePartitionsType:
		iterations := len(item.PartitionKeys.Get(c.partitionKeys[0].Name))
		result := make(map[[32]byte]cqlData, iterations)
		for i := range iterations {
			values := make([]any, 0, c.partitionKeys.LenValues())
			pks := make(map[string][]any, len(c.partitionKeys))
			for _, pk := range c.partitionKeys {
				values = append(values, item.PartitionKeys.Get(pk.Name)[i])
				pks[pk.Name] = append(pks[pk.Name], item.PartitionKeys.Get(pk.Name)[i])
			}

			//nolint:govet
			statements, err := fetchPartitionKeys(ctx, c.session, stmt, values)
			if err != nil {
				// Log the error
				continue
			}

			fragments, err := fetchFragments(ctx, session, c.mutationFragmentsSelect, values)
			if err != nil {
				continue
			}

			// Create a job error for this specific partition to get the correct hash
			partitionJobErr := &joberror.JobError{
				Timestamp:     item.Timestamp,
				Query:         item.Query,
				Message:       item.Message,
				StmtType:      item.StmtType,
				PartitionKeys: typedef.NewValuesFromMap(pks),
			}

			result[partitionJobErr.Hash()] = cqlData{
				statements:        statements,
				mutationFragments: fragments,
				partitionKeys:     pks,
			}
		}

		return result, nil
	case typedef.SelectByIndexStatementType, typedef.SelectFromMaterializedViewStatementType:
		return nil, errors.New("select by index or materialized view is not supported, skipping job error")
	default:
		return nil, nil
	}
}

func buildCreateTableQuery(
	keyspace string,
	table string,
	partitionKeys typedef.Columns,
	replication replication.Replication,
) (string, string) {
	createKeyspace := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH replication=%s AND durable_writes = true;",
		keyspace, replication.ToCQL(),
	)

	var builder bytes.Buffer

	partitions := strings.Join(partitionKeys.Names(), ",")

	builder.WriteString("CREATE TABLE IF NOT EXISTS ")
	builder.WriteString(keyspace)
	builder.WriteRune('.')
	builder.WriteString(table)
	builder.WriteString("(")

	for _, col := range partitionKeys {
		builder.WriteString(col.Name)
		builder.WriteString(" ")
		builder.WriteString(col.Type.CQLDef())
		builder.WriteRune(',')
	}

	builder.WriteString(
		"ts timestamp, ty text, statement text, values frozen<list<text>>, host text, attempt smallint, gemini_attempt smallint, error text, dur duration, ",
	)
	builder.WriteString("PRIMARY KEY ((")
	builder.WriteString(partitions)
	builder.WriteString(", ty), ts, attempt, gemini_attempt)) WITH caching={'enabled':'true'} AND compression={'sstable_compression':'ZstdCompressor'}")
	builder.WriteString(" AND tombstone_gc={'mode':'immediate'} AND comment='Table to store logs from Oracle and Test statements';")

	return createKeyspace, builder.String()
}

func prepareValuesOptimized(values mo.Either[[]any, []byte]) []string {
	if values.IsRight() {
		return []string{string(values.MustRight())}
	}

	valSlice := values.MustLeft()
	if valSlice == nil {
		return nil
	}

	result := make([]string, len(valSlice))

	for i, val := range valSlice {
		result[i] = fmt.Sprintf("%#v", val)
	}

	return result
}
