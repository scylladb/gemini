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
	"encoding/hex"
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
	valuePool               sync.Pool
	session                 *gocql.Session
	oracleSession           func() (*gocql.Session, error)
	testSession             func() (*gocql.Session, error)
	newBatch                func(ctx context.Context) *gocql.Batch
	execBatch               func(ctx context.Context, batch *gocql.Batch) error
	execQuery               func(ctx context.Context, stmt string, args ...any) error
	mutationFragmentsSelect string
	oracleSelect            string
	testSelect              string
	insertStmt              string
	partitionKeys           typedef.Columns
}

type cqlData struct {
	partitionKeys     map[string][]any
	mutationFragments []json.RawMessage
	statements        []json.RawMessage
}

// cqlDataMap is a compact in-memory representation keyed by the 32-byte binary
// hash. It provides a custom JSON marshaler that converts it into a
// map[string]cqlData using hex-encoded keys. This keeps the runtime memory
// footprint small while still enabling easy conversion to a JSON string when
// needed (e.g., for debugging).
type cqlDataMap map[[32]byte]cqlData

// MarshalJSON implements json.Marshaler by copying the map into a
// map[string]cqlData with hex-encoded keys, and marshaling that.
func (m cqlDataMap) MarshalJSON() ([]byte, error) {
	dup := make(map[string]cqlData, len(m))
	for k, v := range m {
		dup[hex.EncodeToString(k[:])] = v
	}
	return json.Marshal(dup)
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

	// MUTATION_FRAGMENTS is a table function; SELECT JSON is not supported on table functions.
	// Build a regular SELECT without JSON for fragments and encode rows ourselves.
	mutationFragmentsSelectBuilder := qb.Select("MUTATION_FRAGMENTS(" + originalKeyspace + "." + originalTable + ")")
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
		newBatch: func(ctx context.Context) *gocql.Batch {
			return session.BatchWithContext(ctx, gocql.UnloggedBatch)
		},
		execBatch: func(_ context.Context, batch *gocql.Batch) error {
			return session.ExecuteBatch(batch)
		},
		execQuery: func(ctx context.Context, stmt string, args ...any) error {
			q := session.QueryWithContext(ctx, stmt, args...)
			defer q.Release()
			return q.Exec()
		},
	}, nil
}

func (c *cqlStatements) Insert(ctx context.Context, item stmtlogger.Item) error {
	valuesPtr := c.buildArgs(item)
	defer c.releaseArgs(valuesPtr)

	q := c.session.QueryWithContext(ctx, c.insertStmt, *valuesPtr...)
	defer q.Release()

	if err := q.Exec(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	return nil
}

func (c *cqlStatements) buildArgs(item stmtlogger.Item) *[]any {
	valuesPtr := c.valuePool.Get().(*[]any)
	// reset capacity but keep underlying storage
	*valuesPtr = (*valuesPtr)[:0]
	*valuesPtr = c.fillArgs(*valuesPtr, item)
	return valuesPtr
}

// releaseArgs returns a previously borrowed args slice to the pool.
func (c *cqlStatements) releaseArgs(valuesPtr *[]any) {
	*valuesPtr = (*valuesPtr)[:0]
	c.valuePool.Put(valuesPtr)
}

// argsCap returns the number of elements we will bind for an insert.
func (c *cqlStatements) argsCap() int {
	return c.partitionKeys.LenValues() + len(additionalColumnsArr)
}

// fillArgs fills dst with the arguments for the provided item and returns the
// resulting slice. dst is truncated to length 0 but capacity is preserved.
func (c *cqlStatements) fillArgs(dst []any, item stmtlogger.Item) []any {
	// truncate to zero length, keep capacity
	dst = dst[:0]

	var itemErr string
	if item.Error.IsLeft() {
		if errVal := item.Error.MustLeft(); errVal != nil {
			itemErr = errVal.Error()
		}
	} else {
		itemErr = item.Error.MustRight()
	}

	dst = append(dst, item.PartitionKeys.ToCQLValues(c.partitionKeys)...)
	dst = append(dst,
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

	return dst
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

		bs, err := encodeRowToJSON(row)
		if err != nil {
			// If JSON marshal fails for a row, skip it but continue scanning
			continue
		}
		data = append(data, bs)
	}

	return data, iter.Close()
}

// encodeRowToJSON encodes a row scanned from Scylla into JSON using Go's encoding/json.
// This locks in the expected formatting of certain types:
//   - time.Time values are encoded as RFC3339Nano strings
//   - []byte values are encoded as base64 strings
//   - numeric values are encoded as JSON numbers
//
// The returned slice is safe to store as json.RawMessage.
func encodeRowToJSON(row map[string]any) (json.RawMessage, error) { //nolint:unused // used in tests via same package
	bs, err := json.Marshal(row)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(bs), nil
}

func (c *cqlStatements) Fetch(ctx context.Context, ty stmtlogger.Type, item *joberror.JobError) (cqlDataMap, error) {
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
		return cqlDataMap{
			item.Hash(): {
				mutationFragments: fragments,
				statements:        statements,
				partitionKeys:     item.PartitionKeys.ToMap(),
			},
		}, nil
	case typedef.SelectMultiPartitionType, typedef.SelectMultiPartitionRangeStatementType,
		typedef.DeleteMultiplePartitionsType:
		iterations := len(item.PartitionKeys.Get(c.partitionKeys[0].Name))
		result := make(cqlDataMap, iterations)
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
