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
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/samber/mo"
	"github.com/scylladb/gocqlx/v3/qb"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
)

type cqlStatements struct {
	valuePool               sync.Pool
	session                 *gocql.Session
	oracleSession           func() (*gocql.Session, error)
	testSession             func() (*gocql.Session, error)
	mutationFragmentsSelect string
	oracleSelect            string
	testSelect              string
	insertStmt              string
	partitionKeys           typedef.Columns
}

// One _logs row holds one execution of a statement on both clusters. The
// shared columns come first, then the columns of each cluster. A cluster that
// gemini did not observe for the execution leaves its columns null.
const (
	additionalColumns = "ts,seq,statement,values,attempt,gemini_attempt," +
		"test_ts,test_host,test_error,test_dur," +
		"oracle_ts,oracle_host,oracle_error,oracle_dur"
)

var additionalColumnsArr = strings.Split(additionalColumns, ",")

// testColumns and oracleColumns read one cluster's columns under the names a
// single-cluster row used, so the statements files keep their shape: ts, host,
// error and dur belong to that cluster.
const (
	testColumns = "test_ts AS ts,seq,statement,values,test_host AS host,attempt,gemini_attempt," +
		"test_error AS error,test_dur AS dur"
	oracleColumns = "oracle_ts AS ts,seq,statement,values,oracle_host AS host,attempt,gemini_attempt," +
		"oracle_error AS error,oracle_dur AS dur"
)

// ts was a CQL timestamp (millisecond resolution): concurrent statements on one
// partition shared a primary key and overwrote each other (QATOOLS-318).
func logTS(start time.Time) int64 {
	return start.UTC().UnixNano()
}

var logSeq atomic.Int64

// Bound at args-build time, so a batch retry or row-by-row fallback re-sends the
// same seq instead of adding a duplicate row.
func nextLogSeq() int64 {
	return logSeq.Add(1)
}

// newStatements creates the _logs keyspace (idempotently) and the table, then
// builds the per-table cqlStatements. It is the self-contained entry point for
// callers that set up a single table in isolation (e.g. tests). New uses
// newTableStatements directly after creating the shared keyspace once, to avoid
// re-issuing the keyspace DDL for every table in a multi-table schema.
func newStatements(
	session *gocql.Session,
	oracleSession, testSession func() (*gocql.Session, error),
	keyspace, table string,
	originalKeyspace, originalTable string,
	partitionKeys typedef.Columns,
	replication replication.Replication,
) (*cqlStatements, error) {
	if err := session.Query(buildCreateKeyspaceQuery(keyspace, replication)).Exec(); err != nil {
		return nil, err
	}

	return newTableStatements(session, oracleSession, testSession,
		keyspace, table, originalKeyspace, originalTable, partitionKeys, replication)
}

// newTableStatements creates the per-table _logs table and builds its
// cqlStatements. The _logs keyspace must already exist (created by New once, or
// by newStatements for single-table callers).
func newTableStatements(
	session *gocql.Session,
	oracleSession, testSession func() (*gocql.Session, error),
	keyspace, table string,
	originalKeyspace, originalTable string,
	partitionKeys typedef.Columns,
	replication replication.Replication,
) (*cqlStatements, error) {
	_, createTable := buildCreateTableQuery(keyspace, table, partitionKeys, replication)

	if err := session.Query(createTable).Exec(); err != nil {
		return nil, err
	}

	// MUTATION_FRAGMENTS is a table function; SELECT JSON is not supported on table functions.
	// Build a regular SELECT without JSON for fragments and encode rows ourselves.
	mutationFragmentsSelectBuilder := qb.Select("MUTATION_FRAGMENTS(" + originalKeyspace + "." + originalTable + ")")
	insertBuilder := qb.Insert(keyspace + "." + table)

	// Add partition key columns first to match the order of values in Insert()
	where := make([]string, 0, len(partitionKeys))
	for _, col := range partitionKeys {
		where = append(where, col.Name+"=?")
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

	// The rows of a partition come back in clustering order, so ts ascends.
	// gocqlx has no column aliases, so the per-cluster SELECT is built here.
	pkNames := strings.Join(partitionKeys.Names(), ",")
	from := " FROM " + keyspace + "." + table + " WHERE " + strings.Join(where, " AND ")
	oracleSelect := "SELECT JSON " + pkNames + "," + oracleColumns + from
	testSelect := "SELECT JSON " + pkNames + "," + testColumns + from
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
	valuesPtr, ok := c.buildArgs(item)
	defer c.releaseArgs(valuesPtr)

	if !ok {
		metrics.StatementLoggerMalformedTotal.Inc()
		return nil
	}

	q := c.session.QueryWithContext(ctx, c.insertStmt, *valuesPtr...)
	defer q.Release()

	if err := q.Exec(); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}

	return nil
}

func (c *cqlStatements) buildArgs(item stmtlogger.Item) (*[]any, bool) {
	valuesPtr, pooled := c.valuePool.Get().(*[]any)
	if !pooled {
		valuesPtr = new([]any)
	}

	// reset capacity but keep underlying storage
	*valuesPtr = (*valuesPtr)[:0]
	var ok bool
	*valuesPtr, ok = c.fillArgs(*valuesPtr, item)
	return valuesPtr, ok
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

// fillArgs fills dst with the arguments of a row that holds item alone, as
// fillPairArgs does for a pair.
func (c *cqlStatements) fillArgs(dst []any, item stmtlogger.Item) ([]any, bool) {
	return c.fillPairArgs(dst, pairOf(item))
}

// fillPairArgs fills dst with the arguments for one execution on both clusters
// and returns the resulting slice. dst is truncated to length 0 but capacity is
// preserved. The columns of a cluster that pair does not hold are bound as null.
//
// The second return is false when the pair is malformed — specifically when the
// number of partition-key values it carries does not match the _logs INSERT
// arity (c.partitionKeys.LenValues()). Such a row, if sent, makes gocql reject
// the whole batch client-side with "expected N values got M". Callers MUST drop
// it (and count metrics.StatementLoggerMalformedTotal) rather than enqueue it,
// otherwise a single buggy generator floods errors and triggers the row-by-row
// fallback storm that previously OOM-killed the loader.
func (c *cqlStatements) fillPairArgs(dst []any, pair logPair) ([]any, bool) {
	// truncate to zero length, keep capacity
	dst = dst[:0]

	shared := pair.shared()

	// Append the partition-key values straight into the pooled dst (no throwaway
	// intermediate slice) and validate the arity by the number appended.
	dst = shared.PartitionKeys.Values.AppendCQLValues(dst, c.partitionKeys)
	if len(dst) != c.partitionKeys.LenValues() {
		return dst[:0], false
	}

	dst = append(dst,
		logTS(pair.start()),
		nextLogSeq(),
		shared.Statement,
		encodeLogValues(shared.Values),
		shared.Attempt,
		shared.GeminiAttempt,
	)
	dst = appendSideArgs(dst, pair.test, pair.hasTest)
	dst = appendSideArgs(dst, pair.oracle, pair.hasOracle)

	return dst, true
}

// appendSideArgs appends one cluster's ts, host, error and dur, or four nulls
// when the pair holds no item for that cluster.
func appendSideArgs(dst []any, item stmtlogger.Item, present bool) []any {
	if !present {
		return append(dst, nil, nil, nil, nil)
	}

	var itemErr string
	if item.Error.IsLeft() {
		if errVal := item.Error.MustLeft(); errVal != nil {
			itemErr = errVal.Error()
		}
	} else {
		itemErr = item.Error.MustRight()
	}

	return append(dst, logTS(item.Start.Time), item.Host, itemErr, item.Duration.Duration)
}

// streamStatements copies every statement row of one partition into e. A
// partition can hold millions of rows, so rows go to the sink one at a time and
// are never collected. NumRows would bound the scan to the first page: Scan
// pages on its own.
func streamStatements(ctx context.Context, e *lineEncoder, session *gocql.Session, stmt string, values []any) error {
	q := session.QueryWithContext(ctx, stmt, values...)
	defer q.Release()

	iter := q.Iter()

	e.array("statements")
	first := true

	// e.failed() stops the scan: once the file rejects a byte nothing later can
	// reach it, and a hot partition would otherwise page millions of rows into a
	// writer that drops them, all while holding the sink.
	for !e.failed() {
		var it jsontext.Value
		if !iter.Scan(&it) {
			break
		}

		// A row holds both clusters. This cluster's file skips the rows that
		// hold only the other cluster: their host column reads as null.
		if !sideObserved(it) {
			continue
		}

		e.row(first, it)
		first = false
	}

	e.endArray(true)

	return iter.Close()
}

// sideObserved reports whether a per-cluster JSON row holds an observation of
// that cluster. A missing cluster leaves host null.
func sideObserved(row jsontext.Value) bool {
	var side struct {
		Host *string `json:"host"`
	}
	if err := json.Unmarshal(row, &side); err != nil {
		// Keep a row that cannot be read rather than hide a statement.
		return true
	}

	return side.Host != nil
}

func streamFragments(ctx context.Context, e *lineEncoder, session *gocql.Session, stmt string, values []any) error {
	q := session.QueryWithContext(ctx, stmt, values...)
	defer q.Release()

	iter := q.Iter()

	e.array("mutationFragments")
	first := true

	for !e.failed() {
		row := make(map[string]any, 10)
		if !iter.MapScan(row) {
			break
		}

		bs, err := encodeRowToJSON(row)
		if err != nil {
			// If JSON marshal fails for a row, skip it but continue scanning
			continue
		}

		e.row(first, bs)
		first = false
	}

	e.endArray(false)

	return iter.Close()
}

// encodeRowToJSON encodes a row scanned from Scylla into JSON using Go's encoding/json.
// This locks in the expected formatting of certain types:
//   - time.Time values are encoded as RFC3339Nano strings
//   - []byte values are encoded as base64 strings
//   - numeric values are encoded as JSON numbers
//
// The returned slice is safe to store as jsontext.Value.
func encodeRowToJSON(row map[string]any) (jsontext.Value, error) {
	bs, err := json.Marshal(row, json.Deterministic(true))
	if err != nil {
		return nil, err
	}
	return jsontext.Value(bs), nil
}

// FetchTo streams the statement history of a job error's partitions to w as
// JSONL, one line per partition. Rows go from the CQL iterator into w one at a
// time: a hot partition's history is far too big to assemble in memory now that
// _logs keeps every statement instead of one per millisecond.
//
// It returns the number of bytes written.
func (c *cqlStatements) FetchTo(ctx context.Context, ty stmtlogger.Type, item *joberror.JobError, w io.Writer) (int64, error) {
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
		return 0, newReadError(err)
	}

	switch item.StmtType {
	case typedef.SelectStatementType, typedef.SelectRangeStatementType,
		typedef.InsertStatementType, typedef.InsertJSONStatementType,
		typedef.UpdateStatementType, typedef.DeleteWholePartitionType,
		typedef.DeleteSingleRowType, typedef.DeleteClusteringSubsetType:
		values := item.PartitionKeys.ToCQLValues(c.partitionKeys)

		n, writeErr, readErr := c.streamLine(ctx, session, stmt, w, item, item.PartitionKeys.ToMap(), values)
		if writeErr != nil {
			return n, writeErr
		}

		return n, newReadError(readErr)
	case typedef.SelectMultiPartitionType, typedef.SelectMultiPartitionRangeStatementType,
		typedef.DeleteMultiplePartitionsType:
		var (
			written  int64
			readErrs []error
		)

		for i := range len(item.PartitionKeys.Get(c.partitionKeys[0].Name)) {
			values := make([]any, 0, c.partitionKeys.LenValues())
			pks := make(map[string][]any, len(c.partitionKeys))

			for _, pk := range c.partitionKeys {
				values = append(values, item.PartitionKeys.Get(pk.Name)[i])
				pks[pk.Name] = append(pks[pk.Name], item.PartitionKeys.Get(pk.Name)[i])
			}

			n, writeErr, readErr := c.streamLine(ctx, session, stmt, w, item, pks, values)
			written += n

			// A failed read costs one partition its content. A failed write means
			// the file is no longer trustworthy, so it stops everything.
			if writeErr != nil {
				return written, writeErr
			}

			if readErr != nil {
				readErrs = append(readErrs, readErr)
			}
		}

		return written, newReadError(readErrs...)
	case typedef.SelectByIndexStatementType, typedef.SelectFromMaterializedViewStatementType:
		return 0, newReadError(errors.New("select by index or materialized view is not supported, skipping job error"))
	default:
		return 0, nil
	}
}

// streamLine writes one JSONL line for a single partition and reports the write
// error and the read error apart. A read error still leaves a syntactically
// complete line behind, so one unreadable partition cannot corrupt the file for
// the partitions that follow.
func (c *cqlStatements) streamLine(
	ctx context.Context,
	fragmentSession *gocql.Session,
	stmt string,
	w io.Writer,
	item *joberror.JobError,
	partitionKeys map[string][]any,
	values []any,
) (n int64, writeErr, readErr error) {
	var errStr string
	if item.Err != nil {
		errStr = item.Err.Error()
	}

	e := newLineEncoder(w)
	e.head(reorganizePartitionKeys(partitionKeys, item), item.Timestamp, errStr, item.Query, item.Message)

	fragmentsErr := streamFragments(ctx, e, fragmentSession, c.mutationFragmentsSelect, values)

	// The second query only pays off if its rows can still be written. After a
	// write failure the line is already lost, so reading the statement history
	// would only load the cluster and delay shutdown.
	var statementsErr error
	if !e.failed() {
		statementsErr = streamStatements(ctx, e, c.session, stmt, values)
	}

	// Join, not first-wins: when both reads fail, each names a different query.
	readErr = errors.Join(statementsErr, fragmentsErr)

	e.end(readErr != nil)

	n, writeErr = e.Close()

	return n, writeErr, readErr
}

// buildCreateKeyspaceQuery builds the CQL that creates the shared _logs keyspace.
// New creates it once up front; the per-table newTableStatements calls then only
// create their own tables, so the keyspace DDL is not re-issued per table.
func buildCreateKeyspaceQuery(keyspace string, replication replication.Replication) string {
	return fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH replication=%s AND durable_writes = true;",
		keyspace, replication.ToCQL(),
	)
}

func buildCreateTableQuery(
	keyspace string,
	table string,
	partitionKeys typedef.Columns,
	replication replication.Replication,
) (string, string) {
	createKeyspace := buildCreateKeyspaceQuery(keyspace, replication)

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
		// ts, test_ts and oracle_ts are UTC nanoseconds, not CQL timestamps. ts
		// is the earlier start of the two clusters.
		"ts bigint, seq bigint, statement text, values frozen<list<text>>, attempt smallint, gemini_attempt smallint, " +
			"test_ts bigint, test_host text, test_error text, test_dur duration, " +
			"oracle_ts bigint, oracle_host text, oracle_error text, oracle_dur duration, ",
	)
	builder.WriteString("PRIMARY KEY ((")
	builder.WriteString(partitions)
	builder.WriteString("), ts, attempt, gemini_attempt, seq)) WITH caching={'enabled':'true'} AND compression={'sstable_compression':'ZstdCompressor'}")
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
		result[i] = formatValueFast(val)
	}

	return result
}

// formatValueFast converts a value to string without reflection.
// Handles the common CQL types directly instead of using fmt.Sprintf("%#v").
func formatValueFast(val any) string {
	return string(appendValueFast(nil, val))
}
