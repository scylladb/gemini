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
	"context"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
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
	queries                 *queryDict
	mutationFragmentsSelect string
	selectStmt              string
	insertStmt              string
	queriesInsert           string
	queriesSelect           string
	partitionKeys           typedef.Columns
}

// One _logs row holds one execution of a statement on both clusters. The
// partition key of the source table comes first, then the clustering columns
// that order and separate the executions of one partition, then the data cell
// (see logData) with everything else.
var logColumns = []string{"ts", "attempt", "gemini_attempt", "seq", "data"}

// queriesTable holds the CQL text of every query id in a _logs keyspace.
const queriesTable = "queries"

// logTableOptions keep the write cost of the _logs tables low on the cluster
// that holds them, which is the oracle and the pace setter of the whole run.
// LZ4 compresses with far less CPU than Zstd. Gemini reads a partition back
// only when a validation fails, so the row cache only takes memory.
const logTableOptions = " WITH caching={'enabled':'false'}" +
	" AND compression={'sstable_compression':'LZ4Compressor'}" +
	" AND tombstone_gc={'mode':'immediate'}"

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

// newStatements creates the _logs keyspace (idempotently), its queries table
// and the table, then builds the per-table cqlStatements. It is the
// self-contained entry point for callers that set up a single table in
// isolation (e.g. tests). New uses newTableStatements directly after creating
// the shared keyspace once, to avoid re-issuing the keyspace DDL for every
// table in a multi-table schema.
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

	if err := session.Query(buildCreateQueriesTableQuery(keyspace)).Exec(); err != nil {
		return nil, err
	}

	return newTableStatements(session, oracleSession, testSession,
		keyspace, table, originalKeyspace, originalTable, partitionKeys, replication)
}

// newTableStatements creates the per-table _logs table and builds its
// cqlStatements. The _logs keyspace and its queries table must already exist
// (created by New once, or by newStatements for single-table callers).
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
	selectBuilder := qb.Select(keyspace + "." + table).Columns(logColumns...)

	// Add partition key columns first to match the order of values in Insert()
	for _, col := range partitionKeys {
		mutationFragmentsSelectBuilder.Where(qb.Eq(col.Name))
		selectBuilder.Where(qb.Eq(col.Name))
		switch colType := col.Type.(type) {
		case *typedef.TupleType:
			insertBuilder.TupleColumn(col.Name, len(colType.ValueTypes))
		default:
			insertBuilder.Columns(col.Name)
		}
	}

	insertBuilder.Columns(logColumns...)

	// The rows of a partition come back in clustering order, so ts ascends.
	selectStmt, _ := selectBuilder.ToCql()
	insertStmt, _ := insertBuilder.ToCql()
	mutationFragmentsSelect, _ := mutationFragmentsSelectBuilder.ToCql()

	return &cqlStatements{
		selectStmt:              selectStmt,
		mutationFragmentsSelect: mutationFragmentsSelect,
		insertStmt:              insertStmt,
		queriesInsert:           "INSERT INTO " + keyspace + "." + queriesTable + " (id, statement) VALUES (?, ?)",
		queriesSelect:           "SELECT statement FROM " + keyspace + "." + queriesTable + " WHERE id = ?",
		session:                 session,
		oracleSession:           oracleSession,
		testSession:             testSession,
		valuePool: sync.Pool{
			New: func() any {
				slice := make([]any, 0, partitionKeys.LenValues()+len(logColumns))
				return &slice
			},
		},
		partitionKeys: partitionKeys,
	}, nil
}

// Insert writes one row that holds item alone, and the queries row of its
// statement when this process has not written it yet.
func (c *cqlStatements) Insert(ctx context.Context, item stmtlogger.Item) error {
	id := queryID(item.Statement)

	if c.queries == nil || c.queries.remember(id, item.Statement) {
		if err := c.session.QueryWithContext(ctx, c.queriesInsert, id, item.Statement).Exec(); err != nil &&
			!errors.Is(err, context.Canceled) {
			return err
		}
	}

	valuesPtr, ok := c.buildArgs(item, id)
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

func (c *cqlStatements) buildArgs(item stmtlogger.Item, id int64) (*[]any, bool) {
	valuesPtr, pooled := c.valuePool.Get().(*[]any)
	if !pooled {
		valuesPtr = new([]any)
	}

	// reset capacity but keep underlying storage
	*valuesPtr = (*valuesPtr)[:0]
	var ok bool
	*valuesPtr, ok = c.fillPairArgs(*valuesPtr, pairOf(item), id)
	return valuesPtr, ok
}

// releaseArgs returns a previously borrowed args slice to the pool.
func (c *cqlStatements) releaseArgs(valuesPtr *[]any) {
	*valuesPtr = (*valuesPtr)[:0]
	c.valuePool.Put(valuesPtr)
}

// argsCap returns the number of elements we will bind for an insert.
func (c *cqlStatements) argsCap() int {
	return c.partitionKeys.LenValues() + len(logColumns)
}

// fillArgs fills dst with the arguments of a row that holds item alone, as
// fillPairArgs does for a pair.
func (c *cqlStatements) fillArgs(dst []any, item stmtlogger.Item) ([]any, bool) {
	return c.fillPairArgs(dst, pairOf(item), queryID(item.Statement))
}

// fillPairArgs fills dst with the arguments for one execution on both clusters
// and returns the resulting slice. dst is truncated to length 0 but capacity is
// preserved. id is the query id of the statement of the pair.
//
// The second return is false when the pair is malformed — specifically when the
// number of partition-key values it carries does not match the _logs INSERT
// arity (c.partitionKeys.LenValues()). Such a row, if sent, makes gocql reject
// the whole batch client-side with "expected N values got M". Callers MUST drop
// it (and count metrics.StatementLoggerMalformedTotal) rather than enqueue it,
// otherwise a single buggy generator floods errors and triggers the row-by-row
// fallback storm that previously OOM-killed the loader.
func (c *cqlStatements) fillPairArgs(dst []any, pair logPair, id int64) ([]any, bool) {
	// truncate to zero length, keep capacity
	dst = dst[:0]

	shared := pair.shared()

	// Append the partition-key values straight into the pooled dst (no throwaway
	// intermediate slice) and validate the arity by the number appended.
	dst = shared.PartitionKeys.Values.AppendCQLValues(dst, c.partitionKeys)
	if len(dst) != c.partitionKeys.LenValues() {
		return dst[:0], false
	}

	return append(dst,
		logTS(pair.start()),
		int16(shared.Attempt),
		int16(shared.GeminiAttempt),
		nextLogSeq(),
		encodeLogData(pair, id),
	), true
}

// logSide is the observation of one cluster in the data cell.
type logSide struct {
	Error    string `json:"e"`
	Host     string `json:"h"`
	TS       int64  `json:"ts"`
	Duration int64  `json:"d"`
}

// logRecord is the data cell of a _logs row as the fetch path reads it.
type logRecord struct {
	Test   *logSide       `json:"t"`
	Oracle *logSide       `json:"o"`
	Values jsontext.Value `json:"v"`
	Query  int64          `json:"q"`
}

// statementText returns the CQL text of a query id: from this process when its
// committers wrote it, or else from the queries table.
func (c *cqlStatements) statementText(ctx context.Context, id int64) string {
	if text, ok := c.queries.text(id); ok {
		return text
	}

	var text string
	if err := c.session.QueryWithContext(ctx, c.queriesSelect, id).Scan(&text); err != nil {
		return "unknown query " + strconv.FormatInt(id, 10)
	}

	return text
}

// streamStatements copies every statement row of one partition that ty
// observed into e. A partition can hold millions of rows, so rows go to the
// sink one at a time and are never collected. NumRows would bound the scan to
// the first page: Scan pages on its own.
//
// Each entry has the fields of one execution on that cluster: ts, seq,
// statement, values, host, attempt, gemini_attempt, error (left out when empty)
// and dur.
func (c *cqlStatements) streamStatements(ctx context.Context, e *lineEncoder, ty stmtlogger.Type, values []any) error {
	q := c.session.QueryWithContext(ctx, c.selectStmt, values...)
	defer q.Release()

	iter := q.Iter()

	e.array("statements")
	first := true

	var (
		ts, seq                int64
		attempt, geminiAttempt int16
		data                   []byte
		entry                  []byte
	)

	// e.failed() stops the scan: once the file rejects a byte nothing later can
	// reach it, and a hot partition would otherwise page millions of rows into a
	// writer that drops them, all while holding the sink.
	for !e.failed() && iter.Scan(&ts, &attempt, &geminiAttempt, &seq, &data) {
		var record logRecord
		if err := json.Unmarshal(data, &record); err != nil {
			continue
		}

		// A row holds both clusters. This cluster's file skips the rows that
		// hold only the other cluster.
		side := record.Test
		if ty == stmtlogger.TypeOracle {
			side = record.Oracle
		}

		if side == nil {
			continue
		}

		entry = appendStatementEntry(entry[:0], side, c.statementText(ctx, record.Query), record.Values,
			seq, attempt, geminiAttempt)

		e.row(first, entry)
		first = false
	}

	e.endArray(true)

	return iter.Close()
}

func appendStatementEntry(
	buf []byte,
	side *logSide,
	statement string,
	values jsontext.Value,
	seq int64,
	attempt, geminiAttempt int16,
) []byte {
	buf = append(buf, `{"ts":`...)
	buf = strconv.AppendInt(buf, side.TS, 10)
	buf = append(buf, `,"seq":`...)
	buf = strconv.AppendInt(buf, seq, 10)
	buf = append(buf, `,"statement":`...)
	buf = appendJSONString(buf, statement)
	buf = append(buf, `,"values":`...)

	if len(values) == 0 {
		buf = append(buf, "null"...)
	} else {
		buf = append(buf, values...)
	}

	buf = append(buf, `,"host":`...)
	buf = appendJSONString(buf, side.Host)
	buf = append(buf, `,"attempt":`...)
	buf = strconv.AppendInt(buf, int64(attempt), 10)
	buf = append(buf, `,"gemini_attempt":`...)
	buf = strconv.AppendInt(buf, int64(geminiAttempt), 10)

	if side.Error != "" {
		buf = append(buf, `,"error":`...)
		buf = appendJSONString(buf, side.Error)
	}

	buf = append(buf, `,"dur":`...)
	buf = appendJSONString(buf, time.Duration(side.Duration).String())

	return append(buf, '}')
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
		session *gocql.Session
		err     error
	)

	switch ty {
	case stmtlogger.TypeOracle:
		session, err = c.oracleSession()
	case stmtlogger.TypeTest:
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

		n, writeErr, readErr := c.streamLine(ctx, session, ty, w, item, item.PartitionKeys.ToMap(), values)
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

			n, writeErr, readErr := c.streamLine(ctx, session, ty, w, item, pks, values)
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
	ty stmtlogger.Type,
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
		statementsErr = c.streamStatements(ctx, e, ty, values)
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

// buildCreateQueriesTableQuery builds the CQL that creates the queries table of
// a _logs keyspace.
func buildCreateQueriesTableQuery(keyspace string) string {
	return "CREATE TABLE IF NOT EXISTS " + keyspace + "." + queriesTable +
		" (id bigint PRIMARY KEY, statement text)" + logTableOptions +
		" AND comment='CQL text of the query ids in the statement log tables';"
}

func buildCreateTableQuery(
	keyspace string,
	table string,
	partitionKeys typedef.Columns,
	replication replication.Replication,
) (string, string) {
	createKeyspace := buildCreateKeyspaceQuery(keyspace, replication)

	var builder strings.Builder

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

	// ts is the earlier start of the two clusters, as UTC nanoseconds, not a
	// CQL timestamp.
	builder.WriteString("ts bigint, attempt smallint, gemini_attempt smallint, seq bigint, data text, ")
	builder.WriteString("PRIMARY KEY ((")
	builder.WriteString(strings.Join(partitionKeys.Names(), ","))
	builder.WriteString("), ts, attempt, gemini_attempt, seq))")
	builder.WriteString(logTableOptions)
	builder.WriteString(" AND comment='Table to store logs from Oracle and Test statements';")

	return createKeyspace, builder.String()
}
