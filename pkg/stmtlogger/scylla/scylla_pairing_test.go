// Copyright 2026 ScyllaDB
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
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/samber/mo"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
)

// distinctItem builds an item whose statement AND values are unique to idx, so
// that any bleed between the statement column and the values column (a mispair)
// is detectable: a coherent bind always has statement[i] paired with values[i].
func distinctItem(idx int) stmtlogger.Item {
	return stmtlogger.Item{
		Start:         stmtlogger.Time{Time: time.Unix(0, int64(idx))},
		PartitionKeys: typedef.PartitionKeys{Values: typedef.NewValuesFromMap(map[string][]any{"pk0": {fmt.Sprintf("k%d", idx)}, "pk1": {idx}})},
		Error:         mo.Right[error, string](""),
		Statement:     fmt.Sprintf("UPDATE ks.tbl SET c%d=? WHERE pk0=? AND pk1=?", idx),
		Host:          "127.0.0.1",
		Type:          stmtlogger.TypeTest,
		Values:        mo.Left[[]any, []byte]([]any{idx, fmt.Sprintf("v%d", idx)}),
		Duration:      stmtlogger.Duration{Duration: time.Millisecond},
		Attempt:       1,
		GeminiAttempt: 1,
		StatementType: typedef.InsertStatementType,
	}
}

// checkCoherent verifies the bound row's partition-key columns and its data
// cell (query id and values) all belong to the SAME item (idx) — the invariant
// that makes _logs triage trustworthy. Layout (see cqlStatements.fillPairArgs):
// [pk values...][ts][attempt][gemini_attempt][seq][data]
//
// It reports a failure as an error rather than failing the test directly, so it
// is safe to call from a worker goroutine: testify's require calls t.FailNow,
// which Go permits only on the goroutine running the test function.
func checkCoherent(out []any, pkLen, idx int) error {
	want := distinctItem(idx)

	wantPK := fmt.Sprintf("k%d", idx)
	if gotPK, ok := out[0].(string); !ok || gotPK != wantPK {
		return fmt.Errorf("idx %d: partition key bled from another item: got %v, want %q", idx, out[0], wantPK)
	}

	data, ok := out[pkLen+len(logColumns)-1].(logData)
	if !ok {
		return fmt.Errorf("idx %d: data column has type %T, want logData", idx, out[pkLen+len(logColumns)-1])
	}

	var record logRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return fmt.Errorf("idx %d: data column is not valid JSON: %w: %s", idx, err, data)
	}

	if record.Query != queryID(want.Statement) {
		return fmt.Errorf("idx %d: query id bled from another item: got %d, want %d", idx, record.Query, queryID(want.Statement))
	}

	if wantValues := appendLogValues(nil, want.Values); !bytes.Equal(record.Values, wantValues) {
		return fmt.Errorf("idx %d: values bled from another item: got %s, want %s", idx, record.Values, wantValues)
	}

	return nil
}

// TestFillArgs_BufferReuseKeepsPairing mirrors the committer's held[idx] reuse:
// one dst buffer is rebound across a long stream of distinct items. Each bound
// row must carry its OWN statement and values — no residue from the prior item.
func TestFillArgs_BufferReuseKeepsPairing(t *testing.T) {
	t.Parallel()
	pks := typedef.Columns{{Name: "pk0", Type: typedef.TypeText}, {Name: "pk1", Type: typedef.TypeInt}}
	c := makeTestCQL(pks)
	pkLen := pks.LenValues()

	dst := make([]any, 0, c.argsCap())
	for i := range 5000 {
		var ok bool
		dst, ok = c.fillArgs(dst, distinctItem(i))
		require.True(t, ok)
		require.NoError(t, checkCoherent(dst, pkLen, i))
	}
}

// TestFillArgs_ConcurrentBindingIsCoherent runs many goroutines binding through
// a SHARED *cqlStatements (as every committer worker does), each with its own
// dst buffer (as held[idx] is per-worker). Run with -race: catches any shared
// mutable state introduced into fillArgs/cqlStatements that would let one
// goroutine's statement pair with another's values.
func TestFillArgs_ConcurrentBindingIsCoherent(t *testing.T) {
	t.Parallel()
	pks := typedef.Columns{{Name: "pk0", Type: typedef.TypeText}, {Name: "pk1", Type: typedef.TypeInt}}
	c := makeTestCQL(pks)
	pkLen := pks.LenValues()

	const workers = 16

	// Workers must not assert: require/t.FailNow are only legal on the goroutine
	// running the test function. Each worker reports its first incoherent bind
	// back through a buffered channel and the parent does the asserting.
	failures := make(chan error, workers)
	start := make(chan struct{})

	var wg sync.WaitGroup

	wg.Add(workers)

	for w := range workers {
		go func(base int) {
			defer wg.Done()

			dst := make([]any, 0, c.argsCap())
			<-start // release all workers together to maximise overlap

			for i := range 2000 {
				idx := base*1_000_000 + i

				var ok bool

				dst, ok = c.fillArgs(dst, distinctItem(idx))
				if !ok {
					failures <- fmt.Errorf("idx %d: fillArgs reported failure", idx)
					return
				}

				if err := checkCoherent(dst, pkLen, idx); err != nil {
					failures <- err
					return
				}
			}
		}(w)
	}

	close(start)
	wg.Wait()
	close(failures)

	for err := range failures {
		t.Error(err)
	}
}

const committerTestTable = "ks.tbl"

var errBatchRejected = errors.New("batch rejected")

func committerTestPartitionKeys() typedef.Columns {
	return typedef.Columns{
		{Name: "pk0", Type: typedef.TypeText},
		{Name: "pk1", Type: typedef.TypeInt},
	}
}

type committedRow struct {
	insert string
	idx    int
}

type recorderAtExecTime struct {
	pkLen           int
	rows            []committedRow
	incoherenceErrs []error
	mu              sync.Mutex
}

func newRecorderAtExecTime(pkLen int) *recorderAtExecTime {
	return &recorderAtExecTime{pkLen: pkLen}
}

func (r *recorderAtExecTime) record(insertStmt string, args []any) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if want := r.pkLen + len(logColumns); len(args) != want {
		r.incoherenceErrs = append(r.incoherenceErrs, fmt.Errorf("bound %d args, want %d", len(args), want))

		return
	}

	pk, ok := args[0].(string)
	if !ok {
		r.incoherenceErrs = append(r.incoherenceErrs, fmt.Errorf("partition key column has type %T, want string", args[0]))

		return
	}

	var idx int
	if _, err := fmt.Sscanf(pk, "k%d", &idx); err != nil {
		r.incoherenceErrs = append(r.incoherenceErrs, fmt.Errorf("unparsable partition key %q: %w", pk, err))

		return
	}

	if err := checkCoherent(args, r.pkLen, idx); err != nil {
		r.incoherenceErrs = append(r.incoherenceErrs, err)

		return
	}

	r.rows = append(r.rows, committedRow{insert: insertStmt, idx: idx})
}

func (r *recorderAtExecTime) addBatch(_ context.Context, batch *gocql.Batch) error {
	for _, entry := range batch.Entries {
		r.record(entry.Stmt, entry.Args)
	}

	return nil
}

func (r *recorderAtExecTime) addQuery(_ context.Context, stmt string, args ...any) error {
	r.record(stmt, args)

	return nil
}

func (r *recorderAtExecTime) result(tb testing.TB) []committedRow {
	tb.Helper()

	r.mu.Lock()
	defer r.mu.Unlock()

	for _, err := range r.incoherenceErrs {
		tb.Error(err)
	}

	return append([]committedRow(nil), r.rows...)
}

// fakeLogSession records what the committer writes in place of a cluster.
type fakeLogSession struct {
	batch func(ctx context.Context, batch *gocql.Batch) error
	query func(ctx context.Context, stmt string, args ...any) error
}

func (fakeLogSession) makeBatch(context.Context) *gocql.Batch {
	return &gocql.Batch{}
}

func (f fakeLogSession) execBatch(ctx context.Context, batch *gocql.Batch) error {
	return f.batch(ctx, batch)
}

func (f fakeLogSession) execQuery(ctx context.Context, stmt string, args ...any) error {
	if f.query == nil {
		return nil
	}

	return f.query(ctx, stmt, args...)
}

func newCommitterLogger(tb testing.TB, ch <-chan stmtlogger.Item) (*Logger, *recorderAtExecTime) {
	tb.Helper()

	pks := committerTestPartitionKeys()
	cql := makeTestCQL(pks)
	cql.insertStmt = "INSERT INTO ks_logs.tbl(pk0,pk1," + strings.Join(logColumns, ",") + ") VALUES (?,?,?,?,?,?,?)"

	committed := newRecorderAtExecTime(pks.LenValues())

	return &Logger{
		logger:     zap.NewNop(),
		channel:    ch,
		statements: map[string]*cqlStatements{committerTestTable: cql},
		logs:       fakeLogSession{batch: committed.addBatch},
	}, committed
}

func committerItem(idx int) stmtlogger.Item {
	it := distinctItem(idx)
	it.Table = committerTestTable

	return it
}

func runCommitter(tb testing.TB, logger *Logger, ch chan stmtlogger.Item, items []stmtlogger.Item) {
	tb.Helper()

	logger.startCommitters(1)

	for _, it := range items {
		ch <- it
	}

	close(ch)
	logger.wg.Wait()
}

func requireEachItemOnce(tb testing.TB, rows []committedRow, want int) {
	tb.Helper()

	seen := make(map[int]bool, len(rows))

	for _, row := range rows {
		require.True(tb, strings.HasPrefix(row.insert, "INSERT INTO ks_logs.tbl"))
		require.False(tb, seen[row.idx], "item %d written twice", row.idx)
		seen[row.idx] = true
	}

	require.Len(tb, seen, want)
}

func TestInsertWorker_BatchEntriesStayPaired(t *testing.T) {
	t.Parallel()

	const items = 4000

	ch := make(chan stmtlogger.Item, 64)
	logger, committed := newCommitterLogger(t, ch)

	queued := make([]stmtlogger.Item, items)
	for i := range queued {
		queued[i] = committerItem(i)
	}

	runCommitter(t, logger, ch, queued)

	rows := committed.result(t)
	require.Len(t, rows, items)
	requireEachItemOnce(t, rows, items)
}

func TestInsertWorker_DroppedItemsDoNotShiftPairing(t *testing.T) {
	t.Parallel()

	const items = 3000

	ch := make(chan stmtlogger.Item, 64)
	logger, committed := newCommitterLogger(t, ch)

	queued := make([]stmtlogger.Item, 0, items)
	kept := 0

	for i := range items {
		it := committerItem(i)

		switch i % 3 {
		case 1:
			it.StatementType = typedef.SelectStatementType
		case 2:
			it.Table = "ks.unknown"
		default:
			kept++
		}

		queued = append(queued, it)
	}

	runCommitter(t, logger, ch, queued)

	rows := committed.result(t)
	require.Len(t, rows, kept)
	requireEachItemOnce(t, rows, kept)

	for _, row := range rows {
		require.Zero(t, row.idx%3)
	}
}

func TestInsertWorker_FallbackRowsMatchTheirStatements(t *testing.T) {
	t.Parallel()

	const items = 600

	ch := make(chan stmtlogger.Item, 64)
	logger, committed := newCommitterLogger(t, ch)

	fallback := newRecorderAtExecTime(committerTestPartitionKeys().LenValues())
	logger.logs = fakeLogSession{
		batch: func(context.Context, *gocql.Batch) error {
			return errBatchRejected
		},
		query: fallback.addQuery,
	}

	queued := make([]stmtlogger.Item, items)
	for i := range queued {
		queued[i] = committerItem(i)
	}

	runCommitter(t, logger, ch, queued)

	require.Empty(t, committed.result(t))

	rows := fallback.result(t)
	require.Len(t, rows, items)
	requireEachItemOnce(t, rows, items)
}
