//go:build testing

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
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
)

func TestLogTSKeepsNanoseconds(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 8, 3, 12, 0, 0, 123_456_789, time.UTC)

	assert.Equal(t, start.UnixNano(), logTS(start))
	assert.Equal(t, int64(123_456_789), logTS(start)%int64(time.Second))
	assert.NotEqual(t, logTS(start), logTS(start.Add(time.Nanosecond)))
}

func TestNextLogSeqIsUniqueUnderConcurrency(t *testing.T) {
	t.Parallel()

	const (
		workers   = 50
		perWorker = 200
	)

	var (
		wg  sync.WaitGroup
		mu  sync.Mutex
		got = make(map[int64]struct{}, workers*perWorker)
	)

	for range workers {
		wg.Go(func() {
			local := make([]int64, 0, perWorker)
			for range perWorker {
				local = append(local, nextLogSeq())
			}

			mu.Lock()
			defer mu.Unlock()
			for _, v := range local {
				got[v] = struct{}{}
			}
		})
	}

	wg.Wait()

	assert.Len(t, got, workers*perWorker, "every call must return a distinct seq")
}

// TestFillArgsBindsTSAndSeq pins the bind values: ts and seq are int64 for their
// bigint columns, and seq advances per row.
func TestFillArgsBindsTSAndSeq(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, 8, 3, 12, 0, 0, 123_456_789, time.UTC)
	partitionKeys := typedef.Columns{{Name: "pk0", Type: typedef.TypeText}}

	c := &cqlStatements{partitionKeys: partitionKeys}
	item := stmtlogger.Item{
		Start: stmtlogger.Time{Time: start},
		PartitionKeys: typedef.PartitionKeys{Values: typedef.NewValuesFromMap(map[string][]any{
			"pk0": {"key"},
		})},
		Error: mo.Left[error, string](nil),
		Type:  stmtlogger.TypeOracle,
	}

	args, ok := c.fillArgs(nil, item)
	require.True(t, ok)
	require.Len(t, args, partitionKeys.LenValues()+len(additionalColumnsArr))
	assert.Equal(t, start.UTC().UnixNano(), args[partitionKeys.LenValues()])

	firstSeq, ok := args[partitionKeys.LenValues()+1].(int64)
	require.True(t, ok, "seq must bind as int64")

	next, ok := c.fillArgs(nil, item)
	require.True(t, ok)
	seq, ok := next[partitionKeys.LenValues()+1].(int64)
	require.True(t, ok)
	assert.Greater(t, seq, firstSeq)
}

// TestLineEncoderHeadAtomicity: a marshal failure must leave the writer
// untouched. A half-written head would merge with the next line, because a
// marshal failure does not poison the writer the way a write failure does.
func TestLineEncoderHeadAtomicity(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	e := newLineEncoder(&buf)
	// A channel cannot be marshaled, so this fails inside head's first field.
	e.head([]PartitionInfo{{PartitionKeys: map[string]any{"pk0": make(chan int)}}},
		time.Now(), "", "SELECT 1", "boom")
	e.array("statements")
	e.endArray(true)
	e.end(false)

	n, err := e.Close()
	require.Error(t, err)
	assert.Zero(t, n, "a failed head must report no bytes")
	assert.Empty(t, buf.Bytes(), "a failed head must write nothing")

	// The same encoder value is never reused, but the writer is: the next line
	// must still parse on its own.
	next := newLineEncoder(&buf)
	next.head(nil, time.Now(), "", "SELECT 2", "ok")
	next.array("mutationFragments")
	next.endArray(false)
	next.array("statements")
	next.endArray(true)
	next.end(false)

	_, err = next.Close()
	require.NoError(t, err)

	lines := parseLines(t, buf.Bytes())
	require.Len(t, lines, 1)
	assert.Equal(t, "SELECT 2", lines[0].Query)
}

// TestLineEncoderPartialMarker: a read that breaks halfway still leaves a
// complete line, so the marker is the only thing that tells a reader the
// history under it is a lower bound.
func TestLineEncoderPartialMarker(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer

	e := newLineEncoder(&buf)
	e.head(nil, time.Now(), "read broke", "SELECT 1", "partial")
	e.array("mutationFragments")
	e.endArray(false)
	e.array("statements")
	e.endArray(true)
	e.end(true)

	_, err := e.Close()
	require.NoError(t, err)

	lines := parseLines(t, buf.Bytes())
	require.Len(t, lines, 1)
	assert.True(t, lines[0].Partial, "a cut-short line must carry the marker")

	buf.Reset()

	full := newLineEncoder(&buf)
	full.head(nil, time.Now(), "", "SELECT 2", "full")
	full.array("mutationFragments")
	full.endArray(false)
	full.array("statements")
	full.endArray(true)
	full.end(false)

	_, err = full.Close()
	require.NoError(t, err)

	lines = parseLines(t, buf.Bytes())
	require.Len(t, lines, 1)
	assert.False(t, lines[0].Partial, "a complete line must not carry the marker")
}

// TestSameMsNoRowLoss reproduces QATOOLS-318: many workers write to one
// partition inside a single millisecond. While ts was a CQL timestamp all of
// those rows shared a primary key and only the last one survived. The "same ns"
// case is the one only seq can save.
func TestSameMsNoRowLoss(t *testing.T) {
	t.Parallel()

	fixed := time.Now()

	tests := []struct {
		start func() time.Time
		name  string
	}{
		{name: "burst", start: time.Now},
		{name: "same ns", start: func() time.Time { return fixed }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			containers := testutils.TestContainers(t)
			logger := zap.NewNop()

			session, err := newSession(containers.TestHosts, containers.TestPort(), containers.DockerMode, "", "", logger)
			require.NoError(t, err)
			t.Cleanup(session.Close)

			keyspace := testutils.GenerateUniqueKeyspaceName(t)
			table := "same_ms_test"

			partitionKeys := typedef.Columns{
				{Name: "pk0", Type: typedef.TypeText},
			}

			cqlStmts, err := newStatements(
				session,
				func() (*gocql.Session, error) { return containers.Oracle, nil },
				func() (*gocql.Session, error) { return containers.Test, nil },
				keyspace, table,
				"test_ks", "test_table",
				partitionKeys,
				replication.NewNetworkTopologyStrategy(),
			)
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = session.Query("DROP KEYSPACE IF EXISTS " + keyspace).Exec()
			})

			const (
				workers      = 50
				perWorker    = 20
				totalWritten = workers * perWorker
			)

			// One partition, one ty, one pair of attempt counters for every item,
			// so ts and seq are all that keep the rows apart.
			items := make([]stmtlogger.Item, 0, totalWritten)
			for i := range totalWritten {
				items = append(items, stmtlogger.Item{
					Start: stmtlogger.Time{Time: tt.start()},
					PartitionKeys: typedef.PartitionKeys{Values: typedef.NewValuesFromMap(map[string][]any{
						"pk0": {"hot_partition"},
					})},
					Error:         mo.Left[error, string](nil),
					Statement:     fmt.Sprintf("INSERT INTO test_table (pk0) VALUES (?) -- item %d", i),
					Host:          "127.0.0.1",
					Type:          stmtlogger.TypeOracle,
					Values:        mo.Left[[]any, []byte]([]any{"hot_partition"}),
					Duration:      stmtlogger.Duration{Duration: time.Millisecond},
					Attempt:       1,
					GeminiAttempt: 1,
				})
			}

			perMillisecond := make(map[int64]int, totalWritten)
			busiest := 0
			for _, item := range items {
				ms := item.Start.Time.UnixMilli()
				perMillisecond[ms]++
				if perMillisecond[ms] > busiest {
					busiest = perMillisecond[ms]
				}
			}
			require.Greater(t, busiest, 1, "the run must contain a millisecond with several statements, otherwise it does not reproduce QATOOLS-318")
			t.Logf("%d statements over %d milliseconds, busiest millisecond holds %d", totalWritten, len(perMillisecond), busiest)

			var wg sync.WaitGroup
			errs := make(chan error, totalWritten)

			for w := range workers {
				wg.Go(func() {
					for i := range perWorker {
						if insErr := cqlStmts.Insert(t.Context(), items[w*perWorker+i]); insErr != nil {
							errs <- insErr
						}
					}
				})
			}

			wg.Wait()
			close(errs)

			for insErr := range errs {
				require.NoError(t, insErr, "insert into _logs should succeed")
			}

			var count int
			require.NoError(t, session.Query(
				fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE pk0 = ? AND ty = ?", keyspace, table),
				"hot_partition", string(stmtlogger.TypeOracle),
			).Scan(&count))

			assert.Equal(t, totalWritten, count, "no _logs row may be lost to a same-millisecond primary-key collision")

			iter := session.Query(
				fmt.Sprintf("SELECT ts, seq, statement FROM %s.%s WHERE pk0 = ? AND ty = ?", keyspace, table),
				"hot_partition", string(stmtlogger.TypeOracle),
			).Iter()

			var (
				ts        int64
				seq       int64
				statement string
				seenSeq   = make(map[int64]struct{}, totalWritten)
				seenStmt  = make(map[string]struct{}, totalWritten)
				prevTS    = int64(-1)
			)

			for iter.Scan(&ts, &seq, &statement) {
				_, dup := seenSeq[seq]
				assert.False(t, dup, "seq %d appeared twice", seq)
				seenSeq[seq] = struct{}{}
				seenStmt[statement] = struct{}{}

				assert.GreaterOrEqual(t, ts, prevTS, "rows must come back in ts order")
				prevTS = ts
			}

			require.NoError(t, iter.Close())
			assert.Len(t, seenSeq, totalWritten, "every write must own a distinct seq")
			assert.Len(t, seenStmt, totalWritten, "every distinct statement must survive")
		})
	}
}

// TestFetchPagesBeyondFirstPage guards the second _logs read defect: gocql
// defaults to 5000 rows per page, and NumRows reports the current page only, so
// bounding the scan loop by it truncated every partition with more statements
// than one page. ORDER BY ts ASC means the newest statements were the ones cut.
func TestFetchPagesBeyondFirstPage(t *testing.T) {
	t.Parallel()

	containers := testutils.TestContainers(t)

	session, err := newSession(containers.TestHosts, containers.TestPort(), containers.DockerMode, "", "", zap.NewNop())
	require.NoError(t, err)
	t.Cleanup(session.Close)

	keyspace := testutils.GenerateUniqueKeyspaceName(t)
	table := "paging_test"
	partitionKeys := typedef.Columns{{Name: "pk0", Type: typedef.TypeText}}

	cqlStmts, err := newStatements(
		session,
		func() (*gocql.Session, error) { return containers.Oracle, nil },
		func() (*gocql.Session, error) { return containers.Test, nil },
		keyspace, table,
		"test_ks", "test_table",
		partitionKeys,
		replication.NewNetworkTopologyStrategy(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = session.Query("DROP KEYSPACE IF EXISTS " + keyspace).Exec()
	})

	// One row over the gocql default page size of 5000.
	const (
		workers      = 50
		perWorker    = 102
		totalWritten = workers * perWorker
	)

	var wg sync.WaitGroup
	errs := make(chan error, totalWritten)

	for w := range workers {
		wg.Go(func() {
			for i := range perWorker {
				insErr := cqlStmts.Insert(t.Context(), stmtlogger.Item{
					Start: stmtlogger.Time{Time: time.Now()},
					PartitionKeys: typedef.PartitionKeys{Values: typedef.NewValuesFromMap(map[string][]any{
						"pk0": {"paged_partition"},
					})},
					Error:         mo.Left[error, string](nil),
					Statement:     fmt.Sprintf("INSERT INTO test_table (pk0) VALUES (?) -- w%d i%d", w, i),
					Host:          "127.0.0.1",
					Type:          stmtlogger.TypeOracle,
					Values:        mo.Left[[]any, []byte]([]any{"paged_partition"}),
					Duration:      stmtlogger.Duration{Duration: time.Millisecond},
					Attempt:       1,
					GeminiAttempt: 1,
				})
				if insErr != nil {
					errs <- insErr
				}
			}
		})
	}

	wg.Wait()
	close(errs)

	for insErr := range errs {
		require.NoError(t, insErr)
	}

	var buf bytes.Buffer

	e := newLineEncoder(&buf)
	require.NoError(t, streamStatements(t.Context(), e, session, cqlStmts.oracleSelect, []any{"paged_partition"}))

	_, err = e.Close()
	require.NoError(t, err)

	var streamed struct {
		Statements []json.RawMessage `json:"statements"`
	}

	require.NoError(t, json.Unmarshal([]byte("{"+buf.String()+"}"), &streamed))
	assert.Len(t, streamed.Statements, totalWritten, "fetch must read every page, not just the first")
}
