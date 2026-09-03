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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

func TestGetScyllaStatementLogsKeyspace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{"test", "test_logs"},
		{"my_keyspace", "my_keyspace_logs"},
		{"", "_logs"},
		{"prod", "prod_logs"},
		{"ks_123", "ks_123_logs"},
	}

	for _, tt := range tests {
		got := GetScyllaStatementLogsKeyspace(tt.input)
		assert.Equal(t, tt.want, got)
	}
}

func TestGetScyllaStatementLogsTable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input string
		want  string
	}{
		{"test", "test_statements"},
		{"my_table", "my_table_statements"},
		{"", "_statements"},
		{"tbl_123", "tbl_123_statements"},
	}

	for _, tt := range tests {
		got := GetScyllaStatementLogsTable(tt.input)
		assert.Equal(t, tt.want, got)
	}
}

func TestBuildCreateTableQueryUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		keyspace      string
		table         string
		partitionKeys typedef.Columns
		replication   replication.Replication
		wantContains  []string
	}{
		{
			name:     "single text partition key",
			keyspace: "test_logs",
			table:    "test_statements",
			partitionKeys: typedef.Columns{
				{Name: "pk0", Type: typedef.TypeText},
			},
			replication: replication.NewNetworkTopologyStrategy(),
			wantContains: []string{
				"CREATE TABLE IF NOT EXISTS test_logs.test_statements",
				"pk0 text",
				"ts bigint",
				"seq bigint",
				"PRIMARY KEY ((pk0, ty), ts, attempt, gemini_attempt, seq)",
			},
		},
		{
			name:     "multiple partition keys",
			keyspace: "multi_logs",
			table:    "multi_statements",
			partitionKeys: typedef.Columns{
				{Name: "pk0", Type: typedef.TypeText},
				{Name: "pk1", Type: typedef.TypeInt},
			},
			replication: replication.NewNetworkTopologyStrategy(),
			wantContains: []string{
				"pk0 text",
				"pk1 int",
				"PRIMARY KEY ((pk0,pk1, ty)",
			},
		},
		{
			name:     "uuid partition key",
			keyspace: "uuid_logs",
			table:    "uuid_statements",
			partitionKeys: typedef.Columns{
				{Name: "id", Type: typedef.TypeUUID},
			},
			replication: replication.NewNetworkTopologyStrategy(),
			wantContains: []string{
				"id uuid",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotKeyspace, gotTable := buildCreateTableQuery(
				tt.keyspace,
				tt.table,
				tt.partitionKeys,
				tt.replication,
			)

			assert.Contains(t, gotKeyspace, "CREATE KEYSPACE IF NOT EXISTS")
			assert.Contains(t, gotKeyspace, tt.keyspace)

			for _, want := range tt.wantContains {
				assert.Contains(t, gotTable, want)
			}
		})
	}
}

func TestPrepareValuesOptimizedUnit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		values   mo.Either[[]any, []byte]
		expected []string
	}{
		{
			name:     "nil left values",
			values:   mo.Left[[]any, []byte](nil),
			expected: nil,
		},
		{
			name:     "empty left slice",
			values:   mo.Left[[]any, []byte]([]any{}),
			expected: []string{},
		},
		{
			name:     "single string value",
			values:   mo.Left[[]any, []byte]([]any{"test"}),
			expected: []string{`"test"`},
		},
		{
			name:     "mixed types",
			values:   mo.Left[[]any, []byte]([]any{"str", 123, true, 45.67}),
			expected: []string{`"str"`, `123`, `true`, `45.67`},
		},
		{
			name:     "right byte array",
			values:   mo.Right[[]any, []byte]([]byte("serialized")),
			expected: []string{"serialized"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := prepareValuesOptimized(tt.values)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAdditionalColumnsUnit(t *testing.T) {
	t.Parallel()

	expected := []string{"ts", "seq", "ty", "statement", "values", "host", "attempt", "gemini_attempt", "error", "dur"}

	assert.Len(t, additionalColumnsArr, len(expected))
	for i, col := range expected {
		assert.Equal(t, col, additionalColumnsArr[i])
	}
}

func TestLine_JSONMarshaling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		line Line
	}{
		{
			name: "complete line",
			line: Line{
				PartitionKeys: []PartitionInfo{
					{
						PartitionKeys: map[string]any{
							"pk0": "key1",
							"pk1": 123,
						},
					},
				},
				Timestamp: time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
				Query:     "SELECT * FROM test",
				Message:   "test message",
				MutationFragments: []json.RawMessage{
					json.RawMessage(`{"data":"test"}`),
				},
				Statements: []json.RawMessage{
					json.RawMessage(`{"stmt":"test"}`),
				},
			},
		},
		{
			name: "line with error",
			line: Line{
				PartitionKeys: []PartitionInfo{
					{
						PartitionKeys: map[string]any{"pk0": "key"},
					},
				},
				Timestamp: time.Now(),
				Err:       "assert.AnError general error for testing",
				Query:     "INSERT INTO test VALUES (?)",
				Message:   "error occurred",
			},
		},
		{
			name: "minimal line",
			line: Line{
				PartitionKeys: []PartitionInfo{},
				Timestamp:     time.Now(),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			data, err := json.Marshal(tt.line)
			require.NoError(t, err)

			var unmarshaled Line
			err = json.Unmarshal(data, &unmarshaled)
			require.NoError(t, err)

			assert.Equal(t, tt.line.Query, unmarshaled.Query)
			assert.Equal(t, tt.line.Message, unmarshaled.Message)
		})
	}
}

func TestLogger_OpenStatementFile(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()
	logger := zaptest.NewLogger(t)

	mockLogger := &Logger{
		logger: logger,
	}

	t.Run("create and write", func(t *testing.T) {
		t.Parallel()
		filePath := filepath.Join(tmpDir, "test.jsonl")

		writer, closer, err := mockLogger.openStatementFile(filePath)
		require.NoError(t, err)

		data := []byte("test\n")
		n, err := writer.Write(data)
		require.NoError(t, err)
		assert.Equal(t, len(data), n)

		err = closer()
		require.NoError(t, err)

		content, err := os.ReadFile(filePath)
		require.NoError(t, err)
		assert.Equal(t, string(data), string(content))
	})

	t.Run("invalid path", func(t *testing.T) {
		t.Parallel()

		// Use a path with a null byte which is invalid on all systems
		invalidPath := "/tmp/test\x00invalid.jsonl"
		_, _, err := mockLogger.openStatementFile(invalidPath)
		assert.Error(t, err)
	})
}

func TestLogger_Insert(t *testing.T) {
	t.Parallel()

	logger := zaptest.NewLogger(t)

	mockLogger := &Logger{
		logger: logger,
	}

	schemaTypes := []typedef.StatementType{
		typedef.CreateKeyspaceStatementType,
		typedef.CreateTableStatementType,
		typedef.DropTableStatementType,
	}

	for _, stmtType := range schemaTypes {
		item := stmtlogger.Item{
			StatementType: stmtType,
			Statement:     "SCHEMA STATEMENT",
		}
		// Should not panic
		mockLogger.insert(item)
	}
}

func TestLogger_Close(t *testing.T) {
	logger := zaptest.NewLogger(t)

	mockLogger := &Logger{
		logger: logger,
	}

	mockLogger.wg.Add(2)

	var completed int
	var mu sync.Mutex

	for range 2 {
		go func() {
			time.Sleep(10 * time.Millisecond)
			mu.Lock()
			completed++
			mu.Unlock()
			mockLogger.wg.Done()
		}()
	}

	err := mockLogger.Close()
	require.NoError(t, err)

	mu.Lock()
	assert.Equal(t, 2, completed)
	mu.Unlock()
}

func TestStatementSink_WritesAndFlushes(t *testing.T) {
	lg := &Logger{
		logger: zaptest.NewLogger(t),
		fetchHook: func(_ context.Context, _ stmtlogger.Type, item *joberror.JobError, w io.Writer) (int64, error) {
			e := newLineEncoder(w)
			e.head(reorganizePartitionKeys(map[string][]any{"pk": {"a"}}, item), item.Timestamp, "", item.Query, item.Message)
			e.array("mutationFragments")
			e.endArray(false)
			e.array("statements")
			e.row(true, json.RawMessage(`{"x":1}`))
			e.endArray(true)
			e.end(false)

			return e.Close()
		},
	}

	dir := t.TempDir()
	oraclePath := filepath.Join(dir, "oracle.jsonl")
	testPath := filepath.Join(dir, "test.jsonl")

	// Prometheus counters cannot be set backwards, so only assert they grow.
	beforeOracle := testutil.ToFloat64(metrics.StatementLoggerFlushes.WithLabelValues("oracle_file"))
	beforeTest := testutil.ToFloat64(metrics.StatementLoggerFlushes.WithLabelValues("test_file"))

	require.NoError(t, lg.openSinks(oraclePath, testPath))

	je := &joberror.JobError{
		Timestamp: time.Now(),
		Query:     "SELECT 1",
		Message:   "boom",
	}

	lg.writeErrorStatements(t.Context(), stmtlogger.TypeOracle, je)
	lg.writeErrorStatements(t.Context(), stmtlogger.TypeTest, je)
	lg.closeSinks()

	readAndCheck := func(p string) string {
		f, err := os.Open(p)
		require.NoError(t, err)
		defer f.Close()

		r := bufio.NewScanner(f)

		var lines []string
		for r.Scan() {
			lines = append(lines, r.Text())
		}

		require.NoError(t, r.Err())
		require.Len(t, lines, 1)

		var line Line
		require.NoError(t, json.Unmarshal([]byte(lines[0]), &line))
		assert.Equal(t, "SELECT 1", line.Query)
		assert.Len(t, line.Statements, 1)

		return lines[0]
	}

	assert.Contains(t, readAndCheck(oraclePath), "SELECT 1")
	assert.Contains(t, readAndCheck(testPath), "SELECT 1")

	afterOracle := testutil.ToFloat64(metrics.StatementLoggerFlushes.WithLabelValues("oracle_file"))
	afterTest := testutil.ToFloat64(metrics.StatementLoggerFlushes.WithLabelValues("test_file"))
	assert.Greater(t, afterOracle, beforeOracle)
	assert.Greater(t, afterTest, beforeTest)
}

// TestStatementSink_NoInterleaving pins the invariant the sink mutex exists for:
// fetches stream row by row straight into a shared file, so two concurrent
// writers must never interleave their lines.
func TestStatementSink_NoInterleaving(t *testing.T) {
	const writers = 32

	lg := &Logger{
		logger: zaptest.NewLogger(t),
		fetchHook: func(_ context.Context, _ stmtlogger.Type, item *joberror.JobError, w io.Writer) (int64, error) {
			e := newLineEncoder(w)
			e.head(nil, item.Timestamp, "", item.Query, item.Message)
			e.array("mutationFragments")
			e.endArray(false)
			e.array("statements")

			// Write the rows one at a time with a yield between them: without the
			// sink lock another writer would slip its own rows in here.
			for i := range 8 {
				e.row(i == 0, json.RawMessage(`{"x":1}`))
				runtime.Gosched()
			}

			e.endArray(true)
			e.end(false)

			return e.Close()
		},
	}

	path := filepath.Join(t.TempDir(), "oracle.jsonl")
	require.NoError(t, lg.openSinks(path, ""))

	var wg sync.WaitGroup

	for i := range writers {
		wg.Go(func() {
			lg.writeErrorStatements(t.Context(), stmtlogger.TypeOracle, &joberror.JobError{
				Timestamp: time.Now(),
				Query:     fmt.Sprintf("SELECT %d", i),
				Message:   "concurrent",
			})
		})
	}

	wg.Wait()
	lg.closeSinks()

	content, err := os.ReadFile(path)
	require.NoError(t, err)

	lines := parseLines(t, content)
	require.Len(t, lines, writers)

	for _, l := range lines {
		assert.Len(t, l.Statements, 8, "a line must carry only its own rows")
	}
}

// TestStatementSink_FlushesOnFetchError pins the behaviour a partial fetch
// depends on: FetchTo returns a read error after writing a complete line, so the
// sink must still flush or that line never reaches the disk.
func TestStatementSink_FlushesOnFetchError(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "oracle.jsonl")

	lg := &Logger{
		logger: zaptest.NewLogger(t),
		fetchHook: func(_ context.Context, _ stmtlogger.Type, item *joberror.JobError, w io.Writer) (int64, error) {
			e := newLineEncoder(w)
			e.head(nil, item.Timestamp, "", item.Query, item.Message)
			e.array("mutationFragments")
			e.endArray(false)
			e.array("statements")
			e.endArray(true)
			e.end(false)

			n, _ := e.Close()

			return n, newReadError(errors.New("read failed halfway"))
		},
	}

	require.NoError(t, lg.openSinks(path, ""))

	lg.writeErrorStatements(t.Context(), stmtlogger.TypeOracle, &joberror.JobError{
		Timestamp: time.Now(),
		Query:     "SELECT 1",
		Message:   "partial",
	})

	// Read before closing the sink: a flush at Close would hide the bug.
	content, err := os.ReadFile(path)
	require.NoError(t, err)

	lines := parseLines(t, content)
	require.Len(t, lines, 1, "the line written before the read error must be on disk")
	assert.Equal(t, "SELECT 1", lines[0].Query)

	lg.closeSinks()
}

// TestStatementSink_WriteFailureIsNotCounted: the flush counter is what an
// operator reads to decide the statements file is complete, so a line that never
// reached the disk must not increment it.
func TestStatementSink_WriteFailureIsNotCounted(t *testing.T) {
	t.Parallel()

	lg := &Logger{
		logger: zaptest.NewLogger(t),
		fetchHook: func(_ context.Context, _ stmtlogger.Type, _ *joberror.JobError, w io.Writer) (int64, error) {
			n, _ := w.Write([]byte(`{"partial":`))

			// A bare error, not a readError: the file did not take the line.
			return int64(n), errors.New("disk full")
		},
	}

	require.NoError(t, lg.openSinks(filepath.Join(t.TempDir(), "oracle.jsonl"), ""))

	t.Cleanup(lg.closeSinks)

	// The flush counter is shared with the other tests in this package, so the
	// assertion uses a label value only this job error can produce.
	marker := errors.New("write-failure-not-counted-marker")

	lg.writeErrorStatements(t.Context(), stmtlogger.TypeOracle, &joberror.JobError{
		Timestamp: time.Now(),
		Query:     "SELECT 1",
		Message:   "boom",
		Err:       marker,
	})

	assert.False(t, hasErrorSeries(marker.Error()), "a write failure must not be stamped as recorded")
}

// TestStatementSink_CancelledMidLineIsCounted: shutdown can cancel the read
// after the line is already on disk. The line is complete and marked partial, so
// it must be counted, not reported as never written.
func TestStatementSink_CancelledMidLineIsCounted(t *testing.T) {
	t.Parallel()

	lg := &Logger{
		logger: zaptest.NewLogger(t),
		fetchHook: func(_ context.Context, _ stmtlogger.Type, item *joberror.JobError, w io.Writer) (int64, error) {
			e := newLineEncoder(w)
			e.head(nil, item.Timestamp, "", item.Query, item.Message)
			e.array("mutationFragments")
			e.endArray(false)
			e.array("statements")
			e.endArray(true)
			e.end(true)

			written, _ := e.Close()

			return written, newReadError(context.Canceled)
		},
	}

	require.NoError(t, lg.openSinks(filepath.Join(t.TempDir(), "oracle.jsonl"), ""))

	t.Cleanup(lg.closeSinks)

	marker := errors.New("cancelled-mid-line-marker")

	lg.writeErrorStatements(t.Context(), stmtlogger.TypeOracle, &joberror.JobError{
		Timestamp: time.Now(),
		Query:     "SELECT 1",
		Message:   "boom",
		Err:       marker,
	})

	assert.True(t, hasErrorSeries(marker.Error()), "a line that reached the disk must be counted")
}

// TestStatementSink_CancelledBeforeWriteIsNotCounted: the same shutdown before
// any byte reached the file records nothing.
func TestStatementSink_CancelledBeforeWriteIsNotCounted(t *testing.T) {
	t.Parallel()

	lg := &Logger{
		logger: zaptest.NewLogger(t),
		fetchHook: func(_ context.Context, _ stmtlogger.Type, _ *joberror.JobError, _ io.Writer) (int64, error) {
			return 0, newReadError(context.Canceled)
		},
	}

	require.NoError(t, lg.openSinks(filepath.Join(t.TempDir(), "oracle.jsonl"), ""))

	t.Cleanup(lg.closeSinks)

	marker := errors.New("cancelled-before-write-marker")

	lg.writeErrorStatements(t.Context(), stmtlogger.TypeOracle, &joberror.JobError{
		Timestamp: time.Now(),
		Query:     "SELECT 1",
		Message:   "boom",
		Err:       marker,
	})

	assert.False(t, hasErrorSeries(marker.Error()), "nothing on disk must not be stamped as recorded")
}

// TestStatementSink_LatchesWriteFailure: after the file rejects a line, no
// later job error may read its history out of _logs. Those reads cost cluster
// time and cannot reach the disk anymore.
func TestStatementSink_LatchesWriteFailure(t *testing.T) {
	t.Parallel()

	var calls atomic.Int64

	lg := &Logger{
		logger: zaptest.NewLogger(t),
		fetchHook: func(_ context.Context, _ stmtlogger.Type, _ *joberror.JobError, w io.Writer) (int64, error) {
			calls.Add(1)

			n, _ := w.Write([]byte(`{"partial":`))

			// A bare error, not a readError: the file did not take the line.
			return int64(n), errors.New("disk full")
		},
	}

	require.NoError(t, lg.openSinks(filepath.Join(t.TempDir(), "oracle.jsonl"), ""))

	t.Cleanup(lg.closeSinks)

	je := &joberror.JobError{Timestamp: time.Now(), Query: "SELECT 1", Message: "boom"}

	lg.writeErrorStatements(t.Context(), stmtlogger.TypeOracle, je)
	lg.writeErrorStatements(t.Context(), stmtlogger.TypeOracle, je)

	assert.Equal(t, int64(1), calls.Load(), "a broken file must reject later fetches before they read _logs")
}

// TestStatementSink_ReadErrorDoesNotLatch: a read error costs one partition its
// content and leaves the file usable, so the next job error must still be
// fetched and written.
func TestStatementSink_ReadErrorDoesNotLatch(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "oracle.jsonl")

	var calls atomic.Int64

	lg := &Logger{
		logger: zaptest.NewLogger(t),
		fetchHook: func(_ context.Context, _ stmtlogger.Type, item *joberror.JobError, w io.Writer) (int64, error) {
			n := calls.Add(1)

			e := newLineEncoder(w)
			e.head(nil, item.Timestamp, "", item.Query, item.Message)
			e.array("mutationFragments")
			e.endArray(false)
			e.array("statements")
			e.endArray(true)
			e.end(false)

			written, _ := e.Close()

			if n == 1 {
				return written, newReadError(errors.New("read failed halfway"))
			}

			return written, nil
		},
	}

	require.NoError(t, lg.openSinks(path, ""))

	lg.writeErrorStatements(t.Context(), stmtlogger.TypeOracle, &joberror.JobError{
		Timestamp: time.Now(), Query: "SELECT 1", Message: "partial",
	})
	lg.writeErrorStatements(t.Context(), stmtlogger.TypeOracle, &joberror.JobError{
		Timestamp: time.Now(), Query: "SELECT 2", Message: "whole",
	})

	assert.Equal(t, int64(2), calls.Load(), "a read error must not close the file for later job errors")

	content, err := os.ReadFile(path)
	require.NoError(t, err)

	lines := parseLines(t, content)
	require.Len(t, lines, 2)
	assert.Equal(t, "SELECT 2", lines[1].Query)

	lg.closeSinks()
}

// TestStatementSink_AdmissionIsCancellable: a queued writer waits behind a whole
// partition scan, so shutdown can only be bounded if that wait ends with the
// context.
func TestStatementSink_AdmissionIsCancellable(t *testing.T) {
	t.Parallel()

	sink := newLineSink("test.jsonl", bufio.NewWriter(io.Discard), func() error { return nil })

	held := make(chan struct{})
	release := make(chan struct{})

	go func() {
		_ = sink.Write(t.Context(), func(io.Writer) error {
			close(held)
			<-release

			return nil
		})
	}()

	<-held

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	var reached atomic.Bool

	err := sink.Write(ctx, func(io.Writer) error {
		reached.Store(true)

		return nil
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.False(t, reached.Load(), "a cancelled writer must not enter the file")

	close(release)
}

// hasErrorSeries reports whether StatementErrorLastTS holds a series whose
// error label equals want.
func hasErrorSeries(want string) bool {
	ch := make(chan prometheus.Metric, 1024)

	go func() {
		metrics.StatementErrorLastTS.Collect(ch)
		close(ch)
	}()

	for m := range ch {
		var dto dto.Metric
		if err := m.Write(&dto); err != nil {
			continue
		}

		for _, l := range dto.GetLabel() {
			if l.GetName() == "error" && l.GetValue() == want {
				return true
			}
		}
	}

	return false
}

// TestOpenSinks_ClosesOnPartialFailure: the caller gets no logger when this
// fails, so a file opened before the failure would leak its descriptor.
func TestOpenSinks_ClosesOnPartialFailure(t *testing.T) {
	t.Parallel()

	lg := &Logger{logger: zaptest.NewLogger(t)}

	oraclePath := filepath.Join(t.TempDir(), "oracle.jsonl")
	boom := errors.New("second file refused to open")

	var closed atomic.Int32

	// Reopening the same path succeeds whether or not the first descriptor was
	// closed, so the double has to report the close itself.
	lg.openHook = func(name string) (*bufio.Writer, func() error, error) {
		if name != oraclePath {
			return nil, nil, boom
		}

		return bufio.NewWriter(io.Discard), func() error {
			closed.Add(1)

			return nil
		}, nil
	}

	err := lg.openSinks(oraclePath, "/tmp/test-invalid.jsonl")
	require.ErrorIs(t, err, boom)
	assert.Nil(t, lg.sinks, "no sink may survive a failed setup")
	assert.Equal(t, int32(1), closed.Load(), "the file opened before the failure must be closed")
}

func TestFetchErrors_DedupAndFanout(t *testing.T) {
	t.Parallel()

	// Both sinks, or a fetch for the missing side would return before it reaches
	// the hook and the fan-out half of this test would assert nothing.
	dir := t.TempDir()

	lg := &Logger{logger: zaptest.NewLogger(t)}
	require.NoError(t, lg.openSinks(filepath.Join(dir, "oracle.jsonl"), filepath.Join(dir, "test.jsonl")))

	t.Cleanup(lg.closeSinks)

	// Two job errors with identical content, and therefore an identical hash.
	base := &joberror.JobError{Timestamp: time.Now(), Query: "Q", Message: "M"}
	je1 := *base
	je2 := *base

	var (
		mu    sync.Mutex
		types []stmtlogger.Type
	)

	lg.fetchHook = func(_ context.Context, ty stmtlogger.Type, _ *joberror.JobError, _ io.Writer) (int64, error) {
		mu.Lock()
		defer mu.Unlock()

		types = append(types, ty)

		return 0, nil
	}

	lg.fetchDelay = time.Millisecond

	in := make(chan *joberror.JobError, 2)
	lg.wg.Add(1)

	go lg.fetchErrors(in)

	in <- &je1
	in <- &je2 // duplicate; should be ignored by dedupe
	close(in)

	lg.wg.Wait()

	mu.Lock()
	defer mu.Unlock()

	// One fetch per cluster side, for one of the two identical job errors.
	slices.Sort(types)
	assert.Equal(t, []stmtlogger.Type{stmtlogger.TypeOracle, stmtlogger.TypeTest}, types)
}

// TestFetchErrors_CancelledOnClose: a queued fetch pages through a whole
// partition under the sink lock, so Close must be able to cut the queue short
// instead of waiting for every scan.
func TestFetchErrors_CancelledOnClose(t *testing.T) {
	t.Parallel()

	lg := &Logger{logger: zaptest.NewLogger(t)}
	require.NoError(t, lg.openSinks(filepath.Join(t.TempDir(), "oracle.jsonl"), ""))

	t.Cleanup(lg.closeSinks)

	lg.fetchDelay = time.Hour

	var calls atomic.Int64

	lg.fetchHook = func(_ context.Context, _ stmtlogger.Type, _ *joberror.JobError, _ io.Writer) (int64, error) {
		calls.Add(1)
		return 0, nil
	}

	in := make(chan *joberror.JobError, 1)
	lg.wg.Add(1)

	go lg.fetchErrors(in)

	in <- &joberror.JobError{Timestamp: time.Now(), Query: "Q", Message: "M"}
	close(in)

	// The fetch is parked on the hour-long delay. Cancelling releases it.
	lg.fetchCancel()
	lg.wg.Wait()

	assert.Zero(t, calls.Load(), "a cancelled fetch must not run")
}

// Benchmarks

func BenchmarkPrepareValuesOptimized(b *testing.B) {
	values := mo.Left[[]any, []byte]([]any{"test", 123, true, "another", 456})

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = prepareValuesOptimized(values)
	}
}

func BenchmarkGetScyllaStatementLogsKeyspace(b *testing.B) {
	keyspace := "test_keyspace"

	b.ResetTimer()
	for range b.N {
		_ = GetScyllaStatementLogsKeyspace(keyspace)
	}
}

func BenchmarkGetScyllaStatementLogsTable(b *testing.B) {
	table := "test_table"

	b.ResetTimer()
	for range b.N {
		_ = GetScyllaStatementLogsTable(table)
	}
}

func BenchmarkBuildCreateTableQuery(b *testing.B) {
	partitionKeys := typedef.Columns{
		{Name: "pk0", Type: typedef.TypeText},
		{Name: "pk1", Type: typedef.TypeInt},
	}
	repl := replication.NewNetworkTopologyStrategy()

	b.ResetTimer()
	for range b.N {
		_, _ = buildCreateTableQuery("test_logs", "test_statements", partitionKeys, repl)
	}
}

func BenchmarkLine_Marshal(b *testing.B) {
	line := Line{
		PartitionKeys: []PartitionInfo{
			{
				PartitionKeys: map[string]any{
					"pk0": "key1",
					"pk1": 123,
				},
			},
		},
		Timestamp: time.Now(),
		Query:     "SELECT * FROM test",
		Message:   "test message",
		MutationFragments: []json.RawMessage{
			json.RawMessage(`{"data":"test"}`),
		},
		Statements: []json.RawMessage{
			json.RawMessage(`{"stmt":"test"}`),
		},
	}

	b.ResetTimer()
	for range b.N {
		_ = utils.MarshalJSONUnchecked(line)
	}
}
