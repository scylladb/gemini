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

//nolint:govet
package scylla

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
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
			replication: replication.NewSimpleStrategy(),
			wantContains: []string{
				"CREATE TABLE IF NOT EXISTS test_logs.test_statements",
				"pk0 text",
				"PRIMARY KEY ((pk0, ty)",
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
			replication: replication.NewSimpleStrategy(),
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
				{Name: "id", Type: typedef.TypeUuid},
			},
			replication: replication.NewSimpleStrategy(),
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

	expected := []string{"ts", "ty", "statement", "values", "host", "attempt", "gemini_attempt", "error", "dur"}

	assert.Equal(t, len(expected), len(additionalColumnsArr))
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
				PartitionKeys: map[string][]any{
					"pk0": {"key1"},
					"pk1": {123},
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
				PartitionKeys: map[string][]any{"pk0": {"key"}},
				Timestamp:     time.Now(),
				Err:           "assert.AnError general error for testing",
				Query:         "INSERT INTO test VALUES (?)",
				Message:       "error occurred",
			},
		},
		{
			name: "minimal line",
			line: Line{
				PartitionKeys: map[string][]any{},
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

func TestStatementChData_Structure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		data statementChData
	}{
		{
			name: "oracle type",
			data: statementChData{
				ty: stmtlogger.TypeOracle,
				Data: cqlDataMap{
					{1}: {
						partitionKeys: map[string][]any{"pk0": {"key"}},
						statements:    []json.RawMessage{json.RawMessage(`{"stmt":"SELECT"}`)},
					},
				},
				Error: &joberror.JobError{
					Timestamp: time.Now(),
					Query:     "SELECT * FROM test",
					StmtType:  typedef.SelectStatementType,
				},
			},
		},
		{
			name: "test type",
			data: statementChData{
				ty: stmtlogger.TypeTest,
				Data: cqlDataMap{
					{2}: {
						partitionKeys: map[string][]any{"pk0": {"key2"}},
					},
				},
				Error: &joberror.JobError{
					Timestamp: time.Now(),
					Query:     "INSERT INTO test VALUES (?)",
					StmtType:  typedef.InsertStatementType,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.NotNil(t, tt.data.Data)
			assert.NotNil(t, tt.data.Error)
			assert.Contains(t, []stmtlogger.Type{stmtlogger.TypeOracle, stmtlogger.TypeTest}, tt.data.ty)
		})
	}
}

func TestCQLData_Structure(t *testing.T) {
	t.Parallel()

	data := cqlData{
		partitionKeys: map[string][]any{
			"pk0": {"test"},
			"pk1": {123},
		},
		mutationFragments: []json.RawMessage{
			json.RawMessage(`{"mutation":"test"}`),
		},
		statements: []json.RawMessage{
			json.RawMessage(`{"statement":"test"}`),
		},
	}

	assert.NotNil(t, data.partitionKeys)
	assert.NotEmpty(t, data.partitionKeys)

	for _, fragment := range data.mutationFragments {
		var decoded map[string]any
		err := json.Unmarshal(fragment, &decoded)
		assert.NoError(t, err)
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
	assert.NoError(t, err)

	mu.Lock()
	assert.Equal(t, 2, completed)
	mu.Unlock()
}

func TestCqlDataMap_MarshalJSON(t *testing.T) {
	var k1, k2 [32]byte
	copy(k1[:], bytes.Repeat([]byte{0xAB}, 32))
	copy(k2[:], bytes.Repeat([]byte{0xCD}, 32))

	m := cqlDataMap{
		k1: {partitionKeys: map[string][]any{"pk": {1}}, mutationFragments: []json.RawMessage{json.RawMessage(`{"a":1}`)}},
		k2: {partitionKeys: map[string][]any{"pk": {2}}, statements: []json.RawMessage{json.RawMessage(`{"b":2}`)}},
	}

	bs, err := json.Marshal(m)
	require.NoError(t, err)

	// It should be a JSON object with two keys (hex-encoded)
	var obj map[string]any
	require.NoError(t, json.Unmarshal(bs, &obj))
	assert.Len(t, obj, 2)
	// keys should be 64-char hex strings
	for k := range obj {
		assert.Equal(t, 64, len(k))
		_, err = os.ReadFile("/dev/null") // no-op to silence lint on err shadowing
		_ = err
	}
}

func TestStatementFlusher_WritesAndFlushes(t *testing.T) {
	lg := &Logger{logger: zaptest.NewLogger(t)}

	dir := t.TempDir()
	oraclePath := filepath.Join(dir, "oracle.jsonl")
	testPath := filepath.Join(dir, "test.jsonl")

	// reset counter
	// Note: prometheus counters cannot be set backwards; we'll only assert it increases
	beforeOracle := testutil.ToFloat64(metrics.StatementLoggerFlushes.WithLabelValues("oracle_file"))
	beforeTest := testutil.ToFloat64(metrics.StatementLoggerFlushes.WithLabelValues("test_file"))

	ch := make(chan statementChData, 2)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		lg.statementFlusher(ch, oraclePath, testPath)
	}()

	// Build one oracle and one test item
	je := &joberror.JobError{
		Timestamp: time.Now(),
		Query:     "SELECT 1",
		Message:   "boom",
	}
	val := cqlData{partitionKeys: map[string][]any{"pk": {"a"}}, statements: []json.RawMessage{json.RawMessage(`{"x":1}`)}}
	data := cqlDataMap{}
	var key [32]byte
	copy(key[:], bytes.Repeat([]byte{1}, 32))
	data[key] = val

	ch <- statementChData{ty: stmtlogger.TypeOracle, Data: data, Error: je}
	ch <- statementChData{ty: stmtlogger.TypeTest, Data: data, Error: je}
	close(ch)
	// Wait for flusher to finish to avoid races on files and metrics
	wg.Wait()

	// Read files and ensure they have exactly one line each and are valid JSON
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
		require.Equal(t, 1, len(lines))
		// Must be valid JSON and contain our query
		var line Line
		require.NoError(t, json.Unmarshal([]byte(lines[0]), &line))
		assert.Equal(t, "SELECT 1", line.Query)
		return lines[0]
	}

	oracleJSON := readAndCheck(oraclePath)
	testJSON := readAndCheck(testPath)
	assert.True(t, strings.Contains(oracleJSON, "SELECT 1"))
	assert.True(t, strings.Contains(testJSON, "SELECT 1"))

	// Counters should have incremented at least once for each file
	afterOracle := testutil.ToFloat64(metrics.StatementLoggerFlushes.WithLabelValues("oracle_file"))
	afterTest := testutil.ToFloat64(metrics.StatementLoggerFlushes.WithLabelValues("test_file"))
	assert.Greater(t, afterOracle, beforeOracle)
	assert.Greater(t, afterTest, beforeTest)
}

func TestFetchErrors_DedupAndFanout(t *testing.T) {
	lg := &Logger{logger: zaptest.NewLogger(t)}

	// Channel to receive items
	out := make(chan statementChData, 10)

	// Prepare two job errors with the same hash (by copying the same struct)
	base := &joberror.JobError{Timestamp: time.Now(), Query: "Q", Message: "M"}
	// Ensure deterministic identical hash by using same content
	je1 := *base
	je2 := *base

	// Replace poolCallback via fetchHook to avoid real DB calls and to observe fanout
	var calls atomic.Int64
	lg.fetchHook = func(_ context.Context, _ stmtlogger.Type, _ *joberror.JobError) (cqlDataMap, error) {
		calls.Add(1)
		return cqlDataMap{}, nil
	}

	// Start fetchErrors which fans out to both storages and deduplicates by hash
	in := make(chan *joberror.JobError, 2)
	go lg.fetchErrors(out, in)

	in <- &je1
	in <- &je2 // duplicate; should be ignored by dedupe
	close(in)

	// Drain output until it's closed
	var received int
	for item := range out {
		_ = item
		received++
	}

	// Expect exactly two calls (oracle + test) despite two inputs with same hash
	assert.Equal(t, int64(2), calls.Load())
	assert.Equal(t, 2, received)
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
	for i := 0; i < b.N; i++ {
		_ = GetScyllaStatementLogsKeyspace(keyspace)
	}
}

func BenchmarkGetScyllaStatementLogsTable(b *testing.B) {
	table := "test_table"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = GetScyllaStatementLogsTable(table)
	}
}

func BenchmarkBuildCreateTableQuery(b *testing.B) {
	partitionKeys := typedef.Columns{
		{Name: "pk0", Type: typedef.TypeText},
		{Name: "pk1", Type: typedef.TypeInt},
	}
	repl := replication.NewSimpleStrategy()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = buildCreateTableQuery("test_logs", "test_statements", partitionKeys, repl)
	}
}

func BenchmarkLine_Marshal(b *testing.B) {
	line := Line{
		PartitionKeys: map[string][]any{
			"pk0": {"key1"},
			"pk1": {123},
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
	for i := 0; i < b.N; i++ {
		_, _ = json.Marshal(line)
	}
}
