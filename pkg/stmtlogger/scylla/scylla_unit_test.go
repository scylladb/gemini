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
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/workpool"
)

// Unit tests - no database connection required

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
				Data: map[[32]byte]cqlData{
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
				Data: map[[32]byte]cqlData{
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
	pool := workpool.New(10)
	defer func() {
		_ = pool.Close()
	}()

	mockLogger := &Logger{
		logger: logger,
		pool:   pool,
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
	t.Parallel()

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
