//go:build testing

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
//
//nolint:govet
package scylla

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
)

// TestCQLStatements_Insert_Comprehensive tests that all types of items are correctly inserted into Scylla
func TestCQLStatements_Insert_Comprehensive(t *testing.T) {
	t.Parallel()

	containers := testutils.TestContainers(t)
	logger := zaptest.NewLogger(t)

	session, err := newSession(containers.TestHosts, "", "", logger)
	require.NoError(t, err)
	t.Cleanup(session.Close)

	keyspace := testutils.GenerateUniqueKeyspaceName(t)
	table := "comprehensive_test"

	partitionKeys := typedef.Columns{
		{Name: "pk0", Type: typedef.TypeText},
		{Name: "pk1", Type: typedef.TypeInt},
	}

	cqlStmts, err := newStatements(
		session,
		func() (*gocql.Session, error) { return containers.Oracle, nil },
		func() (*gocql.Session, error) { return containers.Test, nil },
		keyspace, table,
		"test_ks", "test_table",
		partitionKeys,
		replication.NewSimpleStrategy(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = session.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", keyspace)).Exec()
	})

	testErr := errors.New("test error")

	tests := []struct {
		name            string
		item            stmtlogger.Item
		expectedColumns map[string]interface{}
	}{
		{
			name: "insert with nil error (Left)",
			item: stmtlogger.Item{
				Start: stmtlogger.Time{Time: time.Now()},
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {"test_key_1"},
					"pk1": {int32(100)},
				}),
				Error:         mo.Left[error, string](nil),
				Statement:     "SELECT * FROM test WHERE pk0 = ? AND pk1 = ?",
				Host:          "192.168.1.1",
				Type:          stmtlogger.TypeOracle,
				Values:        mo.Left[[]any, []byte]([]any{"value1", int32(123), "value2"}),
				Duration:      stmtlogger.Duration{Duration: 5 * time.Millisecond},
				Attempt:       1,
				GeminiAttempt: 1,
				StatementType: typedef.SelectStatementType,
			},
			expectedColumns: map[string]interface{}{
				"pk0":            "test_key_1",
				"pk1":            int32(100),
				"ty":             "oracle",
				"statement":      "SELECT * FROM test WHERE pk0 = ? AND pk1 = ?",
				"host":           "192.168.1.1",
				"attempt":        int16(1),
				"gemini_attempt": int16(1),
				"error":          "",
			},
		},
		{
			name: "insert with actual error (Left)",
			item: stmtlogger.Item{
				Start: stmtlogger.Time{Time: time.Now()},
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {"test_key_2"},
					"pk1": {int32(200)},
				}),
				Error:         mo.Left[error, string](testErr),
				Statement:     "INSERT INTO test (pk0, pk1, col1) VALUES (?, ?, ?)",
				Host:          "192.168.1.2",
				Type:          stmtlogger.TypeTest,
				Values:        mo.Left[[]any, []byte]([]any{"key2", int32(200), "data"}),
				Duration:      stmtlogger.Duration{Duration: 10 * time.Millisecond},
				Attempt:       2,
				GeminiAttempt: 1,
				StatementType: typedef.InsertStatementType,
			},
			expectedColumns: map[string]interface{}{
				"pk0":            "test_key_2",
				"pk1":            int32(200),
				"ty":             "test",
				"statement":      "INSERT INTO test (pk0, pk1, col1) VALUES (?, ?, ?)",
				"host":           "192.168.1.2",
				"attempt":        int16(2),
				"gemini_attempt": int16(1),
				"error":          "test error",
			},
		},
		{
			name: "insert with error string (Right)",
			item: stmtlogger.Item{
				Start: stmtlogger.Time{Time: time.Now()},
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {"test_key_3"},
					"pk1": {int32(300)},
				}),
				Error:         mo.Right[error, string]("custom error message"),
				Statement:     "UPDATE test SET col1 = ? WHERE pk0 = ? AND pk1 = ?",
				Host:          "192.168.1.3",
				Type:          stmtlogger.TypeOracle,
				Values:        mo.Left[[]any, []byte]([]any{"newval", "key3", int32(300)}),
				Duration:      stmtlogger.Duration{Duration: 15 * time.Millisecond},
				Attempt:       3,
				GeminiAttempt: 2,
				StatementType: typedef.UpdateStatementType,
			},
			expectedColumns: map[string]interface{}{
				"pk0":            "test_key_3",
				"pk1":            int32(300),
				"ty":             "oracle",
				"statement":      "UPDATE test SET col1 = ? WHERE pk0 = ? AND pk1 = ?",
				"host":           "192.168.1.3",
				"attempt":        int16(3),
				"gemini_attempt": int16(2),
				"error":          "custom error message",
			},
		},
		{
			name: "insert with serialized values (Right)",
			item: stmtlogger.Item{
				Start: stmtlogger.Time{Time: time.Now()},
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {"test_key_4"},
					"pk1": {int32(400)},
				}),
				Error:         mo.Left[error, string](nil),
				Statement:     "DELETE FROM test WHERE pk0 = ? AND pk1 = ?",
				Host:          "192.168.1.4",
				Type:          stmtlogger.TypeTest,
				Values:        mo.Right[[]any, []byte]([]byte(`{"serialized":"data"}`)),
				Duration:      stmtlogger.Duration{Duration: 20 * time.Millisecond},
				Attempt:       1,
				GeminiAttempt: 3,
				StatementType: typedef.DeleteSingleRowType,
			},
			expectedColumns: map[string]interface{}{
				"pk0":            "test_key_4",
				"pk1":            int32(400),
				"ty":             "test",
				"statement":      "DELETE FROM test WHERE pk0 = ? AND pk1 = ?",
				"host":           "192.168.1.4",
				"attempt":        int16(1),
				"gemini_attempt": int16(3),
				"error":          "",
			},
		},
		{
			name: "insert with nil values (Left)",
			item: stmtlogger.Item{
				Start: stmtlogger.Time{Time: time.Now()},
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {"test_key_5"},
					"pk1": {int32(500)},
				}),
				Error:         mo.Left[error, string](nil),
				Statement:     "SELECT COUNT(*) FROM test",
				Host:          "192.168.1.5",
				Type:          stmtlogger.TypeOracle,
				Values:        mo.Left[[]any, []byte](nil),
				Duration:      stmtlogger.Duration{Duration: 1 * time.Millisecond},
				Attempt:       1,
				GeminiAttempt: 1,
				StatementType: typedef.SelectStatementType,
			},
			expectedColumns: map[string]interface{}{
				"pk0":            "test_key_5",
				"pk1":            int32(500),
				"ty":             "oracle",
				"statement":      "SELECT COUNT(*) FROM test",
				"host":           "192.168.1.5",
				"attempt":        int16(1),
				"gemini_attempt": int16(1),
				"error":          "",
			},
		},
		{
			name: "insert with complex values",
			item: stmtlogger.Item{
				Start: stmtlogger.Time{Time: time.Now()},
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {"test_key_6"},
					"pk1": {int32(600)},
				}),
				Error:         mo.Left[error, string](nil),
				Statement:     "INSERT INTO test (pk0, pk1, col1, col2, col3) VALUES (?, ?, ?, ?, ?)",
				Host:          "192.168.1.6",
				Type:          stmtlogger.TypeTest,
				Values:        mo.Left[[]any, []byte]([]any{"key6", int32(600), 123.456, true, []byte("binary")}),
				Duration:      stmtlogger.Duration{Duration: 25 * time.Millisecond},
				Attempt:       4,
				GeminiAttempt: 2,
				StatementType: typedef.InsertStatementType,
			},
			expectedColumns: map[string]interface{}{
				"pk0":            "test_key_6",
				"pk1":            int32(600),
				"ty":             "test",
				"statement":      "INSERT INTO test (pk0, pk1, col1, col2, col3) VALUES (?, ?, ?, ?, ?)",
				"host":           "192.168.1.6",
				"attempt":        int16(4),
				"gemini_attempt": int16(2),
				"error":          "",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := cqlStmts.Insert(t.Context(), tt.item)
			require.NoError(t, err, "Insert should succeed")

			// Verify the data was inserted correctly
			query := fmt.Sprintf(
				"SELECT pk0, pk1, ty, statement, host, attempt, gemini_attempt, error, dur FROM %s.%s WHERE pk0 = ? AND pk1 = ? AND ty = ?",
				keyspace, table,
			)

			var (
				pk0           string
				pk1           int32
				ty            string
				statement     string
				host          string
				attempt       int16
				geminiAttempt int16
				errorStr      string
				dur           time.Duration
			)

			err = session.Query(
				query,
				tt.item.PartitionKeys.Get("pk0")[0],
				tt.item.PartitionKeys.Get("pk1")[0],
				tt.item.Type,
			).Scan(&pk0, &pk1, &ty, &statement, &host, &attempt, &geminiAttempt, &errorStr, &dur)

			require.NoError(t, err, "Should be able to query inserted data")

			// Verify all columns
			assert.Equal(t, tt.expectedColumns["pk0"], pk0, "pk0 should match")
			assert.Equal(t, tt.expectedColumns["pk1"], pk1, "pk1 should match")
			assert.Equal(t, tt.expectedColumns["ty"], ty, "type should match")
			assert.Equal(t, tt.expectedColumns["statement"], statement, "statement should match")
			assert.Equal(t, tt.expectedColumns["host"], host, "host should match")
			assert.Equal(t, tt.expectedColumns["attempt"], attempt, "attempt should match")
			assert.Equal(t, tt.expectedColumns["gemini_attempt"], geminiAttempt, "gemini_attempt should match")
			assert.Equal(t, tt.expectedColumns["error"], errorStr, "error should match")
			assert.Equal(t, tt.item.Duration.Duration, dur, "duration should match")

			// Verify values column
			var values []string
			err = session.Query(
				fmt.Sprintf("SELECT values FROM %s.%s WHERE pk0 = ? AND pk1 = ? AND ty = ?", keyspace, table),
				tt.item.PartitionKeys.Get("pk0")[0],
				tt.item.PartitionKeys.Get("pk1")[0],
				tt.item.Type,
			).Scan(&values)

			require.NoError(t, err, "Should be able to query values")
			if tt.item.Values.IsLeft() && tt.item.Values.MustLeft() != nil {
				assert.NotEmpty(t, values, "values should not be empty for Left with data")
				assert.Len(t, values, len(tt.item.Values.MustLeft()), "values length should match")
			} else if tt.item.Values.IsRight() {
				assert.NotEmpty(t, values, "values should not be empty for Right")
				assert.Len(t, values, 1, "serialized values should have single entry")
			}
		})
	}
}

// TestCQLStatements_Fetch_AllStatementTypes tests fetching for all supported statement types
func TestCQLStatements_Fetch_AllStatementTypes(t *testing.T) {
	t.Parallel()

	containers := testutils.TestContainers(t)
	logger := zaptest.NewLogger(t)

	// Create test keyspace and table in test/oracle sessions
	testKS := testutils.GenerateUniqueKeyspaceName(t)
	testTable := "test_table"

	// Create schema in both oracle and test
	createKS := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
		testKS,
	)
	createTable := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s.%s (pk0 text, pk1 int, col1 text, col2 int, PRIMARY KEY ((pk0, pk1)))",
		testKS, testTable,
	)

	require.NoError(t, containers.Oracle.Query(createKS).Exec())
	require.NoError(t, containers.Oracle.Query(createTable).Exec())
	require.NoError(t, containers.Test.Query(createKS).Exec())
	require.NoError(t, containers.Test.Query(createTable).Exec())

	t.Cleanup(func() {
		_ = containers.Oracle.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", testKS)).Exec()
		_ = containers.Test.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", testKS)).Exec()
	})

	// Insert test data into both oracle and test
	testKeys := []struct {
		pk0 string
		pk1 int32
	}{
		{"fetch_key_1", 1},
		{"fetch_key_2", 2},
		{"fetch_key_3", 3},
	}

	for _, key := range testKeys {
		require.NoError(t, containers.Oracle.Query(
			fmt.Sprintf("INSERT INTO %s.%s (pk0, pk1, col1, col2) VALUES (?, ?, ?, ?)", testKS, testTable),
			key.pk0, key.pk1, "oracle_value", 100,
		).Exec())
		require.NoError(t, containers.Test.Query(
			fmt.Sprintf("INSERT INTO %s.%s (pk0, pk1, col1, col2) VALUES (?, ?, ?, ?)", testKS, testTable),
			key.pk0, key.pk1, "test_value", 200,
		).Exec())
	}

	// Create statement logger
	session, err := newSession(containers.TestHosts, "", "", logger)
	require.NoError(t, err)
	t.Cleanup(session.Close)

	logsKS := testutils.GenerateUniqueKeyspaceName(t)
	logsTable := "fetch_statements"

	partitionKeys := typedef.Columns{
		{Name: "pk0", Type: typedef.TypeText},
		{Name: "pk1", Type: typedef.TypeInt},
	}

	cqlStmts, err := newStatements(
		session,
		func() (*gocql.Session, error) { return containers.Oracle, nil },
		func() (*gocql.Session, error) { return containers.Test, nil },
		logsKS, logsTable,
		testKS, testTable,
		partitionKeys,
		replication.NewSimpleStrategy(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = session.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", logsKS)).Exec()
	})

	tests := []struct {
		name         string
		setupFunc    func(t *testing.T) *joberror.JobError
		validateFunc func(t *testing.T, result map[[32]byte]cqlData, jobErr *joberror.JobError)
	}{
		{
			name: "fetch SELECT statement (oracle and test)",
			setupFunc: func(t *testing.T) *joberror.JobError {
				t.Helper()
				// Insert statement logs for both oracle and test
				for _, ty := range []stmtlogger.Type{stmtlogger.TypeOracle, stmtlogger.TypeTest} {
					item := stmtlogger.Item{
						Start: stmtlogger.Time{Time: time.Now()},
						PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
							"pk0": {"fetch_key_1"},
							"pk1": {int32(1)},
						}),
						Error:         mo.Left[error, string](nil),
						Statement:     "SELECT * FROM test WHERE pk0 = ? AND pk1 = ?",
						Host:          "127.0.0.1",
						Type:          ty,
						Values:        mo.Left[[]any, []byte]([]any{"fetch_key_1", int32(1)}),
						Duration:      stmtlogger.Duration{Duration: time.Millisecond},
						Attempt:       1,
						GeminiAttempt: 1,
						StatementType: typedef.SelectStatementType,
					}
					err := cqlStmts.Insert(t.Context(), item)
					require.NoError(t, err)
				}

				return &joberror.JobError{
					Timestamp: time.Now(),
					Query:     "SELECT * FROM test WHERE pk0 = ? AND pk1 = ?",
					Message:   "test error",
					StmtType:  typedef.SelectStatementType,
					PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
						"pk0": {"fetch_key_1"},
						"pk1": {int32(1)},
					}),
				}
			},
			validateFunc: func(t *testing.T, result map[[32]byte]cqlData, jobErr *joberror.JobError) {
				t.Helper()
				require.NotNil(t, result, "result should not be nil")
				require.NotEmpty(t, result, "result should not be empty")

				// Should have data for the partition
				data, exists := result[jobErr.Hash()]
				require.True(t, exists, "should have data for job error hash")
				assert.NotEmpty(t, data.statements, "should have statements")
				assert.NotEmpty(t, data.mutationFragments, "should have mutation fragments")
				assert.NotEmpty(t, data.partitionKeys, "should have partition keys")
				assert.Equal(t, []any{"fetch_key_1"}, data.partitionKeys["pk0"])
				assert.Equal(t, []any{int32(1)}, data.partitionKeys["pk1"])
			},
		},
		{
			name: "fetch INSERT statement",
			setupFunc: func(t *testing.T) *joberror.JobError {
				t.Helper()
				for _, ty := range []stmtlogger.Type{stmtlogger.TypeOracle, stmtlogger.TypeTest} {
					item := stmtlogger.Item{
						Start: stmtlogger.Time{Time: time.Now()},
						PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
							"pk0": {"fetch_key_2"},
							"pk1": {int32(2)},
						}),
						Error:         mo.Left[error, string](nil),
						Statement:     "INSERT INTO test VALUES (?, ?, ?, ?)",
						Host:          "127.0.0.1",
						Type:          ty,
						Values:        mo.Left[[]any, []byte]([]any{"fetch_key_2", int32(2), "value", 100}),
						Duration:      stmtlogger.Duration{Duration: time.Millisecond},
						Attempt:       1,
						GeminiAttempt: 1,
						StatementType: typedef.InsertStatementType,
					}
					err := cqlStmts.Insert(t.Context(), item)
					require.NoError(t, err)
				}

				return &joberror.JobError{
					Timestamp: time.Now(),
					Query:     "INSERT INTO test VALUES (?, ?, ?, ?)",
					Message:   "insert error",
					StmtType:  typedef.InsertStatementType,
					PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
						"pk0": {"fetch_key_2"},
						"pk1": {int32(2)},
					}),
				}
			},
			validateFunc: func(t *testing.T, result map[[32]byte]cqlData, jobErr *joberror.JobError) {
				t.Helper()
				require.NotEmpty(t, result, "should have results")
				data := result[jobErr.Hash()]
				assert.NotEmpty(t, data.statements, "should have statements")
				assert.NotEmpty(t, data.mutationFragments, "should have mutation fragments")
			},
		},
		{
			name: "fetch UPDATE statement",
			setupFunc: func(t *testing.T) *joberror.JobError {
				t.Helper()
				for _, ty := range []stmtlogger.Type{stmtlogger.TypeOracle, stmtlogger.TypeTest} {
					item := stmtlogger.Item{
						Start: stmtlogger.Time{Time: time.Now()},
						PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
							"pk0": {"fetch_key_3"},
							"pk1": {int32(3)},
						}),
						Error:         mo.Right[error, string]("update error"),
						Statement:     "UPDATE test SET col1 = ? WHERE pk0 = ? AND pk1 = ?",
						Host:          "127.0.0.1",
						Type:          ty,
						Values:        mo.Left[[]any, []byte]([]any{"new_value", "fetch_key_3", int32(3)}),
						Duration:      stmtlogger.Duration{Duration: 2 * time.Millisecond},
						Attempt:       2,
						GeminiAttempt: 1,
						StatementType: typedef.UpdateStatementType,
					}
					err := cqlStmts.Insert(t.Context(), item)
					require.NoError(t, err)
				}

				return &joberror.JobError{
					Timestamp: time.Now(),
					Query:     "UPDATE test SET col1 = ? WHERE pk0 = ? AND pk1 = ?",
					Message:   "update error",
					StmtType:  typedef.UpdateStatementType,
					PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
						"pk0": {"fetch_key_3"},
						"pk1": {int32(3)},
					}),
				}
			},
			validateFunc: func(t *testing.T, result map[[32]byte]cqlData, jobErr *joberror.JobError) {
				t.Helper()
				require.NotEmpty(t, result, "should have results")
				data := result[jobErr.Hash()]
				assert.NotEmpty(t, data.statements, "should have statements")
			},
		},
		{
			name: "fetch DELETE statement",
			setupFunc: func(t *testing.T) *joberror.JobError {
				t.Helper()
				pkValues := typedef.NewValuesFromMap(map[string][]any{
					"pk0": {"delete_key"},
					"pk1": {int32(999)},
				})

				for _, ty := range []stmtlogger.Type{stmtlogger.TypeOracle, stmtlogger.TypeTest} {
					item := stmtlogger.Item{
						Start:         stmtlogger.Time{Time: time.Now()},
						PartitionKeys: pkValues,
						Error:         mo.Left[error, string](nil),
						Statement:     "DELETE FROM test WHERE pk0 = ? AND pk1 = ?",
						Host:          "127.0.0.1",
						Type:          ty,
						Values:        mo.Left[[]any, []byte]([]any{"delete_key", int32(999)}),
						Duration:      stmtlogger.Duration{Duration: time.Millisecond},
						Attempt:       1,
						GeminiAttempt: 1,
						StatementType: typedef.DeleteSingleRowType,
					}
					err := cqlStmts.Insert(t.Context(), item)
					require.NoError(t, err)
				}

				return &joberror.JobError{
					Timestamp:     time.Now(),
					Query:         "DELETE FROM test WHERE pk0 = ? AND pk1 = ?",
					Message:       "delete error",
					StmtType:      typedef.DeleteSingleRowType,
					PartitionKeys: pkValues,
				}
			},
			validateFunc: func(t *testing.T, result map[[32]byte]cqlData, jobErr *joberror.JobError) {
				t.Helper()
				require.NotEmpty(t, result, "should have results")
				data := result[jobErr.Hash()]
				assert.NotEmpty(t, data.partitionKeys, "should have partition keys")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			jobErr := tt.setupFunc(t)

			// Fetch from Oracle
			oracleResult, err := cqlStmts.Fetch(t.Context(), stmtlogger.TypeOracle, jobErr)
			require.NoError(t, err, "Oracle fetch should succeed")
			tt.validateFunc(t, oracleResult, jobErr)

			// Fetch from Test
			testResult, err := cqlStmts.Fetch(t.Context(), stmtlogger.TypeTest, jobErr)
			require.NoError(t, err, "Test fetch should succeed")
			tt.validateFunc(t, testResult, jobErr)
		})
	}
}

// TestCQLStatements_Fetch_MultiPartition tests fetching for multi-partition statements
func TestCQLStatements_Fetch_MultiPartition_Comprehensive(t *testing.T) {
	t.Parallel()

	containers := testutils.TestContainers(t)
	logger := zaptest.NewLogger(t)

	// Use shorter keyspace name to avoid 48 character limit
	testKS := fmt.Sprintf("ks_mp_%d", time.Now().UnixNano()%1000000)
	testTable := "multi_partition_table"

	createKS := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
		testKS,
	)
	createTable := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s.%s (pk0 text, col1 text, PRIMARY KEY (pk0))",
		testKS, testTable,
	)

	require.NoError(t, containers.Oracle.Query(createKS).Exec())
	require.NoError(t, containers.Oracle.Query(createTable).Exec())
	require.NoError(t, containers.Test.Query(createKS).Exec())
	require.NoError(t, containers.Test.Query(createTable).Exec())

	t.Cleanup(func() {
		_ = containers.Oracle.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", testKS)).Exec()
		_ = containers.Test.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", testKS)).Exec()
	})

	// Insert multiple partition data
	multiKeys := []string{"multi_key_1", "multi_key_2", "multi_key_3", "multi_key_4", "multi_key_5"}
	for _, key := range multiKeys {
		require.NoError(t, containers.Oracle.Query(
			fmt.Sprintf("INSERT INTO %s.%s (pk0, col1) VALUES (?, ?)", testKS, testTable),
			key, "oracle_"+key,
		).Exec())
		require.NoError(t, containers.Test.Query(
			fmt.Sprintf("INSERT INTO %s.%s (pk0, col1) VALUES (?, ?)", testKS, testTable),
			key, "test_"+key,
		).Exec())
	}

	session, err := newSession(containers.TestHosts, "", "", logger)
	require.NoError(t, err)
	t.Cleanup(session.Close)

	// Use shorter keyspace name to avoid 48 character limit
	logsKS := fmt.Sprintf("ks_mpl_%d", time.Now().UnixNano()%1000000)
	logsTable := "multi_partition_logs"

	partitionKeys := typedef.Columns{
		{Name: "pk0", Type: typedef.TypeText},
	}

	cqlStmts, err := newStatements(
		session,
		func() (*gocql.Session, error) { return containers.Oracle, nil },
		func() (*gocql.Session, error) { return containers.Test, nil },
		logsKS, logsTable,
		testKS, testTable,
		partitionKeys,
		replication.NewSimpleStrategy(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = session.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", logsKS)).Exec()
	})

	tests := []struct {
		name         string
		stmtType     typedef.StatementType
		partitionNum int
	}{
		{
			name:         "SELECT multi-partition with 3 partitions",
			stmtType:     typedef.SelectMultiPartitionType,
			partitionNum: 3,
		},
		{
			name:         "SELECT multi-partition with 5 partitions",
			stmtType:     typedef.SelectMultiPartitionType,
			partitionNum: 5,
		},
		{
			name:         "DELETE multi-partition with 4 partitions",
			stmtType:     typedef.DeleteMultiplePartitionsType,
			partitionNum: 4,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Create partition keys for multi-partition statement
			pkValues := make([]any, tt.partitionNum)
			for i := 0; i < tt.partitionNum; i++ {
				pkValues[i] = multiKeys[i]
			}

			// Multi-partition format for the job error
			partitionKeyValues := typedef.NewValuesFromMap(map[string][]any{
				"pk0": pkValues,
			})

			// Insert statement logs for each partition separately
			// The Insert method expects one partition at a time
			for _, ty := range []stmtlogger.Type{stmtlogger.TypeOracle, stmtlogger.TypeTest} {
				for i := range tt.partitionNum {
					item := stmtlogger.Item{
						Start: stmtlogger.Time{Time: time.Now()},
						PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
							"pk0": {multiKeys[i]},
						}),
						Error:         mo.Left[error, string](nil),
						Statement:     fmt.Sprintf("SELECT * FROM test WHERE pk0 IN (%s)", generatePlaceholders(tt.partitionNum)),
						Host:          "127.0.0.1",
						Type:          ty,
						Values:        mo.Left[[]any, []byte](pkValues),
						Duration:      stmtlogger.Duration{Duration: time.Millisecond},
						Attempt:       1,
						GeminiAttempt: 1,
						StatementType: tt.stmtType,
					}
					err := cqlStmts.Insert(t.Context(), item)
					require.NoError(t, err)
				}
			}

			jobErr := &joberror.JobError{
				Timestamp:     time.Now(),
				Query:         fmt.Sprintf("SELECT * FROM test WHERE pk0 IN (%s)", generatePlaceholders(tt.partitionNum)),
				Message:       "multi-partition error",
				StmtType:      tt.stmtType,
				PartitionKeys: partitionKeyValues,
			}

			// Fetch from both oracle and test
			for _, ty := range []stmtlogger.Type{stmtlogger.TypeOracle, stmtlogger.TypeTest} {
				result, err := cqlStmts.Fetch(t.Context(), ty, jobErr)
				require.NoError(t, err, "Fetch should succeed for %s", ty)
				assert.NotEmpty(t, result, "Should have results for %s", ty)

				// Verify we have data for multiple partitions
				assert.LessOrEqual(t, 1, len(result), "Should have at least 1 partition result")

				// Verify each partition has the expected data
				for hash, data := range result {
					assert.NotEmpty(t, data.partitionKeys, "Hash %v should have partition keys", hash)
					assert.NotEmpty(t, data.statements, "Hash %v should have statements", hash)
				}
			}
		})
	}
}

// TestCQLStatements_Insert_ConcurrentWrites tests that concurrent inserts work correctly
func TestCQLStatements_Insert_ConcurrentWrites(t *testing.T) {
	t.Parallel()

	containers := testutils.TestContainers(t)
	logger := zaptest.NewLogger(t)

	session, err := newSession(containers.TestHosts, "", "", logger)
	require.NoError(t, err)
	t.Cleanup(session.Close)

	keyspace := testutils.GenerateUniqueKeyspaceName(t)
	table := "concurrent_test"

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
		replication.NewSimpleStrategy(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = session.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", keyspace)).Exec()
	})

	numWorkers := 10
	itemsPerWorker := 10
	done := make(chan struct{})

	// Launch concurrent workers
	for i := 0; i < numWorkers; i++ {
		go func(workerID int) {
			defer func() { done <- struct{}{} }()

			for j := 0; j < itemsPerWorker; j++ {
				item := stmtlogger.Item{
					Start: stmtlogger.Time{Time: time.Now()},
					PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
						"pk0": {fmt.Sprintf("worker_%d_item_%d", workerID, j)},
					}),
					Error:         mo.Left[error, string](nil),
					Statement:     fmt.Sprintf("INSERT INTO test VALUES (worker_%d, item_%d)", workerID, j),
					Host:          fmt.Sprintf("192.168.1.%d", workerID),
					Type:          stmtlogger.TypeOracle,
					Values:        mo.Left[[]any, []byte]([]any{workerID, j}),
					Duration:      stmtlogger.Duration{Duration: time.Millisecond},
					Attempt:       1,
					GeminiAttempt: 1,
					StatementType: typedef.InsertStatementType,
				}

				assert.NoError(t, cqlStmts.Insert(t.Context(), item), "Worker %d insert %d should succeed", workerID, j)
			}
		}(i)
	}

	// Wait for all workers to complete
	for i := 0; i < numWorkers; i++ {
		<-done
	}

	// Verify total count
	var count int64
	err = session.Query(fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", keyspace, table)).Scan(&count)
	require.NoError(t, err)
	assert.Equal(t, int64(numWorkers*itemsPerWorker), count, "Should have all items inserted")
}

// Helper function to generate CQL placeholders
func generatePlaceholders(n int) string {
	if n == 0 {
		return ""
	}
	result := "?"
	for i := 1; i < n; i++ {
		result += ",?"
	}
	return result
}

// TestCQLStatements_Fetch_WithVariousErrors tests fetching with different error scenarios
func TestCQLStatements_Fetch_WithVariousErrors(t *testing.T) {
	t.Parallel()

	containers := testutils.TestContainers(t)
	logger := zaptest.NewLogger(t)

	testKS := testutils.GenerateUniqueKeyspaceName(t)
	testTable := "error_test"

	createKS := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
		testKS,
	)
	createTable := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s.%s (pk0 text, col1 text, PRIMARY KEY (pk0))",
		testKS, testTable,
	)

	require.NoError(t, containers.Oracle.Query(createKS).Exec())
	require.NoError(t, containers.Oracle.Query(createTable).Exec())
	require.NoError(t, containers.Test.Query(createKS).Exec())
	require.NoError(t, containers.Test.Query(createTable).Exec())

	t.Cleanup(func() {
		_ = containers.Oracle.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", testKS)).Exec()
		_ = containers.Test.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", testKS)).Exec()
	})

	// Insert test data
	require.NoError(t, containers.Oracle.Query(
		fmt.Sprintf("INSERT INTO %s.%s (pk0, col1) VALUES (?, ?)", testKS, testTable),
		"error_key", "value",
	).Exec())
	require.NoError(t, containers.Test.Query(
		fmt.Sprintf("INSERT INTO %s.%s (pk0, col1) VALUES (?, ?)", testKS, testTable),
		"error_key", "value",
	).Exec())

	session, err := newSession(containers.TestHosts, "", "", logger)
	require.NoError(t, err)
	t.Cleanup(session.Close)

	logsKS := testutils.GenerateUniqueKeyspaceName(t)
	logsTable := "error_logs"

	partitionKeys := typedef.Columns{
		{Name: "pk0", Type: typedef.TypeText},
	}

	cqlStmts, err := newStatements(
		session,
		func() (*gocql.Session, error) { return containers.Oracle, nil },
		func() (*gocql.Session, error) { return containers.Test, nil },
		logsKS, logsTable,
		testKS, testTable,
		partitionKeys,
		replication.NewSimpleStrategy(),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = session.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", logsKS)).Exec()
	})

	tests := []struct {
		name      string
		error     mo.Either[error, string]
		expectErr bool
	}{
		{
			name:      "timeout error",
			error:     mo.Left[error, string](gocql.ErrTimeoutNoResponse),
			expectErr: false,
		},
		{
			name:      "unavailable error",
			error:     mo.Left[error, string](gocql.ErrUnavailable),
			expectErr: false,
		},
		{
			name:      "custom error string",
			error:     mo.Right[error, string]("custom error from test"),
			expectErr: false,
		},
		{
			name:      "nil error",
			error:     mo.Left[error, string](nil),
			expectErr: false,
		},
	}

	for i, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			pkValue := fmt.Sprintf("error_key_%d", i)

			// Insert statement log with specific error
			item := stmtlogger.Item{
				Start: stmtlogger.Time{Time: time.Now()},
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {pkValue},
				}),
				Error:         tt.error,
				Statement:     "SELECT * FROM test WHERE pk0 = ?",
				Host:          "127.0.0.1",
				Type:          stmtlogger.TypeOracle,
				Values:        mo.Left[[]any, []byte]([]any{pkValue}),
				Duration:      stmtlogger.Duration{Duration: time.Millisecond},
				Attempt:       1,
				GeminiAttempt: 1,
				StatementType: typedef.SelectStatementType,
			}

			require.NoError(t, cqlStmts.Insert(t.Context(), item))

			// Verify the error was stored correctly
			var storedError string
			err = session.Query(
				fmt.Sprintf("SELECT error FROM %s.%s WHERE pk0 = ? AND ty = ?", logsKS, logsTable),
				pkValue,
				stmtlogger.TypeOracle,
			).Scan(&storedError)

			require.NoError(t, err)

			//nolint:gocritic
			if tt.error.IsLeft() && tt.error.MustLeft() != nil {
				assert.Equal(t, tt.error.MustLeft().Error(), storedError)
			} else if tt.error.IsRight() {
				assert.Equal(t, tt.error.MustRight(), storedError)
			} else {
				assert.Empty(t, storedError)
			}
		})
	}
}
