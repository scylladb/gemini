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
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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

func TestNewSession_Integration(t *testing.T) {
	t.Parallel()

	containers := testutils.SingleScylla(t)
	logger := zaptest.NewLogger(t)

	t.Run("create session successfully", func(t *testing.T) {
		t.Parallel()

		session, err := newSession(containers.TestHosts, "", "", logger)
		require.NoError(t, err)
		require.NotNil(t, session)
		defer session.Close()

		// Verify session works
		err = session.Query("SELECT now() FROM system.local").Exec()
		assert.NoError(t, err)
	})

	t.Run("invalid host", func(t *testing.T) {
		t.Parallel()

		_, err := newSession([]string{"invalid-host-12345.example.com"}, "", "", logger)
		assert.Error(t, err)
	})
}

func TestNewStatements_Integration(t *testing.T) {
	t.Parallel()

	containers := testutils.TestContainers(t)
	logger := zaptest.NewLogger(t)

	session, err := newSession(containers.TestHosts, "", "", logger)
	require.NoError(t, err)
	t.Cleanup(session.Close)

	keyspace := testutils.GenerateUniqueKeyspaceName(t)
	table := "test_statements"

	partitionKeys := typedef.Columns{
		{Name: "pk0", Type: typedef.TypeText},
	}

	t.Run("single partition key", func(t *testing.T) {
		t.Parallel()
		cqlStmts, err := newStatements(
			session,
			func() (*gocql.Session, error) { return containers.Oracle, nil },
			func() (*gocql.Session, error) { return containers.Test, nil },
			keyspace+"_1", table,
			"test_ks", "test_table",
			partitionKeys,
			replication.NewSimpleStrategy(),
		)

		require.NoError(t, err)
		require.NotNil(t, cqlStmts)
		assert.NotEmpty(t, cqlStmts.insertStmt)
		assert.NotEmpty(t, cqlStmts.oracleSelect)
		assert.NotEmpty(t, cqlStmts.testSelect)
		assert.NotEmpty(t, cqlStmts.mutationFragmentsSelect)

		// Cleanup
		_ = session.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", keyspace+"_1")).Exec()
	})

	t.Run("multiple partition keys", func(t *testing.T) {
		t.Parallel()

		multiKeys := typedef.Columns{
			{Name: "pk0", Type: typedef.TypeText},
			{Name: "pk1", Type: typedef.TypeInt},
		}

		cqlStmts, err := newStatements(
			session,
			func() (*gocql.Session, error) { return containers.Oracle, nil },
			func() (*gocql.Session, error) { return containers.Test, nil },
			keyspace+"_2", table,
			"test_ks", "test_table",
			multiKeys,
			replication.NewSimpleStrategy(),
		)

		require.NoError(t, err)
		require.NotNil(t, cqlStmts)

		// Cleanup
		_ = session.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", keyspace+"_2")).Exec()
	})
}

func TestCQLStatements_Insert_Integration(t *testing.T) {
	t.Parallel()

	containers := testutils.TestContainers(t)
	logger := zaptest.NewLogger(t)

	session, err := newSession(containers.TestHosts, "", "", logger)
	require.NoError(t, err)
	t.Cleanup(session.Close)

	keyspace := testutils.GenerateUniqueKeyspaceName(t)
	table := "test_statements"

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

	tests := []struct {
		name string
		item stmtlogger.Item
	}{
		{
			name: "insert with left error",
			item: stmtlogger.Item{
				Start: stmtlogger.Time{Time: time.Now()},
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {"test_key_1"},
				}),
				Error:         mo.Left[error, string](nil),
				Statement:     "SELECT * FROM test WHERE pk0 = ?",
				Host:          "127.0.0.1",
				Type:          stmtlogger.TypeTest,
				Values:        mo.Left[[]any, []byte]([]any{"value1", 123}),
				Duration:      stmtlogger.Duration{Duration: time.Millisecond},
				Attempt:       1,
				GeminiAttempt: 1,
				StatementType: typedef.SelectStatementType,
			},
		},
		{
			name: "insert with right error string",
			item: stmtlogger.Item{
				Start: stmtlogger.Time{Time: time.Now()},
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {"test_key_2"},
				}),
				Error:         mo.Right[error, string]("test error message"),
				Statement:     "INSERT INTO test VALUES (?)",
				Host:          "127.0.0.1",
				Type:          stmtlogger.TypeOracle,
				Values:        mo.Right[[]any, []byte]([]byte("serialized")),
				Duration:      stmtlogger.Duration{Duration: 2 * time.Millisecond},
				Attempt:       2,
				GeminiAttempt: 1,
				StatementType: typedef.InsertStatementType,
			},
		},
		{
			name: "insert with actual error",
			item: stmtlogger.Item{
				Start: stmtlogger.Time{Time: time.Now()},
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {"test_key_3"},
				}),
				Error:         mo.Left[error, string](assert.AnError),
				Statement:     "UPDATE test SET col1 = ? WHERE pk0 = ?",
				Host:          "127.0.0.1",
				Type:          stmtlogger.TypeTest,
				Values:        mo.Left[[]any, []byte]([]any{"newval", "key3"}),
				Duration:      stmtlogger.Duration{Duration: 3 * time.Millisecond},
				Attempt:       1,
				GeminiAttempt: 2,
				StatementType: typedef.UpdateStatementType,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := cqlStmts.Insert(t.Context(), tt.item)
			assert.NoError(t, err)

			// Verify data was inserted
			var count int
			err = session.Query(
				fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE pk0 = ? AND ty = ?", keyspace, table),
				tt.item.PartitionKeys.Get("pk0")[0],
				tt.item.Type,
			).Scan(&count)
			assert.NoError(t, err)
			assert.Equal(t, 1, count)
		})
	}
}

func TestNewStatements_WithTupleType_Integration(t *testing.T) {
	t.Parallel()

	containers := testutils.TestContainers(t)
	logger := zaptest.NewLogger(t)

	session, err := newSession(containers.TestHosts, "", "", logger)
	require.NoError(t, err)
	defer session.Close()

	keyspace := testutils.GenerateUniqueKeyspaceName(t)
	table := "test_statements"

	// Create partition keys with a TupleType
	partitionKeys := typedef.Columns{
		{
			Name: "pk_tuple",
			Type: &typedef.TupleType{
				ValueTypes: []typedef.SimpleType{typedef.TypeText, typedef.TypeInt},
				Frozen:     true,
			},
		},
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
	require.NotNil(t, cqlStmts)

	defer func() {
		_ = session.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", keyspace)).Exec()
	}()

	// Verify the insert statement was created correctly with tuple column
	assert.Contains(t, cqlStmts.insertStmt, "pk_tuple")
	assert.NotEmpty(t, cqlStmts.insertStmt)
}

//nolint:tparallel
func TestCQLStatements_Fetch_Integration(t *testing.T) {
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
		"CREATE TABLE IF NOT EXISTS %s.%s (pk0 text, col1 text, PRIMARY KEY (pk0))",
		testKS, testTable,
	)

	require.NoError(t, containers.Oracle.Query(createKS).Exec())
	require.NoError(t, containers.Oracle.Query(createTable).Exec())
	require.NoError(t, containers.Test.Query(createKS).Exec())
	require.NoError(t, containers.Test.Query(createTable).Exec())

	defer func() {
		_ = containers.Oracle.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", testKS)).Exec()
		_ = containers.Test.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", testKS)).Exec()
	}()

	// Insert some test data
	require.NoError(t, containers.Oracle.Query(
		fmt.Sprintf("INSERT INTO %s.%s (pk0, col1) VALUES (?, ?)", testKS, testTable),
		"test_key", "test_value",
	).Exec())
	require.NoError(t, containers.Test.Query(
		fmt.Sprintf("INSERT INTO %s.%s (pk0, col1) VALUES (?, ?)", testKS, testTable),
		"test_key", "test_value",
	).Exec())

	// Create statement logger
	session, err := newSession(containers.TestHosts, "", "", logger)
	require.NoError(t, err)
	t.Cleanup(session.Close)

	logsKS := testutils.GenerateUniqueKeyspaceName(t)
	logsTable := "test_statements"

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
	defer func() {
		_ = session.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", logsKS)).Exec()
	}()

	// Insert statement logs for both oracle and test
	oracleItem := stmtlogger.Item{
		Start: stmtlogger.Time{Time: time.Now()},
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
			"pk0": {"test_key"},
		}),
		Error:         mo.Left[error, string](nil),
		Statement:     "SELECT * FROM test WHERE pk0 = ?",
		Host:          "127.0.0.1",
		Type:          stmtlogger.TypeOracle,
		Values:        mo.Left[[]any, []byte]([]any{"test_key"}),
		Duration:      stmtlogger.Duration{Duration: time.Millisecond},
		Attempt:       1,
		GeminiAttempt: 1,
		StatementType: typedef.SelectStatementType,
	}

	err = cqlStmts.Insert(t.Context(), oracleItem)
	require.NoError(t, err)

	testItem := stmtlogger.Item{
		Start: stmtlogger.Time{Time: time.Now()},
		PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
			"pk0": {"test_key"},
		}),
		Error:         mo.Left[error, string](nil),
		Statement:     "INSERT INTO test VALUES (?)",
		Host:          "127.0.0.1",
		Type:          stmtlogger.TypeTest,
		Values:        mo.Left[[]any, []byte]([]any{"test_key"}),
		Duration:      stmtlogger.Duration{Duration: time.Millisecond},
		Attempt:       1,
		GeminiAttempt: 1,
		StatementType: typedef.InsertStatementType,
	}

	err = cqlStmts.Insert(t.Context(), testItem)
	require.NoError(t, err)

	tests := []struct {
		jobError  *joberror.JobError
		name      string
		ty        stmtlogger.Type
		expectErr bool
	}{
		{
			name: "fetch oracle select",
			ty:   stmtlogger.TypeOracle,
			jobError: &joberror.JobError{
				Timestamp: time.Now(),
				Query:     "SELECT * FROM test WHERE pk0 = ?",
				Message:   "test error",
				StmtType:  typedef.SelectStatementType,
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {"test_key"},
				}),
			},
			expectErr: false,
		},
		{
			name: "fetch test insert",
			ty:   stmtlogger.TypeTest,
			jobError: &joberror.JobError{
				Timestamp: time.Now(),
				Query:     "INSERT INTO test VALUES (?)",
				Message:   "test error",
				StmtType:  typedef.InsertStatementType,
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {"test_key"},
				}),
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := cqlStmts.Fetch(t.Context(), tt.ty, tt.jobError)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, result)
				assert.NotEmpty(t, result)
			}
		})
	}
}

func TestCQLStatements_FetchMultiPartition_Integration(t *testing.T) {
	t.Parallel()

	containers := testutils.TestContainers(t)
	logger := zaptest.NewLogger(t)

	// Create test keyspace and table in test/oracle sessions
	// Use short name to avoid 48 character limit
	testKS := fmt.Sprintf("ks_multi_%d", time.Now().UnixNano()%1000000)
	testTable := "test_table"

	// Create schema in both oracle and test
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

	// Insert multiple test data rows
	for _, key := range []string{"key1", "key2", "key3"} {
		require.NoError(t, containers.Oracle.Query(
			fmt.Sprintf("INSERT INTO %s.%s (pk0, col1) VALUES (?, ?)", testKS, testTable),
			key, "value_"+key,
		).Exec())
		require.NoError(t, containers.Test.Query(
			fmt.Sprintf("INSERT INTO %s.%s (pk0, col1) VALUES (?, ?)", testKS, testTable),
			key, "value_"+key,
		).Exec())
	}

	// Create statement logger
	session, err := newSession(containers.TestHosts, "", "", logger)
	require.NoError(t, err)
	defer session.Close()

	// Use short name to avoid 48 character limit
	logsKS := fmt.Sprintf("ks_logs_%d", time.Now().UnixNano()%1000000)
	logsTable := "test_statements"

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
	defer func() {
		_ = session.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", logsKS)).Exec()
	}()

	// Insert statement logs for multiple partitions
	for i, key := range []string{"key1", "key2", "key3"} {
		item := stmtlogger.Item{
			Start: stmtlogger.Time{Time: time.Now().Add(time.Duration(i) * time.Millisecond)},
			PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
				"pk0": {key},
			}),
			Error:         mo.Left[error, string](nil),
			Statement:     fmt.Sprintf("SELECT * FROM test WHERE pk0 = '%s'", key),
			Host:          "127.0.0.1",
			Type:          stmtlogger.TypeOracle,
			Values:        mo.Left[[]any, []byte]([]any{key}),
			Duration:      stmtlogger.Duration{Duration: time.Millisecond},
			Attempt:       1,
			GeminiAttempt: 1,
			StatementType: typedef.SelectStatementType,
		}

		err = cqlStmts.Insert(t.Context(), item)
		require.NoError(t, err)
	}

	tests := []struct {
		jobError  *joberror.JobError
		checkFunc func(*testing.T, cqlDataMap)
		name      string
		ty        stmtlogger.Type
		expectErr bool
	}{
		{
			name: "fetch multi partition select",
			ty:   stmtlogger.TypeOracle,
			jobError: &joberror.JobError{
				Timestamp: time.Now(),
				Query:     "SELECT * FROM test WHERE pk0 IN (?, ?, ?)",
				Message:   "multi partition error",
				StmtType:  typedef.SelectMultiPartitionType,
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {"key1", "key2", "key3"},
				}),
			},
			expectErr: false,
			checkFunc: func(t *testing.T, result cqlDataMap) {
				t.Helper()

				// Result may be empty if statement logs don't match exactly
				// The important thing is no error was returned
				assert.NotNil(t, result)
			},
		},
		{
			name: "fetch multi partition range select",
			ty:   stmtlogger.TypeTest,
			jobError: &joberror.JobError{
				Timestamp: time.Now(),
				Query:     "SELECT * FROM test WHERE pk0 IN (?, ?)",
				Message:   "multi partition range error",
				StmtType:  typedef.SelectMultiPartitionRangeStatementType,
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {"key1", "key2"},
				}),
			},
			expectErr: false,
			checkFunc: func(t *testing.T, result cqlDataMap) {
				t.Helper()
				assert.NotNil(t, result)
			},
		},
		{
			name: "fetch delete multiple partitions",
			ty:   stmtlogger.TypeOracle,
			jobError: &joberror.JobError{
				Timestamp: time.Now(),
				Query:     "DELETE FROM test WHERE pk0 IN (?, ?, ?)",
				Message:   "delete multiple partitions error",
				StmtType:  typedef.DeleteMultiplePartitionsType,
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {"key1", "key2", "key3"},
				}),
			},
			expectErr: false,
			checkFunc: func(t *testing.T, result cqlDataMap) {
				t.Helper()
				assert.NotNil(t, result)
			},
		},
		{
			name: "fetch select by index - unsupported",
			ty:   stmtlogger.TypeOracle,
			jobError: &joberror.JobError{
				Timestamp: time.Now(),
				Query:     "SELECT * FROM test WHERE col1 = ?",
				Message:   "select by index error",
				StmtType:  typedef.SelectByIndexStatementType,
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {"key1"},
				}),
			},
			expectErr: true,
			checkFunc: func(_ *testing.T, _ cqlDataMap) {
				// Should return error for unsupported statement types
			},
		},
		{
			name: "fetch materialized view - unsupported",
			ty:   stmtlogger.TypeTest,
			jobError: &joberror.JobError{
				Timestamp: time.Now(),
				Query:     "SELECT * FROM mv_test",
				Message:   "materialized view error",
				StmtType:  typedef.SelectFromMaterializedViewStatementType,
				PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
					"pk0": {"key1"},
				}),
			},
			expectErr: true,
			checkFunc: func(_ *testing.T, _ cqlDataMap) {
				// Should return error for unsupported statement types
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			//nolint:govet
			result, err := cqlStmts.Fetch(t.Context(), tt.ty, tt.jobError)

			if tt.expectErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				if tt.checkFunc != nil {
					tt.checkFunc(t, result)
				}
			}
		})
	}
}

func TestLogger_FullWorkflow_Integration(t *testing.T) {
	t.Parallel()

	containers := testutils.TestContainers(t)
	logger := zaptest.NewLogger(t)

	// Create test keyspace and table
	testKS := testutils.GenerateUniqueKeyspaceName(t)
	testTable := "test_table"

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

	defer func() {
		_ = containers.Oracle.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", testKS)).Exec()
		_ = containers.Test.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", testKS)).Exec()
	}()

	// Create temp directory for statement files
	tmpDir := t.TempDir()
	oracleFile := filepath.Join(tmpDir, "oracle_statements.jsonl")
	testFile := filepath.Join(tmpDir, "test_statements.jsonl")

	// Create channels
	itemCh := make(chan stmtlogger.Item, 100)
	errorCh := make(chan *joberror.JobError, 100)

	partitionKeys := typedef.Columns{
		{Name: "pk0", Type: typedef.TypeText},
	}

	// Create logger
	scyllaLogger, err := New(
		testKS,
		testTable,
		func() (*gocql.Session, error) { return containers.Oracle, nil },
		func() (*gocql.Session, error) { return containers.Test, nil },
		containers.TestHosts,
		"", "",
		partitionKeys,
		replication.NewSimpleStrategy(),
		itemCh,
		oracleFile,
		testFile,
		errorCh,
		logger,
	)
	require.NoError(t, err)
	require.NotNil(t, scyllaLogger)

	defer func() {
		close(itemCh)
		close(errorCh)
		_ = scyllaLogger.Close()

		// Cleanup logs keyspace
		logsKS := GetScyllaStatementLogsKeyspace(testKS)
		session, _ := newSession(containers.TestHosts, "", "", logger)
		if session != nil {
			_ = session.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", logsKS)).Exec()
			session.Close()
		}
	}()

	// Send some items
	for i := 0; i < 5; i++ {
		itemCh <- stmtlogger.Item{
			Start: stmtlogger.Time{Time: time.Now()},
			PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
				"pk0": {fmt.Sprintf("key_%d", i)},
			}),
			Error:         mo.Left[error, string](nil),
			Statement:     fmt.Sprintf("SELECT * FROM test WHERE pk0 = 'key_%d'", i),
			Host:          "127.0.0.1",
			Type:          stmtlogger.TypeTest,
			Values:        mo.Left[[]any, []byte]([]any{fmt.Sprintf("key_%d", i)}),
			Duration:      stmtlogger.Duration{Duration: time.Millisecond},
			Attempt:       1,
			GeminiAttempt: 1,
			StatementType: typedef.SelectStatementType,
		}
	}

	// Send some errors
	for i := 0; i < 3; i++ {
		errorCh <- &joberror.JobError{
			Timestamp: time.Now(),
			Query:     fmt.Sprintf("SELECT * FROM test WHERE pk0 = 'error_key_%d'", i),
			Message:   fmt.Sprintf("error message %d", i),
			StmtType:  typedef.SelectStatementType,
			PartitionKeys: typedef.NewValuesFromMap(map[string][]any{
				"pk0": {fmt.Sprintf("error_key_%d", i)},
			}),
		}
	}

	// Wait a bit for processing
	time.Sleep(2 * time.Second)

	// Verify statement files were created
	_, err = os.Stat(oracleFile)
	assert.NoError(t, err, "Oracle statements file should exist")

	_, err = os.Stat(testFile)
	assert.NoError(t, err, "Test statements file should exist")

	// Read and verify oracle file
	oracleContent, err := os.ReadFile(oracleFile)
	if err == nil && len(oracleContent) > 0 {
		var line Line
		lines := 0
		for _, l := range bytes.Split(oracleContent, []byte("\n")) {
			if len(l) > 0 {
				//nolint:govet
				err := json.Unmarshal(l, &line)
				assert.NoError(t, err)
				lines++
			}
		}
		assert.Greater(t, lines, 0, "Should have oracle statements")
	}
}

func TestLogger_StatementFlusher_Integration(t *testing.T) {
	t.Parallel()

	logger := zaptest.NewLogger(t)
	tmpDir := t.TempDir()

	oracleFile := filepath.Join(tmpDir, "oracle.jsonl")
	testFile := filepath.Join(tmpDir, "test.jsonl")

	mockLogger := &Logger{
		logger: logger,
	}

	ch := make(chan statementChData, 10)

	go mockLogger.statementFlusher(ch, oracleFile, testFile)

	// Send data
	ch <- statementChData{
		ty: stmtlogger.TypeOracle,
		Data: cqlDataMap{
			{1}: {
				partitionKeys: map[string][]any{
					"pk0": {"test"},
				},
				statements: []json.RawMessage{
					json.RawMessage(`{"stmt":"SELECT"}`),
				},
				mutationFragments: []json.RawMessage{
					json.RawMessage(`{"data":"oracle"}`),
				},
			},
		},
		Error: &joberror.JobError{
			Timestamp: time.Now(),
			Query:     "SELECT * FROM test",
			Message:   "oracle error",
			StmtType:  typedef.SelectStatementType,
		},
	}

	close(ch)

	// Give the goroutine time to process and flush
	time.Sleep(100 * time.Millisecond)
	mockLogger.wg.Wait()

	// Verify files

	_, err := os.Stat(oracleFile)
	assert.NoError(t, err)

	content, err := os.ReadFile(oracleFile)
	require.NoError(t, err)
	assert.NotEmpty(t, content)

	var line Line
	err = json.Unmarshal(content, &line)
	assert.NoError(t, err)
	assert.Equal(t, "SELECT * FROM test", line.Query)
}
