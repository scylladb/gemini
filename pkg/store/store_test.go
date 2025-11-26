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

//go:build testing

package store

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
)

func TestDelegatingStore_Mutate(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()

	// Create a sample statement for testing
	stmt := typedef.SimpleStmt("INSERT INTO test_table (id, name) VALUES (?, ?)", typedef.InsertStatementType)

	t.Run("successful mutation with both stores", func(t *testing.T) {
		t.Parallel()

		testStore := &mockStoreLoader{}
		oracleStore := &mockStoreLoader{}

		ds := &delegatingStore{
			testStore:          testStore,
			oracleStore:        oracleStore,
			logger:             logger,
			mutationRetries:    1,
			mutationRetrySleep: 100 * time.Millisecond,
		}

		testStore.
			On("mutate", mock.Anything, stmt).
			Once().
			Return(nil)
		oracleStore.
			On("mutate", mock.Anything, stmt).
			Once().
			Return(nil)

		err := ds.Mutate(t.Context(), stmt)

		assert.NoError(t, err)
		testStore.AssertExpectations(t)
		oracleStore.AssertExpectations(t)
	})

	t.Run("successful mutation with only test store (no oracle)", func(t *testing.T) {
		t.Parallel()

		testStore := &mockStoreLoader{}

		ds := &delegatingStore{
			testStore:          testStore,
			oracleStore:        nil, // No oracle store
			logger:             logger,
			mutationRetries:    1,
			mutationRetrySleep: 100 * time.Millisecond,
		}

		ctx := t.Context()

		testStore.On("mutate", mock.Anything, stmt).Return(nil)

		err := ds.Mutate(ctx, stmt)

		assert.NoError(t, err)
		testStore.AssertExpectations(t)
	})

	t.Run("test store fails", func(t *testing.T) {
		t.Parallel()

		testStore := &mockStoreLoader{}
		oracleStore := &mockStoreLoader{}

		ds := &delegatingStore{
			testStore:          testStore,
			oracleStore:        oracleStore,
			logger:             logger,
			mutationRetries:    1,
			mutationRetrySleep: 100 * time.Millisecond,
		}

		testErr := errors.New("test store mutation failed")

		ctx := t.Context()

		// First attempt: test fails, oracle succeeds
		// Retry: test still fails, oracle already succeeded (won't be called again)
		testStore.
			On("mutate", mock.Anything, stmt).
			Return(testErr).
			Times(2) // Initial attempt + 1 retry

		oracleStore.
			On("mutate", mock.Anything, stmt).
			Return(nil).
			Once() // Only called on first attempt, then succeeds

		err := ds.Mutate(ctx, stmt)

		assert.Error(t, err)
		assert.ErrorIs(t, err, testErr)
		testStore.AssertExpectations(t)
		oracleStore.AssertExpectations(t)
	})

	t.Run("oracle store fails", func(t *testing.T) {
		t.Parallel()

		testStore := &mockStoreLoader{}
		oracleStore := &mockStoreLoader{}

		ds := &delegatingStore{
			testStore:          testStore,
			oracleStore:        oracleStore,
			logger:             logger,
			mutationRetries:    1,
			mutationRetrySleep: 100 * time.Millisecond,
		}

		oracleErr := errors.New("oracle store mutation failed")
		ctx := t.Context()

		// First attempt: test succeeds, oracle fails
		// Retry: test already succeeded (won't be called again), oracle still fails
		testStore.
			On("mutate", mock.Anything, stmt).
			Return(nil).
			Once() // Only called on first attempt, then succeeds

		oracleStore.
			On("mutate", mock.Anything, stmt).
			Return(oracleErr).
			Times(2) // Initial attempt + 1 retry

		err := ds.Mutate(ctx, stmt)

		assert.Error(t, err)
		assert.ErrorIs(t, err, oracleErr)
		testStore.AssertExpectations(t)
		oracleStore.AssertExpectations(t)
	})

	t.Run("both stores fail", func(t *testing.T) {
		t.Parallel()

		testStore := &mockStoreLoader{}
		oracleStore := &mockStoreLoader{}

		ds := &delegatingStore{
			testStore:          testStore,
			oracleStore:        oracleStore,
			logger:             logger,
			mutationRetries:    1,
			mutationRetrySleep: 100 * time.Millisecond,
		}

		testErr := errors.New("test store mutation failed")
		oracleErr := errors.New("oracle store mutation failed")
		ctx := t.Context()

		// With mutationRetries=1, we execute up to 2 attempts
		testStore.On("mutate", mock.Anything, stmt).
			Return(testErr).
			Times(2) // Initial attempt + 1 retry

		oracleStore.On("mutate", mock.Anything, stmt).
			Return(oracleErr).
			Times(2) // Initial attempt + 1 retry

		err := ds.Mutate(ctx, stmt)

		assert.Error(t, err)
		testStore.AssertExpectations(t)
		oracleStore.AssertExpectations(t)
	})

	t.Run("concurrent execution timing", func(t *testing.T) {
		t.Parallel()

		testStore := &mockStoreLoader{}
		oracleStore := &mockStoreLoader{}

		ds := &delegatingStore{
			testStore:          testStore,
			oracleStore:        oracleStore,
			logger:             logger,
			mutationRetries:    1,
			mutationRetrySleep: 10 * time.Millisecond,
		}

		var testStartTime, oracleStartTime time.Time
		var mu sync.Mutex
		ctx := t.Context()

		testStore.
			On("mutate", mock.Anything, stmt).
			Return(nil).
			Run(func(_ mock.Arguments) {
				mu.Lock()
				testStartTime = time.Now()
				mu.Unlock()
				time.Sleep(50 * time.Millisecond) // Simulate some work
			})

		oracleStore.
			On("mutate", mock.Anything, stmt).
			Return(nil).
			Run(func(_ mock.Arguments) {
				mu.Lock()
				oracleStartTime = time.Now()
				mu.Unlock()
				time.Sleep(50 * time.Millisecond)
			})

		start := time.Now()
		err := ds.Mutate(ctx, stmt)
		duration := time.Since(start)

		assert.NoError(t, err)

		// Verify concurrent execution - should take less than 100ms if truly concurrent
		// (each store operation takes 50ms)
		assert.Less(t, duration, 100*time.Millisecond)

		// Verify both operations started around the same time (within 10ms)
		mu.Lock()
		timeDiff := oracleStartTime.Sub(testStartTime)
		if timeDiff < 0 {
			timeDiff = -timeDiff
		}
		mu.Unlock()
		assert.Less(t, timeDiff, 10*time.Millisecond)

		testStore.AssertExpectations(t)
		oracleStore.AssertExpectations(t)
	})

	t.Run("oracle store slow but test store fails", func(t *testing.T) {
		t.Parallel()

		testStore := &mockStoreLoader{}
		oracleStore := &mockStoreLoader{}

		ds := &delegatingStore{
			testStore:          testStore,
			oracleStore:        oracleStore,
			logger:             logger,
			mutationRetries:    1,
			mutationRetrySleep: 100 * time.Millisecond,
		}

		testErr := errors.New("test store mutation failed")
		ctx := t.Context()

		// First attempt: test fails, oracle succeeds (but slow)
		// Retry: test still fails, oracle already succeeded (won't be called again)
		testStore.
			On("mutate", mock.Anything, stmt).
			Return(testErr).
			Times(2) // First attempt + 1 retry

		oracleStore.
			On("mutate", mock.Anything, stmt).
			Return(nil).
			Once() // Only called on first attempt, then succeeds

		// Execute mutation
		err := ds.Mutate(ctx, stmt)

		// Should fail because test store keeps failing
		assert.Error(t, err)
		assert.ErrorIs(t, err, testErr)

		testStore.AssertExpectations(t)
		oracleStore.AssertExpectations(t)
	})
}

func TestDelegatingStore_MutationWithChecks(t *testing.T) {
	t.Parallel()

	scyllaContainer := testutils.TestContainers(t)
	keyspace := testutils.GenerateUniqueKeyspaceName(t)

	assert.NoError(t, scyllaContainer.Test.Query(
		fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace),
	).Exec())
	assert.NoError(t, scyllaContainer.Test.Query(
		fmt.Sprintf("CREATE TABLE %s.table_1 (id int, value text, ck1 int, col1 text, PRIMARY KEY ((id,value), ck1));", keyspace),
	).Exec())
	assert.NoError(t, scyllaContainer.Oracle.Query(
		fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace),
	).Exec())
	assert.NoError(t, scyllaContainer.Oracle.Query(
		fmt.Sprintf("CREATE TABLE %s.table_1 (id int, value text, ck1 int, col1 text, PRIMARY KEY ((id,value), ck1));", keyspace),
	).Exec())

	schema := &typedef.Schema{
		Keyspace: typedef.Keyspace{Name: keyspace},
		Config: typedef.SchemaConfig{
			ReplicationStrategy:              replication.NewSimpleStrategy(),
			OracleReplicationStrategy:        replication.NewSimpleStrategy(),
			AsyncObjectStabilizationAttempts: 10,
			UseMaterializedViews:             false,
			AsyncObjectStabilizationDelay:    10 * time.Millisecond,
		},
		Tables: []*typedef.Table{{
			Name:    "table_1",
			Columns: typedef.Columns{},
		}},
	}

	store := &delegatingStore{
		oracleStore: newCQLStoreWithSession(
			scyllaContainer.OracleCluster,
			schema, zap.NewNop(),
			"",
			"oracle",
		),
		testStore:            newCQLStoreWithSession(scyllaContainer.TestCluster, schema, zap.NewNop(), "", "test"),
		logger:               zap.NewNop(),
		mutationRetries:      5,
		mutationRetrySleep:   10 * time.Millisecond,
		serverSideTimestamps: true,
	}

	assert.NoError(t, store.oracleStore.Init())
	assert.NoError(t, store.testStore.Init())

	partitionKeys := []map[string][]any{
		{"id": {1}, "value": {"test"}},
		{"id": {2}, "value": {"test2"}},
		{"id": {3}, "value": {"test3"}},
		{"id": {4}, "value": {"test4"}},
	}

	const rowsPerPartition = 10

	for _, pk := range partitionKeys {
		for i := range rowsPerPartition {
			insertStmt := typedef.PreparedStmt(
				fmt.Sprintf("INSERT INTO %s.table_1 (id, value, ck1, col1) VALUES (?, ?, ?, ?)", keyspace),
				pk,
				[]any{pk["id"][0], pk["value"][0], i, "col1_value"},
				typedef.InsertStatementType,
			)

			assert.NoError(t, store.Mutate(t.Context(), insertStmt))
		}
	}

	time.Sleep(1 * time.Second)

	singlePartitionSelect := typedef.PreparedStmt(
		fmt.Sprintf("SELECT * FROM %s.table_1 WHERE id = ? AND value = ?", keyspace),
		partitionKeys[0],
		[]any{partitionKeys[0]["id"][0], partitionKeys[0]["value"][0]},
		typedef.SelectStatementType,
	)

	multiPartitionSelect := typedef.PreparedStmt(
		fmt.Sprintf("SELECT * FROM %s.table_1 WHERE id IN (?, ?) AND value IN (?, ?)", keyspace),
		map[string][]any{"id": {1, 2}, "value": {"test", "test2"}},
		[]any{1, 2, "test", "test2"},
		typedef.SelectMultiPartitionType,
	)

	singlePartitionRangeSelect := typedef.PreparedStmt(
		fmt.Sprintf("SELECT * FROM %s.table_1 WHERE id = ? AND value = ? AND ck1 >= ? AND ck1 < ?", keyspace),
		partitionKeys[2],
		[]any{partitionKeys[2]["id"][0], partitionKeys[2]["value"][0], 0, 3},
		typedef.SelectRangeStatementType,
	)

	multiPartitionRangeSelect := typedef.PreparedStmt(
		fmt.Sprintf("SELECT * FROM %s.table_1 WHERE id IN (?, ?) AND value IN (?, ?) AND ck1 >= ? AND ck1 < ?", keyspace),
		map[string][]any{"id": {1, 2}, "value": {"test", "test2"}},
		[]any{3, 4, "test3", "test4", 0, 3},
		typedef.SelectMultiPartitionType,
	)

	count, err := store.Check(t.Context(), schema.Tables[0], singlePartitionSelect, 1)
	assert.NoError(t, err)
	assert.Equalf(t, rowsPerPartition, count, "Expected %d row to be returned from check after insert", rowsPerPartition)

	count, err = store.Check(t.Context(), schema.Tables[0], multiPartitionSelect, 1)
	assert.NoError(t, err)
	assert.Equalf(t, 2*rowsPerPartition, count, "Expected %d row to be returned from check after insert", 2*rowsPerPartition)

	count, err = store.Check(t.Context(), schema.Tables[0], singlePartitionRangeSelect, 1)
	assert.NoError(t, err)
	assert.Equalf(t, 3, count, "Expected %d row to be returned from check after insert", 3)

	count, err = store.Check(t.Context(), schema.Tables[0], multiPartitionRangeSelect, 1)
	assert.NoError(t, err)
	assert.Equalf(t, 6, count, "Expected %d row to be returned from check after insert", 6)
}
