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

package store

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/zap"

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
