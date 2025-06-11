package store

import (
	"context"
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
		testStore := &mockStoreLoader{}
		oracleStore := &mockStoreLoader{}

		ds := &delegatingStore{
			workers:     newWorkers(10),
			testStore:   testStore,
			oracleStore: oracleStore,
			logger:      logger,
		}

		ctx := t.Context()

		testStore.
			On("mutate", ctx, stmt).
			Once().
			Return(nil)
		oracleStore.
			On("mutate", ctx, stmt).
			Once().
			Return(nil)

		err := ds.Mutate(t.Context(), stmt)

		assert.NoError(t, err)
		testStore.AssertExpectations(t)
		oracleStore.AssertExpectations(t)
	})

	t.Run("successful mutation with only test store (no oracle)", func(t *testing.T) {
		testStore := &mockStoreLoader{}

		ds := &delegatingStore{
			workers:     newWorkers(10),
			testStore:   testStore,
			oracleStore: nil, // No oracle store
			logger:      logger,
		}

		ctx := t.Context()

		testStore.On("mutate", ctx, stmt).Return(nil)

		err := ds.Mutate(ctx, stmt)

		assert.NoError(t, err)
		testStore.AssertExpectations(t)
	})

	t.Run("test store fails", func(t *testing.T) {
		testStore := &mockStoreLoader{}
		oracleStore := &mockStoreLoader{}

		ds := &delegatingStore{
			workers:     newWorkers(10),
			testStore:   testStore,
			oracleStore: oracleStore,
			logger:      logger,
		}

		testErr := errors.New("test store mutation failed")

		ctx := t.Context()

		testStore.
			On("mutate", ctx, stmt).
			Once().
			Return(testErr)
		oracleStore.
			On("mutate", ctx, stmt).
			Once().
			Return(nil)

		err := ds.Mutate(ctx, stmt)

		assert.Error(t, err)
		assert.Equal(t, testErr, err)
		testStore.AssertExpectations(t)
		oracleStore.AssertExpectations(t)
	})

	t.Run("oracle store fails", func(t *testing.T) {
		testStore := &mockStoreLoader{}
		oracleStore := &mockStoreLoader{}

		ds := &delegatingStore{
			workers:     newWorkers(10),
			testStore:   testStore,
			oracleStore: oracleStore,
			logger:      logger,
		}

		oracleErr := errors.New("oracle store mutation failed")
		ctx := t.Context()

		testStore.
			On("mutate", ctx, stmt).
			Once().
			Return(nil)
		oracleStore.
			On("mutate", ctx, stmt).
			Once().
			Return(oracleErr)

		err := ds.Mutate(ctx, stmt)

		assert.Error(t, err)
		assert.Equal(t, oracleErr, err)
		testStore.AssertExpectations(t)
		oracleStore.AssertExpectations(t)
	})

	t.Run("both stores fail", func(t *testing.T) {
		testStore := &mockStoreLoader{}
		oracleStore := &mockStoreLoader{}

		ds := &delegatingStore{
			workers:     newWorkers(10),
			testStore:   testStore,
			oracleStore: oracleStore,
			logger:      logger,
		}

		testErr := errors.New("test store mutation failed")
		oracleErr := errors.New("oracle store mutation failed")
		ctx := t.Context()

		testStore.On("mutate", ctx, stmt).
			Once().
			Return(testErr)
		oracleStore.On("mutate", ctx, stmt).
			Once().
			Return(oracleErr)

		err := ds.Mutate(ctx, stmt)

		assert.Error(t, err)
		assert.Equal(t, testErr, err)
		testStore.AssertExpectations(t)
		oracleStore.AssertExpectations(t)
	})

	t.Run("context cancellation", func(t *testing.T) {
		testStore := &mockStoreLoader{}
		oracleStore := &mockStoreLoader{}

		ds := &delegatingStore{
			workers:     newWorkers(10),
			testStore:   testStore,
			oracleStore: oracleStore,
			logger:      logger,
		}
		ctx := t.Context()

		cancelCtx, cancel := context.WithCancel(ctx)
		cancel()

		testStore.On("mutate", cancelCtx, stmt).
			Once().
			Return(context.Canceled)
		oracleStore.On("mutate", cancelCtx, stmt).
			Once().
			Return(context.Canceled)

		err := ds.Mutate(cancelCtx, stmt)

		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
		testStore.AssertExpectations(t)
		oracleStore.AssertExpectations(t)
	})
	t.Run("concurrent execution timing", func(t *testing.T) {
		testStore := &mockStoreLoader{}
		oracleStore := &mockStoreLoader{}

		ds := &delegatingStore{
			workers:     newWorkers(10),
			testStore:   testStore,
			oracleStore: oracleStore,
			logger:      logger,
		}

		var testStartTime, oracleStartTime time.Time
		var mu sync.Mutex
		ctx := t.Context()

		testStore.
			On("mutate", ctx, stmt).
			Return(nil).
			Run(func(args mock.Arguments) {
				mu.Lock()
				testStartTime = time.Now()
				mu.Unlock()
				time.Sleep(50 * time.Millisecond) // Simulate some work
			})

		oracleStore.
			On("mutate", ctx, stmt).
			Return(nil).
			Run(func(args mock.Arguments) {
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
		testStore := &mockStoreLoader{}
		oracleStore := &mockStoreLoader{}

		ds := &delegatingStore{
			workers:     newWorkers(10),
			testStore:   testStore,
			oracleStore: oracleStore,
			logger:      logger,
		}

		testErr := errors.New("test store mutation failed")
		ctx := t.Context()

		// Setup expectations
		testStore.
			On("mutate", ctx, stmt).
			Once().
			Return(testErr)
		oracleStore.
			On("mutate", ctx, stmt).
			Once().
			Return(nil).
			Run(func(args mock.Arguments) {
				time.Sleep(100 * time.Millisecond)
			})

		err := ds.Mutate(ctx, stmt)

		// Should return test error immediately, but still wait for oracle to complete
		assert.Error(t, err)
		assert.Equal(t, testErr, err)
		testStore.AssertExpectations(t)
		oracleStore.AssertExpectations(t)
	})

	t.Run("nil statement", func(t *testing.T) {
		testStore := &mockStoreLoader{}
		oracleStore := &mockStoreLoader{}

		ds := &delegatingStore{
			workers:     newWorkers(10),
			testStore:   testStore,
			oracleStore: oracleStore,
			logger:      logger,
		}
		ctx := t.Context()

		testStore.
			On("mutate", ctx, (*typedef.Stmt)(nil)).
			Once().
			Return(nil)
		oracleStore.
			On("mutate", ctx, (*typedef.Stmt)(nil)).
			Once().
			Return(nil)

		err := ds.Mutate(ctx, nil)

		assert.NoError(t, err)
		testStore.AssertExpectations(t)
		oracleStore.AssertExpectations(t)
	})
}
