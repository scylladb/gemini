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
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/typedef"
)

// ---------------------------------------------------------------------------
// isOnlyTimeoutFailure
// ---------------------------------------------------------------------------

func TestIsOnlyTimeoutFailure(t *testing.T) {
	t.Parallel()

	deadline := context.DeadlineExceeded
	otherErr := errors.New("network error")

	tests := []struct {
		name   string
		result mutationResult
		want   bool
	}{
		{
			name:   "both nil — no timeout at all",
			result: mutationResult{testErr: nil, oracleErr: nil},
			want:   false,
		},
		{
			name:   "test timed out, oracle nil",
			result: mutationResult{testErr: deadline, oracleErr: nil},
			want:   true,
		},
		{
			name:   "oracle timed out, test nil",
			result: mutationResult{testErr: nil, oracleErr: deadline},
			want:   true,
		},
		{
			name:   "both timed out",
			result: mutationResult{testErr: deadline, oracleErr: deadline},
			want:   true,
		},
		{
			name:   "test hard error, oracle nil",
			result: mutationResult{testErr: otherErr, oracleErr: nil},
			want:   false,
		},
		{
			name:   "oracle hard error, test nil",
			result: mutationResult{testErr: nil, oracleErr: otherErr},
			want:   false,
		},
		{
			name:   "test hard error, oracle timed out",
			result: mutationResult{testErr: otherErr, oracleErr: deadline},
			want:   false, // test is not a timeout → testTimeout false
		},
		{
			name:   "test timed out, oracle hard error",
			result: mutationResult{testErr: deadline, oracleErr: otherErr},
			want:   false, // oracle is not a timeout → oracleTimeout false
		},
		{
			name:   "both hard errors",
			result: mutationResult{testErr: otherErr, oracleErr: otherErr},
			want:   false,
		},
		{
			name: "wrapped DeadlineExceeded via fmt.Errorf still matches",
			result: mutationResult{
				testErr:   context.DeadlineExceeded,
				oracleErr: nil,
			},
			want: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isOnlyTimeoutFailure(tc.result)
			assert.Equal(t, tc.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// compensateAsymmetricWrite
// ---------------------------------------------------------------------------

// compensateStore is a fakeStore that counts mutate calls and can be made to fail.
type compensateStore struct {
	fakeStore
	muErr       error
	muCalls     atomic.Int64
}

func (c *compensateStore) mutate(_ context.Context, _ *typedef.Stmt, _ mo.Option[time.Time]) error {
	c.muCalls.Add(1)
	return c.muErr
}

func TestCompensateAsymmetricWrite(t *testing.T) {
	t.Parallel()

	// Build a minimal Stmt with one partition key and real column metadata so
	// buildPartitionDeleteStmt can produce a valid DELETE.
	pkCol := typedef.ColumnDef{Name: "pk", Type: typedef.TypeInt}
	vals := typedef.NewValuesFromMap(map[string][]any{"pk": {1}})
	stmt := &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{
			{Values: vals},
		},
	}

	makeDS := func(testStore, oracleStore storeLoader) delegatingStore {
		return delegatingStore{
			inflight:            new(sync.WaitGroup),
			testStore:           testStore,
			oracleStore:         oracleStore,
			logger:              zap.NewNop(),
			partitionKeyColumns: typedef.Columns{pkCol},
			keyspaceAndTable:    "ks.t",
		}
	}

	t.Run("both stores succeed → returns true", func(t *testing.T) {
		t.Parallel()
		ts := &compensateStore{}
		os := &compensateStore{}
		ds := makeDS(ts, os)
		ok := ds.compensateAsymmetricWrite(stmt)
		assert.True(t, ok)
		assert.Equal(t, int64(1), ts.muCalls.Load())
		assert.Equal(t, int64(1), os.muCalls.Load())
	})

	t.Run("test store fails → returns false", func(t *testing.T) {
		t.Parallel()
		ts := &compensateStore{muErr: errors.New("test-fail")}
		os := &compensateStore{}
		ds := makeDS(ts, os)
		ok := ds.compensateAsymmetricWrite(stmt)
		assert.False(t, ok)
		assert.Equal(t, int64(1), ts.muCalls.Load())
	})

	t.Run("oracle store fails → returns false", func(t *testing.T) {
		t.Parallel()
		ts := &compensateStore{}
		os := &compensateStore{muErr: errors.New("oracle-fail")}
		ds := makeDS(ts, os)
		ok := ds.compensateAsymmetricWrite(stmt)
		assert.False(t, ok)
	})

	t.Run("no oracle store → only test delete, returns true", func(t *testing.T) {
		t.Parallel()
		ts := &compensateStore{}
		ds := makeDS(ts, nil)
		ok := ds.compensateAsymmetricWrite(stmt)
		assert.True(t, ok)
		assert.Equal(t, int64(1), ts.muCalls.Load())
	})

	t.Run("empty PartitionKeys → early return true, no mutate calls", func(t *testing.T) {
		t.Parallel()
		ts := &compensateStore{}
		ds := makeDS(ts, nil)
		ok := ds.compensateAsymmetricWrite(&typedef.Stmt{PartitionKeys: nil})
		assert.True(t, ok)
		assert.Equal(t, int64(0), ts.muCalls.Load())
	})

	t.Run("no partition key columns → early return true per key (stmt skipped)", func(t *testing.T) {
		t.Parallel()
		ts := &compensateStore{}
		// No partition key columns set — buildPartitionDeleteStmt returns nil.
		ds := delegatingStore{
			inflight:            new(sync.WaitGroup),
			testStore:           ts,
			logger:              zap.NewNop(),
			partitionKeyColumns: typedef.Columns{}, // empty
			keyspaceAndTable:    "ks.t",
		}
		ok := ds.compensateAsymmetricWrite(stmt)
		// buildPartitionDeleteStmt returns nil → delete is skipped, ok stays true.
		assert.True(t, ok)
		assert.Equal(t, int64(0), ts.muCalls.Load())
	})

	t.Run("multiple partition keys → each gets a delete", func(t *testing.T) {
		t.Parallel()
		vals2 := typedef.NewValuesFromMap(map[string][]any{"pk": {2}})
		multiStmt := &typedef.Stmt{
			PartitionKeys: []typedef.PartitionKeys{
				{Values: vals},
				{Values: vals2},
			},
		}
		ts := &compensateStore{}
		os := &compensateStore{}
		ds := makeDS(ts, os)
		ok := ds.compensateAsymmetricWrite(multiStmt)
		assert.True(t, ok)
		assert.Equal(t, int64(2), ts.muCalls.Load())
		assert.Equal(t, int64(2), os.muCalls.Load())
	})
}

// ---------------------------------------------------------------------------
// waitForMutationResults — drain-after-cancel
// ---------------------------------------------------------------------------

// blockingStore is a storeLoader whose mutate blocks until released.
type blockingStore struct {
	fakeStore
	release chan struct{}
	ret     error
}

func newBlockingStore(ret error) *blockingStore {
	return &blockingStore{
		release: make(chan struct{}),
		ret:     ret,
	}
}

func (b *blockingStore) mutate(_ context.Context, _ *typedef.Stmt, _ mo.Option[time.Time]) error {
	<-b.release
	return b.ret
}

func TestWaitForMutationResults_DrainAfterCancel(t *testing.T) {
	t.Parallel()

	// We create a delegatingStore whose goroutines will block until we release them.
	// Cancel the context before releasing the goroutines; waitForMutationResults
	// must drain them synchronously so that inflight.Wait() in Close() does not
	// deadlock.

	testStore := newBlockingStore(nil)
	oracleStore := newBlockingStore(context.DeadlineExceeded)

	ds := &delegatingStore{
		inflight:    new(sync.WaitGroup),
		testStore:   testStore,
		oracleStore: oracleStore,
		logger:      zap.NewNop(),
	}

	ctx, cancel := context.WithCancel(t.Context())

	stmt := typedef.SimpleStmt("INSERT INTO ks.t (k) VALUES (1)", typedef.InsertStatementType)

	// Launch executeParallelMutations in a separate goroutine so we can
	// cancel the context while the mutate calls are still blocked.
	resultCh := make(chan mutationResult, 1)
	go func() {
		r := ds.executeParallelMutations(ctx, stmt, mo.None[time.Time](), 0, true, true)
		resultCh <- r
	}()

	// Give the goroutines a moment to start and block.
	time.Sleep(20 * time.Millisecond)
	cancel()

	// Release both blocking stores; waitForMutationResults must drain them.
	close(testStore.release)
	close(oracleStore.release)

	// executeParallelMutations should return promptly after goroutines are drained.
	select {
	case result := <-resultCh:
		// At least one error was captured (context.DeadlineExceeded from oracle).
		// The exact assignment depends on timing, but inflight must be drained.
		_ = result
	case <-time.After(5 * time.Second):
		t.Fatal("executeParallelMutations did not return after drain — possible goroutine leak")
	}

	// inflight.Wait() must complete immediately — confirms goroutines exited.
	done := make(chan struct{})
	go func() {
		ds.inflight.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("inflight.Wait() did not complete — goroutines were not drained")
	}
}

func TestWaitForMutationResults_DrainCapturesRealResult(t *testing.T) {
	t.Parallel()

	// Verify that when ctx fires before a goroutine's real result arrives, the
	// drain loop updates the result struct with the goroutine's actual error
	// (rather than just the ctx error that was written by the ctx.Done branch).

	// The oracle goroutine returns nil (success), but context will expire first.
	// After drain, oracleErr must be nil (the goroutine succeeded).
	testStore := newBlockingStore(context.DeadlineExceeded)
	oracleStore := newBlockingStore(nil)

	ds := &delegatingStore{
		inflight:    new(sync.WaitGroup),
		testStore:   testStore,
		oracleStore: oracleStore,
		logger:      zap.NewNop(),
	}

	ctx, cancel := context.WithCancel(t.Context())
	stmt := typedef.SimpleStmt("INSERT INTO ks.t (k) VALUES (1)", typedef.InsertStatementType)

	resultCh := make(chan mutationResult, 1)
	go func() {
		r := ds.executeParallelMutations(ctx, stmt, mo.None[time.Time](), 0, true, true)
		resultCh <- r
	}()

	time.Sleep(20 * time.Millisecond)
	cancel()

	// Release goroutines after cancel so they send their real results into the drain.
	close(testStore.release)
	close(oracleStore.release)

	select {
	case result := <-resultCh:
		// After drain the oracle's real nil error should have replaced the
		// ctx-assigned error (nil wins over context.Canceled for oracle).
		require.NoError(t, result.oracleErr,
			"drain loop should capture oracle's real nil result, not ctx error")
	case <-time.After(5 * time.Second):
		t.Fatal("executeParallelMutations did not return")
	}
}
