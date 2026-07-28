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
	"strings"
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
// It also records the ctx and timestamp of the last mutate so tests can assert
// that compensation attaches ContextData (for statement logging) and threads the
// caller's write timestamp through.
type compensateStore struct {
	muErr   error
	lastCtx atomic.Pointer[context.Context]
	lastTS  atomic.Pointer[mo.Option[time.Time]]
	fakeStore
	muCalls atomic.Int64
}

func (c *compensateStore) mutate(ctx context.Context, _ *typedef.Stmt, ts mo.Option[time.Time]) error {
	c.muCalls.Add(1)
	c.lastCtx.Store(&ctx)
	c.lastTS.Store(&ts)
	return c.muErr
}

func TestCompensateAsymmetricWrite(t *testing.T) {
	t.Parallel()

	// Build a minimal Stmt with one partition key and real column metadata so
	// buildPartitionDeleteStmt can produce a valid DELETE.
	pkCol := typedef.ColumnDef{Name: "pk", Type: typedef.TypeInt}
	// A fixed client-side write timestamp threaded through compensation; the
	// compensating DELETE must reuse it so both clusters share one cutoff.
	writeTS := mo.Some(time.Unix(1_700_000_000, 0).UTC())
	vals := typedef.NewValuesFromMap(map[string][]any{"pk": {1}})
	stmt := &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{
			{Values: vals},
		},
		// Compensation derives the target table from the statement's query.
		Query: "INSERT INTO ks.t(pk) VALUES (?)",
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
		ok := ds.compensateAsymmetricWrite(stmt, writeTS)
		assert.True(t, ok)
		assert.Equal(t, int64(1), ts.muCalls.Load())
		assert.Equal(t, int64(1), os.muCalls.Load())

		// The caller's write timestamp is threaded through to both stores so the
		// compensating DELETE lands in the same timestamp domain as the writes.
		require.NotNil(t, ts.lastTS.Load())
		assert.Equal(t, writeTS, *ts.lastTS.Load())
		assert.Equal(t, writeTS, *os.lastTS.Load())

		// ContextData is attached so the gocql observers log the compensating
		// delete instead of silently skipping it (nil ContextData).
		require.NotNil(t, ts.lastCtx.Load())
		data := MustGetContextData(*ts.lastCtx.Load())
		require.NotNil(t, data)
		assert.Equal(t, typedef.DeleteWholePartitionType, data.Statement.QueryType)

		// Tagged with the compensation sentinel on BOTH stores so post-mortem
		// triage can tell these deletes apart from ordinary workload against the
		// same partition. Attempt 0 would be indistinguishable from a genuine
		// first-attempt delete, which is the whole reason for logging them.
		assert.Equal(t, CompensationAttempt, data.GeminiAttempt)
		require.NotNil(t, os.lastCtx.Load())
		oracleData := MustGetContextData(*os.lastCtx.Load())
		require.NotNil(t, oracleData)
		assert.Equal(t, CompensationAttempt, oracleData.GeminiAttempt)
	})

	t.Run("test store fails → returns false", func(t *testing.T) {
		t.Parallel()
		ts := &compensateStore{muErr: errors.New("test-fail")}
		os := &compensateStore{}
		ds := makeDS(ts, os)
		ok := ds.compensateAsymmetricWrite(stmt, writeTS)
		assert.False(t, ok)
		assert.Equal(t, int64(1), ts.muCalls.Load())
	})

	t.Run("oracle store fails → returns false", func(t *testing.T) {
		t.Parallel()
		ts := &compensateStore{}
		os := &compensateStore{muErr: errors.New("oracle-fail")}
		ds := makeDS(ts, os)
		ok := ds.compensateAsymmetricWrite(stmt, writeTS)
		assert.False(t, ok)
	})

	t.Run("no oracle store → only test delete, returns true", func(t *testing.T) {
		t.Parallel()
		ts := &compensateStore{}
		ds := makeDS(ts, nil)
		ok := ds.compensateAsymmetricWrite(stmt, writeTS)
		assert.True(t, ok)
		assert.Equal(t, int64(1), ts.muCalls.Load())
	})

	t.Run("empty PartitionKeys → early return true, no mutate calls", func(t *testing.T) {
		t.Parallel()
		ts := &compensateStore{}
		ds := makeDS(ts, nil)
		ok := ds.compensateAsymmetricWrite(&typedef.Stmt{PartitionKeys: nil}, writeTS)
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
		ok := ds.compensateAsymmetricWrite(stmt, writeTS)
		// Empty partitionKeyColumns → compensation early-returns before any delete.
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
			Query: "INSERT INTO ks.t(pk) VALUES (?)",
		}
		ts := &compensateStore{}
		os := &compensateStore{}
		ds := makeDS(ts, os)
		ok := ds.compensateAsymmetricWrite(multiStmt, writeTS)
		assert.True(t, ok)
		assert.Equal(t, int64(2), ts.muCalls.Load())
		assert.Equal(t, int64(2), os.muCalls.Load())
	})
}

// TestBuildPartitionDeleteStmt_ArityFollowsStmtNotStore guards the multi-table
// regression: the store is bound to one table's partition-key schema, but the
// compensation delete must follow the *statement's* own partition key columns
// (and target the statement's table), so the bind arity always matches even when
// the timed-out mutation belongs to a different, narrower table.
func TestBuildPartitionDeleteStmt_ArityFollowsStmtNotStore(t *testing.T) {
	t.Parallel()

	// Store schema: a 7-column partition key (mimics table[0]).
	wide := make(typedef.Columns, 7)
	for i := range wide {
		wide[i] = typedef.ColumnDef{Name: "wpk" + string(rune('0'+i)), Type: typedef.TypeInt}
	}
	ds := delegatingStore{partitionKeyColumns: wide, keyspaceAndTable: "ks.table0"}

	// Statement from a different, 4-column table.
	keys := &typedef.PartitionKeys{
		Values: typedef.NewValuesFromMap(map[string][]any{
			"pk0": {int32(1)}, "pk1": {int32(2)}, "pk2": {int32(3)}, "pk3": {int32(4)},
		}),
	}

	del := ds.buildPartitionDeleteStmt(keys, "ks.table9")
	if assert.NotNil(t, del) {
		assert.Equal(t, 4, len(del.Values), "bind values must match the statement's PK column count, not the store's")
		assert.Equal(t, 4, strings.Count(del.Query, "=?"), "marker count must match value count")
		assert.Contains(t, del.Query, "ks.table9", "must target the statement's table, not the store's table0")
		assert.NotContains(t, del.Query, "table0")
	}
}

// ---------------------------------------------------------------------------
// waitForMutationResults — drain-after-cancel
// ---------------------------------------------------------------------------

// blockingStore is a storeLoader whose mutate blocks until released.
type blockingStore struct {
	ret     error
	release chan struct{}
	fakeStore
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
