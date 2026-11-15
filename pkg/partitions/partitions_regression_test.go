// Copyright 2026 ScyllaDB
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

package partitions

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"uuid"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

func (p *Partitions) isMapped(id uuid.UUID) bool {
	p.validationMu.RLock()
	defer p.validationMu.RUnlock()
	_, ok := p.uuidToIdx[id]
	return ok
}

func (p *Partitions) mappedCount() int {
	p.validationMu.RLock()
	defer p.validationMu.RUnlock()
	return len(p.uuidToIdx)
}

// TestBorrowRacesReplace runs lock-free borrows against Replace on a small
// pool. A borrowed UUID must stay mapped while its keys are held, so
// MarkInvalid can find it. After every reference is released, only the
// current partitions stay mapped.
func TestBorrowRacesReplace(t *testing.T) {
	t.Parallel()

	const (
		count   = 4
		readers = 8
		rounds  = 2000
	)
	parts := createTestPartitions(t, count)

	var unmapped atomic.Int64
	var wg sync.WaitGroup
	for range readers {
		wg.Go(func() {
			for i := range rounds {
				var keys typedef.PartitionKeys
				if i%2 == 0 {
					keys = parts.Next()
				} else {
					keys = parts.NextForWrite()
				}
				if !parts.isMapped(keys.ID) {
					unmapped.Add(1)
				}
				keys.Release()
			}
		})
	}
	wg.Go(func() {
		for i := range rounds {
			parts.Replace(uint64(i % count)).Release()
		}
	})
	wg.Wait()

	assert.Zero(t, unmapped.Load(), "a held borrow must keep its UUID mapped")
	assert.Equal(t, count, parts.mappedCount(), "only the current partitions may stay mapped")
}

// TestKeysReleaseIsIdempotent verifies that a second Release of the same keys
// does not drop a reference that another borrow holds.
func TestKeysReleaseIsIdempotent(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 2)
	first := parts.Get(0)
	second := parts.Get(0)

	parts.Replace(0).Release()
	first.Release()
	first.Release()

	assert.True(t, parts.isMapped(second.ID), "the second borrow still holds the retired partition")
	assert.True(t, parts.MarkInvalid(&second))

	second.Release()
	assert.False(t, parts.isMapped(second.ID), "the last release must unmap the retired partition")
}

// TestMarkInvalid_ReportsAtCapAfterWriteInvalidations models a run where
// write-side invalidations already used the whole cap. A divergence that a
// validator finds later must still be reported.
func TestMarkInvalid_ReportsAtCapAfterWriteInvalidations(t *testing.T) {
	t.Parallel()

	const limit = 2
	parts := createTestPartitionsWithMaxInvalid(t, 10, limit)

	for i := range uint64(limit) {
		keys := parts.Get(i)
		require.True(t, parts.MarkInvalid(&keys))
		keys.Release()
	}
	require.Equal(t, uint64(limit), parts.InvalidCount())

	keys := parts.Get(5)
	t.Cleanup(keys.Release)

	assert.True(t, parts.MarkInvalid(&keys), "a divergence at the cap must be reported")
	assert.False(t, parts.IsInvalid(5), "the cap must leave the slot unmarked")
	assert.False(t, parts.MarkInvalid(&keys), "the divergence must be reported once")
	assert.Equal(t, uint64(limit), parts.InvalidCount())

	// The cap frees up when a marked slot is replaced. The partition reported
	// at the cap then gets its mark, but no second report.
	parts.Replace(0).Release()
	assert.False(t, parts.MarkInvalid(&keys), "the mark after the cap must not report again")
	assert.True(t, parts.IsInvalid(5))
}

// TestDeletedEmissionSurvivesEviction covers an intermediate emission of a
// deleted partition whose window entry is then evicted. The validator still
// holds the emitted copy, so the UUID must stay mapped and MarkInvalid must
// report a divergence on it.
func TestDeletedEmissionSurvivesEviction(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 4)
	// One live entry at most, and no background drainer, so the test drives
	// every emission and eviction.
	d := newDeleted(t.Context(), []time.Duration{time.Millisecond, 10 * time.Minute}, 1, false)
	t.Cleanup(d.Close)
	parts.deleted = d

	parts.Replace(0).Release()

	// The first bucket fires. The window emits a copy and moves the entry to
	// the next bucket.
	future := time.Now().Add(time.Hour).UnixNano()
	d.win.drainReady(future, 64, d.trySend)
	var emitted typedef.PartitionKeys
	select {
	case emitted = <-d.ch:
	default:
		require.FailNow(t, "the first bucket must emit the deleted partition")
	}
	require.NotNil(t, emitted.Release, "an intermediate emission must carry its own reference")

	// A second delete pushes the window over its cap and evicts the entry of
	// the first partition, which has the largest readyAt.
	parts.Replace(1).Release()
	require.Equal(t, uint64(1), d.evicted.Load(), "the first entry must be evicted")

	assert.True(t, parts.isMapped(emitted.ID), "the emitted copy must keep the UUID mapped")
	assert.True(t, parts.MarkInvalid(&emitted), "a divergence on the emitted copy must be reported")

	emitted.Release()
	assert.False(t, parts.isMapped(emitted.ID), "the last release must unmap the deleted partition")
}

// TestDeletedEmissionFullChannelKeepsCount verifies that an emission that
// fails on a full channel drops the reference it took.
func TestDeletedEmissionFullChannelKeepsCount(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 2)
	d := newDeleted(t.Context(), []time.Duration{time.Millisecond, 10 * time.Minute}, 0, false)
	t.Cleanup(d.Close)
	parts.deleted = d

	old := parts.Get(0)
	t.Cleanup(old.Release)
	part := parts.partition(0)
	parts.Replace(0).Release()
	require.Equal(t, int32(2), part.refCount.Load(), "the borrow and the window hold a reference")

	future := time.Now().Add(time.Hour).UnixNano()
	d.win.drainReady(future, 64, func(typedef.PartitionKeys) bool { return false })

	assert.Equal(t, int32(2), part.refCount.Load(), "a failed send must drop the reference it took")
}

// TestReplaceRetiresTrackedRowsLazily verifies that rows of a replaced
// partition never come out of Pop or Peek, that later rows of that partition
// are dropped, and that FillRatio counts only valid rows.
func TestReplaceRetiresTrackedRowsLazily(t *testing.T) {
	t.Parallel()

	const capacity = 1000
	parts := createTestPartitionsWithTracker(t, capacity)

	oldKeys := parts.Get(0)
	t.Cleanup(oldKeys.Release)
	liveKeys := parts.Get(1)
	t.Cleanup(liveKeys.Release)

	for i := range 900 {
		parts.TrackRow(TrackedRow{PartitionID: oldKeys.ID, ClusteringValues: []any{int64(i)}})
	}
	for i := range 10 {
		parts.TrackRow(TrackedRow{PartitionID: liveKeys.ID, ClusteringValues: []any{int64(i)}})
	}

	parts.Replace(0).Release()
	assert.InDelta(t, 10.0/capacity, parts.RowTrackerFillRatio(), 1e-9)

	parts.TrackRow(TrackedRow{PartitionID: oldKeys.ID})
	assert.Equal(t, uint64(910), parts.TrackedRowCount(), "a row of a replaced partition must be dropped")

	for range 20 {
		row, ok := parts.PeekTrackedRow()
		require.True(t, ok)
		assert.Equal(t, liveKeys.ID, row.PartitionID)
	}

	popped := 0
	for {
		row, ok := parts.PopTrackedRow()
		if !ok {
			break
		}
		assert.Equal(t, liveKeys.ID, row.PartitionID)
		popped++
	}
	assert.Equal(t, 10, popped)
	assert.Zero(t, parts.TrackedRowCount())
	assert.Zero(t, parts.RowTrackerFillRatio())
}

// TestPickValidIdxScanSpreadsPicks verifies that the linear-scan branch of
// pickValidIdx spreads picks over every valid slot.
func TestPickValidIdxScanSpreadsPicks(t *testing.T) {
	t.Parallel()

	const total = 10
	parts := createTestPartitions(t, total)

	// Mark slots 0-5 invalid, so at least half the pool is invalid and the
	// linear scan runs.
	for i := range uint64(6) {
		keys := parts.Get(i)
		require.True(t, parts.MarkInvalid(&keys))
		keys.Release()
	}

	seen := make(map[uint64]int)
	for range 2000 {
		idx := parts.pickValidIdx(*parts.parts.slots.Load(), parts.idxFunc)
		require.False(t, parts.IsInvalid(idx), "pick %d is invalid", idx)
		seen[idx]++
	}

	assert.Len(t, seen, total-6, "every valid slot must get picks")
	for idx, n := range seen {
		assert.Greater(t, n, 100, "slot %d got too few picks", idx)
	}
}

// TestWorkerDrawsFromItsOwnFunction: a worker view picks the indexes its own
// function returns, and leaves the shared function of the pool alone.
func TestWorkerDrawsFromItsOwnFunction(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 10)
	w := parts.ForWorker(func() uint64 { return 7 })

	keys := w.NextForWrite()
	t.Cleanup(keys.Release)
	if keys.ID != parts.partition(7).id {
		t.Fatalf("NextForWrite took slot of %v, want slot 7", keys.ID)
	}
	if !isWritten(*parts.parts.written.Load(), 7) {
		t.Fatal("NextForWrite must mark slot 7 written")
	}

	next := w.Next()
	t.Cleanup(next.Release)
	if next.ID != parts.partition(7).id {
		t.Fatalf("Next took %v, want slot 7", next.ID)
	}

	old := parts.partition(7).id
	replaced := w.ReplaceNext()
	t.Cleanup(replaced.Release)
	if replaced.ID != old || parts.partition(7).id == old {
		t.Fatal("ReplaceNext must replace slot 7")
	}
}
