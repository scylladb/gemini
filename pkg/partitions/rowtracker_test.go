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

package partitions

import (
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRowTracker_NewZeroCapacity(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(0)
	require.NotNil(t, rt)
	assert.Equal(t, uint64(0), rt.Capacity())
	assert.Equal(t, uint64(0), rt.Len())

	// Push should be a no-op
	rt.Push(TrackedRow{
		PartitionID:     uuid.New(),
		PartitionValues: []any{int32(1)},
	})
	assert.Equal(t, uint64(0), rt.Len())

	// Pop should always return false
	_, ok := rt.Pop()
	assert.False(t, ok)
}

func TestRowTracker_PushPop(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(10)
	require.NotNil(t, rt)
	assert.Equal(t, uint64(10), rt.Capacity())
	assert.Equal(t, uint64(0), rt.Len())

	id := uuid.New()
	rt.Push(TrackedRow{
		PartitionID:      id,
		PartitionValues:  []any{int32(42)},
		ClusteringValues: []any{int64(100)},
	})
	assert.Equal(t, uint64(1), rt.Len())

	row, ok := rt.Pop()
	require.True(t, ok)
	assert.Equal(t, id, row.PartitionID)
	assert.Equal(t, []any{int64(100)}, row.ClusteringValues)
	assert.Equal(t, uint64(0), rt.Len())

	_, ok = rt.Pop()
	assert.False(t, ok)
}

func TestRowTracker_QueueFullSkipsPush(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(3)

	// Push 5 rows into a queue of capacity 3; the last 2 should be dropped.
	for i := range 5 {
		rt.Push(TrackedRow{
			PartitionID:      uuid.New(),
			PartitionValues:  []any{int32(i)},
			ClusteringValues: []any{int64(i)},
		})
	}

	// Queue is full at capacity 3; extra pushes are no-ops.
	assert.Equal(t, uint64(3), rt.Len())

	// Pop returns the oldest entries (0, 1, 2) — FIFO order, no overwrite.
	row, ok := rt.Pop()
	require.True(t, ok)
	assert.Equal(t, []any{int64(0)}, row.ClusteringValues)

	row, ok = rt.Pop()
	require.True(t, ok)
	assert.Equal(t, []any{int64(1)}, row.ClusteringValues)

	row, ok = rt.Pop()
	require.True(t, ok)
	assert.Equal(t, []any{int64(2)}, row.ClusteringValues)

	_, ok = rt.Pop()
	assert.False(t, ok)
}

func TestRowTracker_Concurrent(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(100)
	const goroutines = 20
	const opsPerGoroutine = 50

	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	// Producers
	for range goroutines {
		go func() {
			defer wg.Done()
			for range opsPerGoroutine {
				rt.Push(TrackedRow{
					PartitionID:      uuid.New(),
					PartitionValues:  []any{int32(1)},
					ClusteringValues: []any{int64(1)},
				})
			}
		}()
	}

	// Consumers
	for range goroutines {
		go func() {
			defer wg.Done()
			for range opsPerGoroutine {
				rt.Pop()
			}
		}()
	}

	wg.Wait()
	assert.True(t, rt.Len() <= rt.Capacity())
}

func TestRowTracker_FullQueueDropsSilently(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(2)

	for range 2 {
		rt.Push(TrackedRow{
			PartitionID:      uuid.New(),
			PartitionValues:  []any{int32(1)},
			ClusteringValues: []any{int64(1)},
		})
	}

	// Push a third row — queue is full, so it should be dropped silently.
	rt.Push(TrackedRow{
		PartitionID:      uuid.New(),
		PartitionValues:  []any{int32(3)},
		ClusteringValues: []any{int64(3)},
	})

	// Queue should still hold the original 2 rows.
	assert.Equal(t, uint64(2), rt.Len())
}

// ---------------------------------------------------------------------------
// FillRatio tests
// ---------------------------------------------------------------------------

func TestRowTracker_FillRatio_ZeroCapacity(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(0)
	assert.Equal(t, 0.0, rt.FillRatio())
}

func TestRowTracker_FillRatio_Empty(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(10)
	assert.Equal(t, 0.0, rt.FillRatio())
}

func TestRowTracker_FillRatio_Half(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(10)
	for range 5 {
		rt.Push(TrackedRow{PartitionID: uuid.New(), PartitionValues: []any{int32(1)}})
	}
	assert.InDelta(t, 0.5, rt.FillRatio(), 0.001)
}

func TestRowTracker_FillRatio_Full(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(4)
	for range 4 {
		rt.Push(TrackedRow{PartitionID: uuid.New(), PartitionValues: []any{int32(1)}})
	}
	assert.Equal(t, 1.0, rt.FillRatio())
}

func TestRowTracker_FillRatio_DecreasesOnPop(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(4)
	for range 4 {
		rt.Push(TrackedRow{PartitionID: uuid.New(), PartitionValues: []any{int32(1)}})
	}
	require.Equal(t, 1.0, rt.FillRatio())

	_, ok := rt.Pop()
	require.True(t, ok)
	assert.InDelta(t, 0.75, rt.FillRatio(), 0.001)
}

func TestRowTracker_FillRatio_ExcludesInvalidated(t *testing.T) {
	t.Parallel()

	const capacity = 10
	rt := NewRowTracker(capacity)

	deadID := uuid.New()
	liveID := uuid.New()

	for range 9 {
		rt.Push(TrackedRow{PartitionID: deadID, PartitionValues: []any{int32(1)}})
	}
	rt.Push(TrackedRow{PartitionID: liveID, PartitionValues: []any{int32(2)}})
	require.Equal(t, 1.0, rt.FillRatio(), "tracker is physically full")

	rt.Invalidate(deadID)

	assert.InDelta(t, 0.1, rt.FillRatio(), 0.001,
		"FillRatio must reflect live occupancy (1/10), not physical occupancy")
	assert.Less(t, rt.FillRatio(), FillZoneAlwaysPush,
		"with mostly-invalidated slots the sampler must be able to leave the skip zone")
}

// ---------------------------------------------------------------------------
// FillZone constant tests — document intended thresholds
// ---------------------------------------------------------------------------

func TestFillZoneConstants(t *testing.T) {
	t.Parallel()

	// Verify ordering: AlwaysPush < Sampled < Skip < 1.0
	assert.Less(t, FillZoneAlwaysPush, FillZoneSampled)
	assert.Less(t, FillZoneSampled, FillZoneSkip)
	assert.Less(t, FillZoneSkip, 1.0)

	// Verify expected values match documented thresholds
	assert.Equal(t, 0.30, FillZoneAlwaysPush)
	assert.Equal(t, 0.70, FillZoneSampled)
	assert.Equal(t, 0.90, FillZoneSkip)
}

// ---------------------------------------------------------------------------
// Invalidate tests
// ---------------------------------------------------------------------------

func TestRowTracker_Invalidate_ZeroCapacity(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(0)
	// Must not panic
	rt.Invalidate(uuid.New())
}

func TestRowTracker_Invalidate_Basic(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(10)
	id := uuid.New()

	rt.Push(TrackedRow{
		PartitionID:      id,
		PartitionValues:  []any{int32(1)},
		ClusteringValues: []any{int64(1)},
	})

	rt.Invalidate(id)

	// Release is called eagerly; Pop should find nothing valid.
	_, ok := rt.Pop()
	assert.False(t, ok, "Pop must return false after all entries are invalidated")
	assert.Equal(t, uint64(0), rt.Len(), "Len must be 0 after Pop drains the invalidated entry")
}

func TestRowTracker_Invalidate_ReleaseCalled(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(10)
	id := uuid.New()

	rt.Push(TrackedRow{
		PartitionID:     id,
		PartitionValues: []any{int32(1)},
	})

	rt.Invalidate(id)

	// The slot must be invalidated: Pop should return nothing valid.
	_, ok := rt.Pop()
	assert.False(t, ok, "Pop must return false after the only entry is invalidated")
}

func TestRowTracker_Invalidate_OnlyMatchingPartition(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(10)
	id1 := uuid.New()
	id2 := uuid.New()

	rt.Push(TrackedRow{PartitionID: id1, PartitionValues: []any{int32(1)}, ClusteringValues: []any{int64(10)}})
	rt.Push(TrackedRow{PartitionID: id2, PartitionValues: []any{int32(2)}, ClusteringValues: []any{int64(20)}})
	rt.Push(TrackedRow{PartitionID: id1, PartitionValues: []any{int32(3)}, ClusteringValues: []any{int64(30)}})
	require.Equal(t, uint64(3), rt.Len())

	rt.Invalidate(id1)

	// Only the id2 row should be returned by Pop; id1 entries are skipped.
	row, ok := rt.Pop()
	require.True(t, ok, "expected the id2 row to be returned")
	assert.Equal(t, id2, row.PartitionID)
	assert.Equal(t, []any{int64(20)}, row.ClusteringValues)

	// No more valid entries.
	_, ok = rt.Pop()
	assert.False(t, ok)
	assert.Equal(t, uint64(0), rt.Len())
}

func TestRowTracker_Invalidate_UnknownID(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(10)
	id := uuid.New()
	other := uuid.New()

	rt.Push(TrackedRow{PartitionID: id, PartitionValues: []any{int32(1)}})
	require.Equal(t, uint64(1), rt.Len())

	// Invalidating an ID that doesn't exist must not affect the live entry
	rt.Invalidate(other)

	assert.Equal(t, uint64(1), rt.Len())

	row, ok := rt.Pop()
	require.True(t, ok)
	assert.Equal(t, id, row.PartitionID)
}

func TestRowTracker_Invalidate_AlreadyInvalidated(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(10)
	id := uuid.New()

	rt.Push(TrackedRow{
		PartitionID:     id,
		PartitionValues: []any{int32(1)},
	})

	rt.Invalidate(id)

	// Second call must be a no-op: no panic.
	rt.Invalidate(id)

	// Pop drains the (already-invalidated) slot.
	_, ok := rt.Pop()
	assert.False(t, ok)
}

func TestRowTracker_Invalidate_NoRelease(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(10)
	id := uuid.New()

	rt.Push(TrackedRow{
		PartitionID:     id,
		PartitionValues: []any{int32(1)},
	})

	rt.Invalidate(id) // must not panic

	_, ok := rt.Pop()
	assert.False(t, ok, "invalidated entry must not be returned by Pop")
}

func TestRowTracker_Invalidate_PopSkipsInvalidSlot(t *testing.T) {
	t.Parallel()

	rt := NewRowTracker(10)
	badID := uuid.New()
	goodID := uuid.New()

	rt.Push(TrackedRow{PartitionID: badID, PartitionValues: []any{int32(1)}})
	rt.Push(TrackedRow{PartitionID: goodID, PartitionValues: []any{int32(2)}, ClusteringValues: []any{int64(99)}})

	rt.Invalidate(badID)

	// Pop should skip the bad slot and return the good one
	row, ok := rt.Pop()
	require.True(t, ok)
	assert.Equal(t, goodID, row.PartitionID)
	assert.Equal(t, []any{int64(99)}, row.ClusteringValues)
}

func TestRowTracker_Invalidate_AllSlots(t *testing.T) {
	t.Parallel()

	const capacity = 8
	rt := NewRowTracker(capacity)
	id := uuid.New()

	for range capacity {
		rt.Push(TrackedRow{PartitionID: id, PartitionValues: []any{int32(1)}})
	}
	require.Equal(t, uint64(capacity), rt.Len())

	rt.Invalidate(id)

	// All slots invalidated; Pop must return nothing.
	_, ok := rt.Pop()
	assert.False(t, ok, "all slots invalidated — Pop must return false")
	assert.Equal(t, uint64(0), rt.Len())
}

func TestRowTracker_Invalidate_AfterQueueFull(t *testing.T) {
	t.Parallel()

	// Fill the queue to capacity, then push extras (dropped).
	// Invalidate entries and verify only valid ones remain.
	const capacity = 4
	rt := NewRowTracker(capacity)

	targetID := uuid.New()
	otherID := uuid.New()

	// Fill: 2 targetID + 2 otherID rows.
	for range 2 {
		rt.Push(TrackedRow{PartitionID: targetID, PartitionValues: []any{int32(1)}, ClusteringValues: []any{int64(42)}})
	}
	for range 2 {
		rt.Push(TrackedRow{PartitionID: otherID, PartitionValues: []any{int32(0)}})
	}
	require.Equal(t, uint64(capacity), rt.Len())

	// Extra pushes must be dropped since the queue is full.
	rt.Push(TrackedRow{PartitionID: targetID, PartitionValues: []any{int32(99)}})
	require.Equal(t, uint64(capacity), rt.Len())

	rt.Invalidate(targetID)

	// Only the 2 otherID rows should remain.
	row, ok := rt.Pop()
	require.True(t, ok)
	assert.Equal(t, otherID, row.PartitionID)

	row, ok = rt.Pop()
	require.True(t, ok)
	assert.Equal(t, otherID, row.PartitionID)

	_, ok = rt.Pop()
	assert.False(t, ok)
	assert.Equal(t, uint64(0), rt.Len())
}

func TestRowTracker_Invalidate_WrappedWindow(t *testing.T) {
	t.Parallel()

	const capacity = 4
	rt := NewRowTracker(capacity)

	targetID := uuid.New()
	otherID := uuid.New()

	for range 4 {
		rt.Push(TrackedRow{PartitionID: targetID, PartitionValues: []any{int32(1)}})
	}
	for range 2 {
		_, ok := rt.Pop()
		require.True(t, ok)
	}

	for range 2 {
		rt.Push(TrackedRow{PartitionID: otherID, PartitionValues: []any{int32(2)}})
	}
	require.Equal(t, uint64(capacity), rt.Len())

	rt.Invalidate(targetID)

	// Only the two otherID rows must survive.
	for range 2 {
		row, ok := rt.Pop()
		require.True(t, ok)
		assert.Equal(t, otherID, row.PartitionID, "only otherID rows survive invalidation")
	}
	_, ok := rt.Pop()
	assert.False(t, ok)
	assert.Equal(t, uint64(0), rt.Len())
}

func TestRowTracker_Invalidate_Concurrent(t *testing.T) {
	t.Parallel()

	const capacity = 200
	rt := NewRowTracker(capacity)

	// Two partitions, push interleaved rows from multiple goroutines
	id1 := uuid.New()
	id2 := uuid.New()

	var wg sync.WaitGroup
	const producers = 10
	wg.Add(producers)
	for range producers {
		go func() {
			defer wg.Done()
			for range 20 {
				rt.Push(TrackedRow{PartitionID: id1, PartitionValues: []any{int32(1)}})
				rt.Push(TrackedRow{PartitionID: id2, PartitionValues: []any{int32(2)}})
			}
		}()
	}
	wg.Wait()

	// Invalidate id1 from a separate goroutine while consumers drain
	var invalidateDone sync.WaitGroup
	invalidateDone.Add(1)
	go func() {
		defer invalidateDone.Done()
		rt.Invalidate(id1)
	}()

	invalidateDone.Wait()

	// After invalidation all remaining pops must only return id2 rows
	for {
		row, ok := rt.Pop()
		if !ok {
			break
		}
		assert.Equal(t, id2, row.PartitionID, "only id2 rows should survive invalidation of id1")
	}
}

// ---------------------------------------------------------------------------
// setBit / bitset word boundary tests
// ---------------------------------------------------------------------------

func TestRowTracker_BitsetWordBoundary(t *testing.T) {
	t.Parallel()

	// Use a capacity that spans exactly two 64-bit words (128 slots).
	// Slots 63 and 64 sit in different words — exercise the word-boundary path.
	const capacity = 128
	rt := NewRowTracker(capacity)

	id := uuid.New()
	other := uuid.New()

	// Push cap rows; place the target ID at the word-boundary slots.
	for i := range capacity {
		pid := other
		if i == 63 || i == 64 {
			pid = id
		}
		rt.Push(TrackedRow{PartitionID: pid, PartitionValues: []any{int32(i)}})
	}
	require.Equal(t, uint64(capacity), rt.Len())

	rt.Invalidate(id)

	// Drain everything via Pop; exactly (cap - 2) valid entries must be returned.
	valid := 0
	for {
		row, ok := rt.Pop()
		if !ok {
			break
		}
		assert.Equal(t, other, row.PartitionID, "only 'other' entries must be returned")
		valid++
	}
	assert.Equal(t, capacity-2, valid, "expected cap-2 valid rows after invalidating 2 word-boundary slots")
}
