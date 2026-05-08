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
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeHeap builds a valid min-heap from a slice of readyAt offsets.
func makeHeap(offsets ...time.Duration) *deletedPartitionHeap {
	base := time.Now()
	h := &deletedPartitionHeap{
		data: make([]deletedPartition, max(len(offsets), 64)),
	}
	for _, d := range offsets {
		h.pushInline(deletedPartition{readyAt: base.Add(d)})
	}
	return h
}

// isValidMinHeap checks the heap property: every parent ≤ both children.
func isValidMinHeap(h *deletedPartitionHeap) bool {
	for i := range h.len {
		left := (i<<1 + 1)
		right := left + 1
		if left < h.len && h.data[left].readyAt.Before(h.data[i].readyAt) {
			return false
		}
		if right < h.len && h.data[right].readyAt.Before(h.data[i].readyAt) {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// popMaxInline
// ---------------------------------------------------------------------------

func TestPopMaxInline_Empty(t *testing.T) {
	t.Parallel()
	h := &deletedPartitionHeap{data: make([]deletedPartition, 8)}
	_, ok := h.popMaxInline()
	assert.False(t, ok)
}

func TestPopMaxInline_Single(t *testing.T) {
	t.Parallel()
	h := makeHeap(10 * time.Second)
	elem, ok := h.popMaxInline()
	require.True(t, ok)
	assert.Equal(t, 0, h.len)
	assert.False(t, elem.readyAt.IsZero())
}

func TestPopMaxInline_ReturnsMaximum(t *testing.T) {
	t.Parallel()
	// Insert 5 elements with distinct readyAt values.
	offsets := []time.Duration{
		1 * time.Second,
		5 * time.Second, // max
		2 * time.Second,
		4 * time.Second,
		3 * time.Second,
	}
	h := makeHeap(offsets...)

	maxElem, ok := h.popMaxInline()
	require.True(t, ok)

	// The returned element must be the one with the largest readyAt.
	expected := time.Duration(0)
	for _, d := range offsets {
		if d > expected {
			expected = d
		}
	}
	// Check that no remaining element has a readyAt strictly after maxElem.readyAt.
	for i := range h.len {
		assert.False(t, h.data[i].readyAt.After(maxElem.readyAt),
			"remaining element at index %d has readyAt after the popped maximum", i)
	}
	assert.Equal(t, 4, h.len)
}

func TestPopMaxInline_HeapPropertyPreserved(t *testing.T) {
	t.Parallel()
	offsets := []time.Duration{
		1 * time.Second,
		10 * time.Second,
		3 * time.Second,
		7 * time.Second,
		2 * time.Second,
		8 * time.Second,
		4 * time.Second,
	}
	h := makeHeap(offsets...)

	for range 4 {
		prevLen := h.len
		_, ok := h.popMaxInline()
		require.True(t, ok)
		assert.Equal(t, prevLen-1, h.len)
		assert.True(t, isValidMinHeap(h),
			"min-heap property violated after popMaxInline (len=%d)", h.len)
	}
}

func TestPopMaxInline_AllElements_DecreasingOrder(t *testing.T) {
	t.Parallel()
	offsets := []time.Duration{
		1 * time.Second,
		5 * time.Second,
		3 * time.Second,
		7 * time.Second,
		2 * time.Second,
	}
	h := makeHeap(offsets...)

	// Popping the max repeatedly must yield times in non-increasing order.
	var popped []time.Time
	for h.len > 0 {
		elem, ok := h.popMaxInline()
		require.True(t, ok)
		popped = append(popped, elem.readyAt)
	}

	require.Len(t, popped, len(offsets))
	for i := 1; i < len(popped); i++ {
		assert.False(t, popped[i].After(popped[i-1]),
			"popped[%d]=%v is after popped[%d]=%v — not non-increasing", i, popped[i], i-1, popped[i-1])
	}
}

func TestPopMaxInline_LastElement(t *testing.T) {
	t.Parallel()
	// When maxIdx == h.len after decrement (the max was already the last entry),
	// the "replace with last" branch is skipped. Verify the heap remains valid.
	// Build a heap where the leaf with the largest value happens to be the last
	// element in the backing array.
	offsets := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		3 * time.Second, // this will land in a leaf position
	}
	h := makeHeap(offsets...)
	for h.len > 0 {
		_, ok := h.popMaxInline()
		require.True(t, ok)
		assert.True(t, isValidMinHeap(h))
	}
}

// ---------------------------------------------------------------------------
// siftInline
// ---------------------------------------------------------------------------

func TestSiftInline_NoOp_OnEmptyOrOutOfBounds(t *testing.T) {
	t.Parallel()
	h := &deletedPartitionHeap{data: make([]deletedPartition, 8), len: 0}
	// Should not panic.
	h.siftInline(0)
	h.siftInline(5)
}

func TestSiftInline_RestoresHeapAfterRootUpdate(t *testing.T) {
	t.Parallel()
	offsets := []time.Duration{
		1 * time.Second,
		3 * time.Second,
		2 * time.Second,
		5 * time.Second,
		4 * time.Second,
	}
	h := makeHeap(offsets...)
	require.True(t, isValidMinHeap(h))

	// Replace the root with a large value — violates the heap property.
	h.data[0].readyAt = h.data[0].readyAt.Add(100 * time.Second)
	h.siftInline(0)
	assert.True(t, isValidMinHeap(h), "heap property not restored after siftInline on root")
}

func TestSiftInline_RestoresHeapAfterLeafUpdate(t *testing.T) {
	t.Parallel()
	offsets := []time.Duration{
		5 * time.Second,
		10 * time.Second,
		7 * time.Second,
		15 * time.Second,
		12 * time.Second,
	}
	h := makeHeap(offsets...)
	require.True(t, isValidMinHeap(h))

	// Set the last leaf to a very small value — it must bubble up.
	leaf := h.len - 1
	h.data[leaf].readyAt = h.data[0].readyAt.Add(-10 * time.Second) // well before the minimum
	h.siftInline(leaf)
	assert.True(t, isValidMinHeap(h), "heap property not restored after siftInline on leaf")
}

func TestSiftInline_MidHeapUpdate(t *testing.T) {
	t.Parallel()
	offsets := []time.Duration{
		1 * time.Second,
		4 * time.Second,
		2 * time.Second,
		8 * time.Second,
		6 * time.Second,
		3 * time.Second,
		9 * time.Second,
	}
	h := makeHeap(offsets...)
	require.True(t, isValidMinHeap(h))

	// Update an interior node with an out-of-place value.
	h.data[2].readyAt = h.data[0].readyAt.Add(50 * time.Second)
	h.siftInline(2)
	assert.True(t, isValidMinHeap(h), "heap property not restored after siftInline on interior node")
}

func TestSiftInline_AllElementsSorted(t *testing.T) {
	t.Parallel()
	// Build heap, then for each position corrupt it and repair — always valid.
	offsets := []time.Duration{
		2 * time.Second,
		9 * time.Second,
		4 * time.Second,
		11 * time.Second,
		6 * time.Second,
		1 * time.Second,
		7 * time.Second,
		3 * time.Second,
	}
	for pos := range len(offsets) {
		h := makeHeap(offsets...)
		require.True(t, isValidMinHeap(h))
		// Assign a random-ish value to position pos.
		h.data[pos].readyAt = h.data[0].readyAt.Add(time.Duration(pos+100) * time.Second)
		h.siftInline(pos)
		assert.True(t, isValidMinHeap(h),
			"heap invalid after siftInline at pos=%d", pos)
	}
}

// ---------------------------------------------------------------------------
// collectExcess uses popMaxInline — validate end-to-end eviction policy
// ---------------------------------------------------------------------------

func TestCollectExcess_EvictsNewest(t *testing.T) {
	t.Parallel()
	// Insert items with known readyAt times. After capping at maxHeapSize,
	// the surviving entries must all have readyAt ≤ the evicted entries.
	d := newDeleted(t.Context(), []time.Duration{time.Minute}, 3, false)

	base := time.Now()
	// Insert 5 entries with increasing readyAt values.
	for i := range 5 {
		offset := time.Duration(i+1) * time.Second
		dp := deletedPartition{readyAt: base.Add(offset)}
		d.heap.pushInline(dp)
	}

	evicted := d.collectExcess() // maxHeapSize = 3, must evict 2
	require.Len(t, evicted, 2)

	// The 3 survivors must have smaller readyAt than the 2 evicted items.
	allEvictedTimes := make([]time.Time, len(evicted))
	for i, e := range evicted {
		allEvictedTimes[i] = e.readyAt
	}
	sort.Slice(allEvictedTimes, func(i, j int) bool {
		return allEvictedTimes[i].Before(allEvictedTimes[j])
	})

	// Every surviving heap entry must be before the smallest evicted time.
	minEvicted := allEvictedTimes[0]
	for i := range d.heap.len {
		assert.False(t, d.heap.data[i].readyAt.After(minEvicted),
			"surviving entry readyAt=%v is after minimum evicted readyAt=%v (newest should be evicted)",
			d.heap.data[i].readyAt, minEvicted)
	}
}
