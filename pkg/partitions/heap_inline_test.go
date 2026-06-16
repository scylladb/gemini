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
// evictLeafInline — O(1) hard-cap backstop eviction
// ---------------------------------------------------------------------------

func TestEvictLeafInline_Empty(t *testing.T) {
	t.Parallel()
	h := &deletedPartitionHeap{data: make([]deletedPartition, 8)}
	_, ok := h.evictLeafInline()
	assert.False(t, ok)
}

func TestEvictLeafInline_Single(t *testing.T) {
	t.Parallel()
	h := makeHeap(10 * time.Second)
	elem, ok := h.evictLeafInline()
	require.True(t, ok)
	assert.Equal(t, 0, h.len)
	assert.False(t, elem.readyAt.IsZero())
}

// evictLeafInline removes the last array slot, which is always a leaf, so the
// heap stays valid with no sift and the count drops by exactly one each time.
func TestEvictLeafInline_ReducesLenAndPreservesHeap(t *testing.T) {
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
	require.True(t, isValidMinHeap(h))

	for h.len > 0 {
		prevLen := h.len
		_, ok := h.evictLeafInline()
		require.True(t, ok)
		assert.Equal(t, prevLen-1, h.len)
		assert.True(t, isValidMinHeap(h),
			"min-heap property violated after evictLeafInline (len=%d)", h.len)
	}
}

// The root (soonest-ready entry) must survive eviction down to the last element:
// evicting a leaf never removes the minimum.
func TestEvictLeafInline_NeverEvictsRoot(t *testing.T) {
	t.Parallel()
	offsets := []time.Duration{
		1 * time.Second, // minimum / soonest-ready
		9 * time.Second,
		4 * time.Second,
		11 * time.Second,
		6 * time.Second,
		2 * time.Second,
		7 * time.Second,
	}
	h := makeHeap(offsets...)
	rootReadyAt := h.data[0].readyAt

	for h.len > 1 {
		evicted, ok := h.evictLeafInline()
		require.True(t, ok)
		// The minimum (root) is never the element evicted (offsets are distinct).
		assert.NotEqual(t, rootReadyAt, evicted.readyAt, "evicted entry must not be the soonest-ready root")
		// Root remains the global minimum after each eviction.
		assert.Equal(t, rootReadyAt, h.data[0].readyAt, "root (soonest-ready) changed after leaf eviction")
		assert.True(t, isValidMinHeap(h))
	}
}

// ---------------------------------------------------------------------------
// collectExcess — end-to-end hard-cap trimming
// ---------------------------------------------------------------------------

func TestCollectExcess_TrimsToCapPreservingSoonest(t *testing.T) {
	t.Parallel()
	// maxHeapSize = 3; insert 5 entries with increasing readyAt. collectExcess
	// must trim the heap down to the cap, keep the heap valid, and preserve the
	// soonest-ready entry (the root) — it only ever evicts leaves.
	d := newDeleted(t.Context(), []time.Duration{time.Minute}, 3, false)

	base := time.Now()
	soonest := base.Add(1 * time.Second)
	for i := range 5 {
		offset := time.Duration(i+1) * time.Second
		d.heap.pushInline(deletedPartition{readyAt: base.Add(offset)})
	}
	require.Equal(t, soonest, d.heap.data[0].readyAt, "soonest entry should be at the root before trimming")

	evicted := d.collectExcess() // 5 -> 3, must evict 2
	require.Len(t, evicted, 2)
	assert.Equal(t, 3, d.heap.len, "heap must be trimmed to maxHeapSize")
	assert.True(t, isValidMinHeap(&d.heap), "heap property must hold after collectExcess")
	assert.Equal(t, soonest, d.heap.data[0].readyAt,
		"the soonest-ready entry must survive trimming (eviction only removes leaves)")
}
