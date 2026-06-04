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
	"sync/atomic"

	"github.com/google/uuid"
)

// Fill-zone thresholds that govern when sampling is performed.
// These are exported so validation.go can reference them without magic numbers.
const (
	// FillZoneAlwaysPush: below this fill ratio the queue is lean — always push
	// rows regardless of the configured sample rate.
	FillZoneAlwaysPush = 0.30
	// FillZoneSampled: above this fill ratio apply the probabilistic sample-rate
	// gate before pushing, to slow down queue growth.
	FillZoneSampled = 0.70
	// FillZoneSkip: above this fill ratio the queue is nearly full — skip sampling
	// entirely so we do not waste CPU on work that will be dropped immediately.
	FillZoneSkip = 0.90
)

// TrackedRow holds the minimum information required to identify and delete a
// specific row. It intentionally avoids storing the full PartitionKeys.Values
// map (which contains every column name → CQL value mapping) to keep memory
// usage per entry as small as possible.
//
// Layout rationale:
//   - PartitionID: only the UUID is kept for partition-deletion invalidation.
//   - PartitionValues: flat []any of raw CQL bind values for the PK columns,
//     in the same order as table.PartitionKeys. No column-name keys.
//   - ClusteringValues: flat []any of raw CQL bind values for the CK columns,
//     in column order. Kept separate so callers can use a prefix slice for
//     cluster (prefix) deletes without copying.
//
// There is deliberately no Release closure here. The partition's ref-count
// lifecycle is owned entirely by the statement that triggered validation; the
// tracked row is a lightweight snapshot used only to build a targeted-delete
// query. Carrying a stale closure would risk double-decrement bugs.
type TrackedRow struct {
	PartitionValues  []any
	ClusteringValues []any
	PartitionID      uuid.UUID
}

// RowTracker is a bounded concurrent FIFO queue that stores rows observed
// during validation. Delete operations consume rows from this tracker to
// perform targeted single-row or cluster deletions.
//
// Queue semantics:
//   - Push is a no-op when the queue is full (never overwrites existing rows).
//     This prevents deletes from consuming rows faster than they are produced.
//   - Pop returns the oldest valid entry (FIFO order).
//   - Invalidation bitset: invalidBits []uint64, one bit per slot, sized at
//     construction time. Avoids any map allocation or GC pressure. Each word
//     covers 64 consecutive slots.
//   - When a partition is deleted, Invalidate(id) scans the buffer under the
//     lock, marks matching slots in the bitset, and calls Release immediately.
//   - Pop skips invalidated slots in a tight retry loop; valid entries are
//     returned as-is.
type RowTracker struct {
	buf          []TrackedRow
	invalidBits  []uint64
	head         uint64
	count        atomic.Uint64
	invalidCount atomic.Uint64
	capacity     uint64
	mu           sync.Mutex
}

// NewRowTracker creates a new RowTracker with the given capacity.
// A capacity of 0 disables row tracking (Push/Invalidate are no-ops, Pop
// always returns false).
func NewRowTracker(capacity uint64) *RowTracker {
	if capacity == 0 {
		return &RowTracker{}
	}

	words := (capacity + 63) / 64
	return &RowTracker{
		buf:         make([]TrackedRow, capacity),
		invalidBits: make([]uint64, words),
		capacity:    capacity,
	}
}

// setBit marks slot i as invalid in the bitset (caller must hold mu).
func (rt *RowTracker) setBit(i uint64) {
	rt.invalidBits[i>>6] |= 1 << (i & 63)
}

// clearBit clears the invalid bit for slot i (caller must hold mu).
func (rt *RowTracker) clearBit(i uint64) {
	rt.invalidBits[i>>6] &^= 1 << (i & 63)
}

// testBit reports whether slot i is marked invalid (caller must hold mu).
func (rt *RowTracker) testBit(i uint64) bool {
	return rt.invalidBits[i>>6]&(1<<(i&63)) != 0
}

// Push stores a tracked row. If the queue is full the row is dropped silently.
// This prevents delete workers from consuming rows faster than validation
// populates the queue — when full, validation is already keeping up.
// Safe to call concurrently.
func (rt *RowTracker) Push(row TrackedRow) {
	if rt.capacity == 0 {
		return
	}

	rt.mu.Lock()
	// count is read under the lock; the atomic is only used for lock-free reads
	// in FillRatio/Len. Inside the lock a plain Load is correct.
	if rt.count.Load() >= rt.capacity {
		// Queue is full — drop the row.
		rt.mu.Unlock()
		return
	}

	idx := rt.head % rt.capacity
	rt.buf[idx] = row
	rt.clearBit(idx)
	rt.head++
	rt.count.Add(1)
	rt.mu.Unlock()
}

// Pop removes and returns the oldest valid row from the tracker.
// Invalidated slots are skipped (their Release is called and they are
// consumed from the count). Returns (TrackedRow, true) if a valid row was
// found, or (zero, false) if the tracker is empty.
// Safe to call concurrently.
func (rt *RowTracker) Pop() (TrackedRow, bool) {
	if rt.capacity == 0 {
		return TrackedRow{}, false
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()

	for {
		if rt.count.Load() == 0 {
			return TrackedRow{}, false
		}

		oldestIdx := (rt.head - rt.count.Load()) % rt.capacity
		row := rt.buf[oldestIdx]
		invalid := rt.testBit(oldestIdx)

		// Clear slot.
		rt.buf[oldestIdx] = TrackedRow{}
		rt.clearBit(oldestIdx)
		rt.count.Add(^uint64(0)) // decrement

		if !invalid {
			return row, true
		}
		rt.invalidCount.Add(^uint64(0)) // decrement
	}
}

func (rt *RowTracker) Invalidate(id uuid.UUID) {
	if rt.capacity == 0 {
		return
	}

	rt.mu.Lock()
	n := rt.count.Load()
	for k := uint64(0); k < n; k++ {
		i := (rt.head - n + k) % rt.capacity
		if rt.buf[i].PartitionID != id {
			continue
		}
		if rt.testBit(i) {
			// Already invalidated; nothing to do for this slot.
			continue
		}
		rt.setBit(i)
		rt.invalidCount.Add(1)
		// Do NOT decrement count here. The live-window pointer (head-count)
		// advances sequentially from oldest to newest; decrementing count for
		// a non-oldest slot would move the window past valid entries between
		// the invalidated slot and the current oldest, making them unreachable.
		// Pop's retry loop will consume and discard invalidated slots naturally
		// as they become the oldest entry, decrementing count then.
	}
	rt.mu.Unlock()
}

func (rt *RowTracker) FillRatio() float64 {
	if rt.capacity == 0 {
		return 0
	}
	count := rt.count.Load()
	invalid := rt.invalidCount.Load()
	// Guard against transient invalid > count skew from lock-free reads.
	if invalid >= count {
		return 0
	}
	return float64(count-invalid) / float64(rt.capacity)
}

// Len returns the current number of tracked rows (including any that are
// invalidated but not yet consumed by Pop).
func (rt *RowTracker) Len() uint64 {
	return rt.count.Load()
}

// Capacity returns the maximum number of rows that can be tracked.
func (rt *RowTracker) Capacity() uint64 {
	return rt.capacity
}
