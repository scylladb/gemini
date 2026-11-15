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
	"uuid"
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
//     Push also drops a row of a partition that has invalidated rows still in
//     the queue.
//   - Pop returns the oldest valid entry (FIFO order).
//   - live counts the queued rows of each partition. Invalidate(id) moves the
//     count of id to dead in O(1). A row whose partition is in dead is
//     invalid.
//   - Pop and Peek skip invalid rows. Pop removes them from the queue and from
//     dead, so each invalid row costs one skip.
type RowTracker struct {
	// live counts the queued valid rows of each partition.
	live map[uuid.UUID]uint32
	// dead counts the queued invalid rows of each invalidated partition.
	dead         map[uuid.UUID]uint32
	buf          []TrackedRow
	head         uint64
	count        atomic.Uint64
	invalidCount atomic.Uint64
	capacity     uint64
	// peekCursor walks the live rows for Peek. mu guards it.
	peekCursor uint64
	mu         sync.Mutex
}

// NewRowTracker creates a new RowTracker with the given capacity.
// A capacity of 0 disables row tracking (Push/Invalidate are no-ops, Pop
// always returns false).
func NewRowTracker(capacity uint64) *RowTracker {
	if capacity == 0 {
		return &RowTracker{}
	}

	return &RowTracker{
		buf:      make([]TrackedRow, capacity),
		live:     make(map[uuid.UUID]uint32),
		dead:     make(map[uuid.UUID]uint32),
		capacity: capacity,
	}
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
	defer rt.mu.Unlock()

	// count is read under the lock; the atomic is only used for lock-free reads
	// in FillRatio/Len. Inside the lock a plain Load is correct.
	if rt.count.Load() >= rt.capacity {
		// Queue is full — drop the row.
		return
	}

	// A validator that still holds keys of a replaced partition can observe
	// its rows after Invalidate. Those rows target a partition that no longer
	// exists, so they are dropped.
	if _, invalidated := rt.dead[row.PartitionID]; invalidated {
		return
	}

	idx := rt.head % rt.capacity
	rt.buf[idx] = row
	rt.live[row.PartitionID]++
	rt.head++
	rt.count.Add(1)
}

// Pop removes and returns the oldest valid row from the tracker.
// Invalidated rows are skipped and removed from the count. Returns
// (TrackedRow, true) if a valid row was found, or (zero, false) if the tracker
// is empty.
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

		// Clear slot.
		rt.buf[oldestIdx] = TrackedRow{}
		rt.count.Add(^uint64(0)) // decrement

		if decrement(rt.dead, row.PartitionID) {
			rt.invalidCount.Add(^uint64(0)) // decrement
			continue
		}
		decrement(rt.live, row.PartitionID)
		return row, true
	}
}

// decrement lowers the count of id in m and removes the entry at zero. It
// reports whether m held id.
func decrement(m map[uuid.UUID]uint32, id uuid.UUID) bool {
	n, ok := m[id]
	if !ok {
		return false
	}
	if n <= 1 {
		delete(m, id)
	} else {
		m[id] = n - 1
	}
	return true
}

// Peek returns a live tracked row and leaves it in the tracker. Successive
// calls walk the live rows in turn, so readers spread over them. Targeted
// mutations take rows with Pop. A reader that only needs a row's keys uses
// Peek, so it does not take rows away from those mutations.
func (rt *RowTracker) Peek() (TrackedRow, bool) {
	if rt.capacity == 0 {
		return TrackedRow{}, false
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()

	n := rt.count.Load()
	if n == rt.invalidCount.Load() {
		return TrackedRow{}, false
	}
	for range n {
		rt.peekCursor++
		i := (rt.head - n + rt.peekCursor%n) % rt.capacity
		if _, invalidated := rt.dead[rt.buf[i].PartitionID]; !invalidated {
			return rt.buf[i], true
		}
	}

	return TrackedRow{}, false
}

// Invalidate marks every queued row of partition id invalid in constant time.
// It moves the row count of id from live to dead. Pop and Peek skip the rows
// later.
func (rt *RowTracker) Invalidate(id uuid.UUID) {
	if rt.capacity == 0 {
		return
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()

	n, ok := rt.live[id]
	if !ok {
		return
	}
	delete(rt.live, id)
	rt.dead[id] += n
	rt.invalidCount.Add(uint64(n))
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
