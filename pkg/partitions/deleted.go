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
	"container/heap"
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gemini/pkg/typedef"
)

// deletedPartitions implements a time-bucket based tracking system for deleted partitions.
//
// Design:
// - Uses a min-heap (priority queue) sorted by readyAt time for O(log n) insertion and O(1) peek
// - Background goroutine continuously checks the heap and emits ready partitions to a channel
// - Mutex protects the heap for concurrent access from Delete() and background processor
// - Atomic counter tracks total number of deletions for statistics
// - Memory optimized: pre-allocated backing array to minimize allocations
// - Performance optimized: inlined heap operations, bit-shift indices, sync.Pool for partition objects
//
// The heap ensures we always process partitions in time order, making it efficient to check
// if any partitions are ready for validation without scanning all entries.
type (
	deletedPartition struct {
		readyAt time.Time
		values  *typedef.Values
		counter int
	}

	// deletedPartitionHeap implements a min-heap based on readyAt time
	// Memory optimized: uses a backing array to avoid per-push allocations
	// Performance optimized: inlined operations, bit-shift arithmetic for indices
	deletedPartitionHeap struct {
		data []deletedPartition
		len  int
	}

	deletedPartitions struct {
		ctx          context.Context
		ch           chan *typedef.Values
		cancel       context.CancelFunc
		buckets      []time.Duration
		heap         deletedPartitionHeap
		deleted      atomic.Uint64
		nextReadyNs  atomic.Int64
		batchSize    int
		lastShrinkNs int64
		mu           sync.RWMutex
	}
)

const initialCapacity = 1024

func newDeleted(base context.Context, timeBuckets []time.Duration, start ...bool) *deletedPartitions {
	ctx, cancel := context.WithCancel(base)

	h := deletedPartitionHeap{
		data: make([]deletedPartition, initialCapacity), // Pre-allocate to avoid early allocations
		len:  0,
	}

	heap.Init(&h)

	d := &deletedPartitions{
		heap:         h,
		ch:           make(chan *typedef.Values, 100),
		ctx:          ctx,
		cancel:       cancel,
		buckets:      timeBuckets,
		batchSize:    16, // Process up to 16 items per batch for better throughput
		lastShrinkNs: time.Now().UnixNano(),
	}

	s := true

	if len(start) > 0 {
		s = start[0]
	}

	if s {
		go d.start(100 * time.Millisecond)
	}

	return d
}

// Heap interface implementation for deletedPartitionHeap
func (h *deletedPartitionHeap) Len() int { return h.len }

func (h *deletedPartitionHeap) Less(i, j int) bool {
	return h.data[i].readyAt.Before(h.data[j].readyAt)
}

func (h *deletedPartitionHeap) Swap(i, j int) {
	h.data[i], h.data[j] = h.data[j], h.data[i]
}

func (h *deletedPartitionHeap) Push(x any) {
	// Grow capacity if needed - double the size to amortize allocations
	if h.len >= len(h.data) {
		newCap := len(h.data) * 2
		if newCap == 0 {
			newCap = 64 // Initial capacity
		}
		newData := make([]deletedPartition, newCap)
		copy(newData, h.data[:h.len])
		h.data = newData
	}
	h.data[h.len] = x.(deletedPartition)
	h.len++
}

func (h *deletedPartitionHeap) Pop() any {
	h.len--
	item := h.data[h.len]
	// Clear the popped element to allow GC
	h.data[h.len] = deletedPartition{}
	return item
}

// peek returns the minimum element without removing it
//
//go:inline
func (h *deletedPartitionHeap) peek() *deletedPartition {
	if h.len == 0 {
		return nil
	}
	return &h.data[0]
}

// peekTimeNs returns the readyAt time of the root element as Unix nanoseconds
// Returns 0 if heap is empty. Used for lock-free fast path checks.
//
//go:inline
func (h *deletedPartitionHeap) peekTimeNs() int64 {
	if h.len == 0 {
		return 0
	}
	return h.data[0].readyAt.UnixNano()
}

// pushInline is a highly optimized heap insertion that avoids allocations
// Uses bit-shift arithmetic for parent calculation: (pos-1)>>1 == (pos-1)/2
//
//go:inline
func (h *deletedPartitionHeap) pushInline(dp deletedPartition) {
	// Grow capacity if needed - use bit-shift for doubling
	if h.len >= len(h.data) {
		newCap := len(h.data) << 1 // Same as *2 but faster
		if newCap == 0 {
			newCap = 64
		}
		newData := make([]deletedPartition, newCap)
		copy(newData, h.data[:h.len])
		h.data = newData
	}

	// Add at end and bubble up
	pos := h.len
	h.len++

	// Bubble up using bit-shift for parent: (pos-1)>>1 instead of (pos-1)/2
	for pos > 0 {
		parent := (pos - 1) >> 1
		if !dp.readyAt.Before(h.data[parent].readyAt) {
			break
		}
		h.data[pos] = h.data[parent]
		pos = parent
	}
	h.data[pos] = dp
}

// popInline removes and returns the minimum element
// Uses bit-shift arithmetic for child calculations: pos<<1+1 == pos*2+1
//
//go:inline
func (h *deletedPartitionHeap) popInline() (deletedPartition, bool) {
	if h.len == 0 {
		return deletedPartition{}, false
	}

	minElem := h.data[0]
	h.len--

	if h.len == 0 {
		h.data[0] = deletedPartition{}
		return minElem, true
	}

	// Move last element to root and bubble down
	last := h.data[h.len]
	h.data[h.len] = deletedPartition{} // Clear for GC

	pos := 0
	for {
		left := (pos << 1) + 1 // Same as pos*2+1 but faster
		if left >= h.len {
			break
		}

		// Find smaller child using bit-shift
		right := left + 1
		smallest := left
		if right < h.len && h.data[right].readyAt.Before(h.data[left].readyAt) {
			smallest = right
		}

		// Check if we need to continue
		if !h.data[smallest].readyAt.Before(last.readyAt) {
			break
		}

		h.data[pos] = h.data[smallest]
		pos = smallest
	}
	h.data[pos] = last

	return minElem, true
}

// fixInline repairs the heap after modifying the root element
// Uses bit-shift arithmetic for efficient child index calculation
//
//go:inline
func (h *deletedPartitionHeap) fixInline(pos int) {
	if pos >= h.len {
		return
	}

	elem := h.data[pos]

	// Bubble down
	for {
		left := (pos << 1) + 1
		if left >= h.len {
			break
		}

		right := left + 1
		smallest := left
		if right < h.len && h.data[right].readyAt.Before(h.data[left].readyAt) {
			smallest = right
		}

		if !h.data[smallest].readyAt.Before(elem.readyAt) {
			break
		}

		h.data[pos] = h.data[smallest]
		pos = smallest
	}
	h.data[pos] = elem
}

// shrinkIfNeeded reduces the backing array size if utilization is very low
// This prevents memory bloat after temporary spikes
//
//go:inline
func (h *deletedPartitionHeap) shrinkIfNeeded() {
	if h.len < cap(h.data)/4 {
		// Shrink to 2x current size (maintain some headroom)
		newCap := max(h.len*2, 1024)
		newData := make([]deletedPartition, newCap)
		copy(newData, h.data[:h.len])
		h.data = newData
	}
}

// start processes deleted partitions and sends them to the channel when ready
// Optimized with adaptive check intervals based on next ready time
func (d *deletedPartitions) start(checkInterval time.Duration) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()
	defer close(d.ch)

	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			if nextCheck := d.processReady(); nextCheck > 0 {
				ticker.Reset(nextCheck)
			} else {
				ticker.Reset(checkInterval)
			}
		}
	}
}

// processReady checks for partitions that are ready for validation and sends them to the channel
// Optimized with:
// - Batch processing to reduce lock overhead
// - Fast UnixNano time comparisons
// - Periodic heap shrinking to reduce memory
func (d *deletedPartitions) processReady() time.Duration {
	nowNs := time.Now().UnixNano()
	now := time.Unix(0, nowNs)

	// Fast path: check if anything is ready without locking
	if nextNs := d.nextReadyNs.Load(); nextNs > 0 && nowNs < nextNs {
		// Nothing ready yet, return wait time
		return time.Duration(nextNs - nowNs)
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	// Check for heap shrinking every 60 seconds
	if nowNs-d.lastShrinkNs > int64(60*time.Second) {
		d.heap.shrinkIfNeeded()
		d.lastShrinkNs = nowNs
	}

	processed := 0
	for d.heap.Len() > 0 && processed < d.batchSize {
		earliest := d.heap.peek()

		// Use UnixNano for faster comparison
		if nowNs < earliest.readyAt.UnixNano() {
			// Update next ready time atomically for fast path
			d.nextReadyNs.Store(earliest.readyAt.UnixNano())
			return earliest.readyAt.Sub(now)
		}

		select {
		case d.ch <- earliest.values:
			processed++

			if earliest.counter >= len(d.buckets) {
				d.heap.popInline()
				continue
			}

			earliest.readyAt = now.Add(d.buckets[earliest.counter])
			earliest.counter++

			d.heap.fixInline(0)

		case <-d.ctx.Done():
			d.nextReadyNs.Store(0)
			return 0
		default:
			// Channel full, try again later
			if d.heap.Len() > 0 {
				d.nextReadyNs.Store(d.heap.data[0].readyAt.UnixNano())
			}
			return 10 * time.Millisecond
		}
	}

	// Update next ready time or clear it
	if d.heap.Len() > 0 {
		d.nextReadyNs.Store(d.heap.data[0].readyAt.UnixNano())
		return d.heap.data[0].readyAt.Sub(now)
	}

	d.nextReadyNs.Store(0)
	return 0
}

// Delete adds a deleted partition to the tracking system
// Optimized with:
// - Minimal lock time
// - Atomic update of next ready time
// - Pre-computed readyAt time
func (d *deletedPartitions) Delete(values *typedef.Values) {
	// If no buckets configured, just count the deletion
	if len(d.buckets) == 0 {
		d.deleted.Add(1)
		return
	}

	now := time.Now()
	readyAt := now.Add(d.buckets[0])
	readyAtNs := readyAt.UnixNano()

	dp := deletedPartition{
		values:  values,
		counter: 1, // Start at 1 since we've already used buckets[0]
		readyAt: readyAt,
	}

	d.mu.Lock()

	wasEmpty := d.heap.Len() == 0
	d.heap.pushInline(dp)

	// Update next ready time if this is the earliest or heap was empty
	if wasEmpty || readyAtNs < d.nextReadyNs.Load() {
		d.nextReadyNs.Store(readyAtNs)
	}
	d.mu.Unlock()

	d.deleted.Add(1)
}

// DeleteBulk adds multiple deleted partitions in a single lock acquisition
// This is more efficient than calling Delete multiple times when you have many items
func (d *deletedPartitions) DeleteBulk(values []*typedef.Values) {
	if len(values) == 0 {
		return
	}

	now := time.Now()
	readyAt := now.Add(d.buckets[0])
	readyAtNs := readyAt.UnixNano()

	d.mu.Lock()

	wasEmpty := d.heap.Len() == 0
	minReadyNs := readyAtNs

	for _, v := range values {
		d.heap.pushInline(deletedPartition{
			values:  v,
			counter: 1,
			readyAt: readyAt,
		})
	}

	// Update next ready time if needed
	if wasEmpty || minReadyNs < d.nextReadyNs.Load() {
		d.nextReadyNs.Store(minReadyNs)
	}
	d.mu.Unlock()

	d.deleted.Add(uint64(len(values)))
}

// Close stops the background processor
func (d *deletedPartitions) Close() {
	d.cancel()
}

// Len returns the number of partitions waiting for validation
func (d *deletedPartitions) Len() int {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.heap.Len()
}
