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
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gemini/pkg/metrics"
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
//
// Eviction policy: when the heap exceeds maxHeapSize, entries with the *largest* readyAt
// (i.e. the newest, furthest from their validation deadline) are discarded.  This preserves
// entries that have nearly waited out their full bucket window — the exact opposite of what
// would happen if we evicted the minimum.
type (
	deletedPartition struct {
		readyAt time.Time
		onDone  func()
		keys    typedef.PartitionKeys
		counter int
	}

	// deletedPartitionHeap implements a min-heap based on readyAt time
	// Memory optimized: uses a backing array to avoid per-push allocations
	// Performance optimized: inlined operations, bit-shift arithmetic for indices
	deletedPartitionHeap struct {
		data []deletedPartition
		len  int
	}

	// rateEstimator computes a smoothed estimate of how many partitions per
	// second are being admitted into the deleted-partitions tracker. It uses a
	// fixed accounting window plus an exponential moving average so the estimate
	// is stable under bursty traffic. All state is atomic; no external lock is
	// required and observe() is safe to call from many goroutines concurrently.
	rateEstimator struct {
		windowStartNs  atomic.Int64
		windowCount    atomic.Int64
		ratePerSecBits atomic.Uint64 // float64 bits of the EWMA estimate (events/sec)
	}

	deletedPartitions struct {
		ctx          context.Context
		ch           chan typedef.PartitionKeys
		cancel       context.CancelFunc
		buckets      []time.Duration
		heap         deletedPartitionHeap
		rate         rateEstimator
		deleted      atomic.Uint64
		evicted      atomic.Uint64
		sampledOut   atomic.Uint64
		nextReadyNs  atomic.Int64
		maxHeapSize  int
		residenceSec float64
		batchSize    int
		lastShrinkNs int64
		mu           sync.RWMutex
		closeOnce    sync.Once
	}
)

const (
	initialCapacity = 4096

	// rateWindowNs is the length of the accounting window over which the
	// admission rate is measured before being folded into the EWMA.
	rateWindowNs = int64(time.Second)

	// rateEWMAAlpha is the smoothing factor for the admission-rate EWMA. A
	// higher value reacts faster to changes; a lower value is steadier.
	rateEWMAAlpha = 0.3
)

// observe records that n partitions were admitted at nowNs and rolls the
// accounting window into the EWMA when it has elapsed. It is lock-free and safe
// for concurrent use.
func (e *rateEstimator) observe(n, nowNs int64) {
	e.windowCount.Add(n)

	start := e.windowStartNs.Load()
	if start == 0 {
		// First observation — anchor the window. If several goroutines race
		// here only one wins; the rest simply keep counting.
		e.windowStartNs.CompareAndSwap(0, nowNs)
		return
	}

	elapsed := nowNs - start
	if elapsed < rateWindowNs {
		return
	}

	// Window elapsed: exactly one goroutine rolls it over.
	if !e.windowStartNs.CompareAndSwap(start, nowNs) {
		return
	}

	count := e.windowCount.Swap(0)
	instant := float64(count) / (float64(elapsed) / float64(time.Second))

	for {
		oldBits := e.ratePerSecBits.Load()
		old := math.Float64frombits(oldBits)
		newRate := instant
		if old > 0 {
			newRate = rateEWMAAlpha*instant + (1-rateEWMAAlpha)*old
		}
		if e.ratePerSecBits.CompareAndSwap(oldBits, math.Float64bits(newRate)) {
			return
		}
	}
}

// perSec returns the current smoothed admission rate in events per second, or 0
// if not enough data has been gathered yet.
func (e *rateEstimator) perSec() float64 {
	return math.Float64frombits(e.ratePerSecBits.Load())
}

func newDeleted(base context.Context, timeBuckets []time.Duration, maxHeapSize int, start ...bool) *deletedPartitions {
	// Always return a valid instance so callers can safely use it even when
	// delete buckets are not configured. In that case, we disable background
	// processing and channel emission, but still track counters.

	ctx, cancel := context.WithCancel(base)

	h := deletedPartitionHeap{
		data: make([]deletedPartition, initialCapacity), // Pre-allocate to avoid early allocations
		len:  0,
	}

	heap.Init(&h)

	var ch chan typedef.PartitionKeys
	if len(timeBuckets) > 0 {
		ch = make(chan typedef.PartitionKeys, 50_000)
	} else {
		// When disabled, leave channel nil; reads from it are non-blocking in
		// select with default and nobody should range over it in this mode.
		ch = nil
	}

	d := &deletedPartitions{
		heap:    h,
		ch:      ch,
		ctx:     ctx,
		cancel:  cancel,
		buckets: timeBuckets,
		// Increase batch size so the emitter drains cohorts faster without tight loops
		batchSize:    64,
		maxHeapSize:  maxHeapSize,
		residenceSec: residenceSeconds(timeBuckets),
		lastShrinkNs: time.Now().UnixNano(),
	}

	s := true

	if len(start) > 0 {
		s = start[0]
	}

	// Only start background processing if enabled and requested.
	if s && len(timeBuckets) > 0 {
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

// siftInline restores min-heap order at pos by first bubbling up, then down.
// Use this after placing a replacement element at an arbitrary position.
//
//go:inline
func (h *deletedPartitionHeap) siftInline(pos int) {
	if pos >= h.len {
		return
	}

	elem := h.data[pos]

	// Bubble up
	for pos > 0 {
		parent := (pos - 1) >> 1
		if !elem.readyAt.Before(h.data[parent].readyAt) {
			break
		}
		h.data[pos] = h.data[parent]
		pos = parent
	}
	h.data[pos] = elem

	// Bubble down from the final bubble-up position
	h.fixInline(pos)
}

// popMaxInline removes and returns the element with the largest readyAt value.
// In a min-heap the maximum always lives in a leaf node (indices [len/2, len-1]).
// This is the correct operation for eviction: we discard the newest entries
// (furthest from their validation deadline) rather than the entries that are
// soonest to be validated.
//
//go:inline
func (h *deletedPartitionHeap) popMaxInline() (deletedPartition, bool) {
	if h.len == 0 {
		return deletedPartition{}, false
	}

	// Scan leaf nodes to find the maximum.
	maxIdx := h.len / 2
	for i := maxIdx + 1; i < h.len; i++ {
		if h.data[i].readyAt.After(h.data[maxIdx].readyAt) {
			maxIdx = i
		}
	}

	maxElem := h.data[maxIdx]
	h.len--

	if maxIdx == h.len {
		// Max was the last element — just clear and return.
		h.data[h.len] = deletedPartition{}
		return maxElem, true
	}

	// Replace the gap with the last element and restore heap order.
	h.data[maxIdx] = h.data[h.len]
	h.data[h.len] = deletedPartition{} // Clear for GC
	h.siftInline(maxIdx)

	return maxElem, true
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
	defer d.closeOnce.Do(func() { close(d.ch) })

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
		case d.ch <- earliest.keys:
			processed++

			if earliest.counter >= len(d.buckets) {
				// Final emission done; remove and release original keys once
				_, _ = d.heap.popInline()
				if earliest.onDone != nil {
					earliest.onDone()
				}
				continue
			}

			// Re-schedule with jitter to prevent cohort alignment ("thundering herd")
			next := d.buckets[earliest.counter]
			earliest.readyAt = now.Add(next).Add(jitterDuration(next))
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
		next := d.heap.data[0].readyAt
		d.nextReadyNs.Store(next.UnixNano())
		// If the next item is already ready (<= now), keep draining aggressively
		if !next.After(now) {
			return time.Millisecond
		}
		return next.Sub(now)
	}

	d.nextReadyNs.Store(0)
	return 0
}

// residenceSeconds returns the time, in seconds, a partition is expected to
// occupy the heap: the SUM of every bucket delay. A deleted partition is
// re-scheduled through each bucket in turn before it is finally evicted, so it
// stays in the heap for the total of all bucket durations — not just the
// largest one. Using only the max would under-estimate residence for
// multi-bucket configs, under-sample, and let the heap rely on hard eviction.
// Returns 0 when no buckets are configured.
func residenceSeconds(buckets []time.Duration) float64 {
	var total time.Duration
	for _, b := range buckets {
		total += b
	}
	return total.Seconds()
}

// sampleRate returns the probability in (0, 1] with which a freshly-deleted
// partition should be admitted into the heap. The goal is to keep the
// steady-state heap size near maxHeapSize: over one residence window the
// tracker admits roughly admissionRate * residenceSeconds partitions, so
// admitting a maxHeapSize/(admissionRate*residenceSeconds) fraction leaves
// about maxHeapSize entries once partitions start aging out. residenceSeconds
// is the SUM of all bucket delays (a partition passes through every bucket).
//
// Example: at 2000 deletes/s, a single 1h bucket (3600s total residence) and a
// 1M cap, sampleRate = 1e6 / (2000*3600) ≈ 0.139 (≈14%).
//
// Returns 1.0 (admit everything) when sampling is disabled (no cap configured),
// when the admission rate is not yet known, or when the projected inflow is
// already within the cap.
func (d *deletedPartitions) sampleRate() float64 {
	if d.maxHeapSize <= 0 || d.residenceSec <= 0 {
		return 1.0
	}

	rate := d.rate.perSec()
	if rate <= 0 {
		return 1.0
	}

	expected := rate * d.residenceSec
	if expected <= float64(d.maxHeapSize) {
		return 1.0
	}

	return float64(d.maxHeapSize) / expected
}

// admit reports whether a single partition should be kept given a sampling
// rate previously computed by sampleRate. A rate >= 1.0 always admits.
//
//go:inline
func admit(rate float64) bool {
	return rate >= 1.0 || rand.Float64() < rate
}

// dropSampled releases the keys of a partition rejected by the admission
// sampler and updates the relevant counters. The partition is still counted as
// deleted; it is simply not tracked for re-validation.
func (d *deletedPartitions) dropSampled(keys typedef.PartitionKeys) {
	if keys.Release != nil {
		keys.Release()
	}
	d.sampledOut.Add(1)
	metrics.DeletedPartitionsSampledOut.Add(1)
	d.deleted.Add(1)
}

// Delete adds a deleted partition to the tracking system
// Optimized with:
// - Minimal lock time
// - Atomic update of next ready time
// - Pre-computed readyAt time
func (d *deletedPartitions) Delete(keys typedef.PartitionKeys) {
	// If no buckets configured, just count the deletion
	if len(d.buckets) == 0 {
		d.deleted.Add(1)
		if keys.Release != nil {
			keys.Release()
		}
		return
	}

	now := time.Now()

	// Measure the admission rate and sample before touching the heap so that
	// under high DELETE throughput we never let the heap balloon past its cap.
	d.rate.observe(1, now.UnixNano())
	if rate := d.sampleRate(); !admit(rate) {
		d.dropSampled(keys)
		return
	}

	keys.DeletedAtNS = uint64(now.UnixNano())
	// Apply jitter to avoid aligning many items on the exact same boundary
	readyAt := now.Add(d.buckets[0]).Add(jitterDuration(d.buckets[0]))
	readyAtNs := readyAt.UnixNano()

	dp := deletedPartition{
		keys:    keys,
		counter: 1, // Start at 1 since we've already used buckets[0]
		readyAt: readyAt,
	}

	d.mu.Lock()

	// Prevent consumers from releasing multiple times; we'll release on final pop
	dp.onDone = dp.keys.Release
	dp.keys.Release = nil

	wasEmpty := d.heap.Len() == 0
	d.heap.pushInline(dp)

	// Evict oldest entries if the heap exceeds the configured cap.
	// This bounds memory at the cost of skipping re-validation for the
	// evicted (oldest) entries. With high DELETE throughput and long bucket
	// durations, the heap can grow to millions of entries otherwise.
	evicted := d.collectExcess()

	// Update next ready time if this is the earliest or heap was empty
	if wasEmpty || readyAtNs < d.nextReadyNs.Load() {
		d.nextReadyNs.Store(readyAtNs)
	}
	d.mu.Unlock()

	// Run onDone callbacks outside the lock to avoid holding d.mu while
	// invoking partition cleanup (which may acquire other locks).
	d.runEvicted(evicted)

	d.deleted.Add(1)
}

// collectExcess pops the newest (largest readyAt) entries from the heap when
// it exceeds maxHeapSize and returns them. Evicting the entries furthest from
// their validation deadline is the correct policy: entries that have nearly
// waited out their full bucket window are preserved, while freshly-added
// entries with the longest remaining wait are discarded instead.
// The caller is responsible for invoking onDone on each returned entry AFTER
// releasing d.mu.
// MUST be called with d.mu held.
func (d *deletedPartitions) collectExcess() []deletedPartition {
	if d.maxHeapSize <= 0 {
		return nil
	}
	var evicted []deletedPartition
	for d.heap.Len() > d.maxHeapSize {
		e, ok := d.heap.popMaxInline()
		if !ok {
			break
		}
		evicted = append(evicted, e)
	}
	return evicted
}

// runEvicted invokes onDone callbacks for evicted entries and updates metrics.
// MUST be called WITHOUT d.mu held.
func (d *deletedPartitions) runEvicted(evicted []deletedPartition) {
	if len(evicted) == 0 {
		return
	}
	for i := range evicted {
		if evicted[i].onDone != nil {
			evicted[i].onDone()
		}
	}
	n := uint64(len(evicted))
	d.evicted.Add(n)
	metrics.DeletedPartitionsHeapEvictions.Add(float64(n))
}

// DeleteBulk adds multiple deleted partitions in a single lock acquisition
// This is more efficient than calling Delete multiple times when you have many items
func (d *deletedPartitions) DeleteBulk(keys []typedef.PartitionKeys) {
	if len(keys) == 0 {
		return
	}

	// If no buckets configured, just count the deletions and return.
	if len(d.buckets) == 0 {
		d.deleted.Add(uint64(len(keys)))
		for _, k := range keys {
			if k.Release != nil {
				k.Release()
			}
		}
		return
	}

	now := time.Now()

	// Measure the admission rate over the whole batch, then sample per-key so
	// the kept fraction matches the configured cap under high throughput.
	d.rate.observe(int64(len(keys)), now.UnixNano())
	if rate := d.sampleRate(); rate < 1.0 {
		kept := keys[:0]
		var dropped int
		for i := range keys {
			if admit(rate) {
				kept = append(kept, keys[i])
				continue
			}
			if keys[i].Release != nil {
				keys[i].Release()
			}
			dropped++
		}
		keys = kept
		if dropped > 0 {
			d.sampledOut.Add(uint64(dropped))
			metrics.DeletedPartitionsSampledOut.Add(float64(dropped))
			d.deleted.Add(uint64(dropped))
		}
		if len(keys) == 0 {
			return
		}
	}

	// Apply the same jitter policy for bulk scheduling
	readyAt := now.Add(d.buckets[0]).Add(jitterDuration(d.buckets[0]))
	readyAtNs := readyAt.UnixNano()

	d.mu.Lock()

	wasEmpty := d.heap.Len() == 0
	minReadyNs := readyAtNs

	deletedAtNS := uint64(now.UnixNano())
	for i := range keys {
		keys[i].DeletedAtNS = deletedAtNS
		// wrap each to defer release until final
		onDone := keys[i].Release
		keys[i].Release = nil
		d.heap.pushInline(deletedPartition{
			keys:    keys[i],
			onDone:  onDone,
			counter: 1,
			readyAt: readyAt,
		})
	}

	// Evict oldest entries if the heap exceeds the configured cap.
	evicted := d.collectExcess()

	// Update next ready time if needed
	if wasEmpty || minReadyNs < d.nextReadyNs.Load() {
		d.nextReadyNs.Store(minReadyNs)
	}
	d.mu.Unlock()

	// Run onDone callbacks outside the lock.
	d.runEvicted(evicted)

	d.deleted.Add(uint64(len(keys)))
}

// jitterDuration adds a bounded random delay to the given bucket duration to
// spread revalidation over time and prevent synchronized bursts.
// It returns a random duration in [0, min(20% of d, 5s)].
// For non-positive d it returns 0.
//
//go:inline
func jitterDuration(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	maxDuration := time.Duration(float64(d) * 0.2)
	if maxDuration > 5*time.Second {
		maxDuration = 5 * time.Second
	}
	if maxDuration <= 0 {
		return 0
	}
	// Use math/rand/v2 global functions which are safe for concurrent use.
	n := rand.Int64N(int64(maxDuration))
	if n <= 0 {
		return 0
	}
	return time.Duration(n)
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
