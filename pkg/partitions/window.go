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
	"time"

	"github.com/scylladb/gemini/pkg/typedef"
)

const (
	// minRingCap is the smallest non-empty backing-array size a ringBuf grows
	// to. Small so per-bucket rings for short buckets (few live entries) stay
	// cheap; the ring still reclaims to nil when it empties.
	minRingCap = 64

	// shrinkInterval is how often idle ring memory is reclaimed.
	shrinkInterval = int64(60 * time.Second)
)

// bucketEntry is one tracked deleted partition waiting on its current bucket.
// readyAtNs is the Unix-nanos time it becomes due for re-validation. bucket is
// the index of the time bucket it is currently waiting on (0-based); when it
// fires and more buckets remain it advances to bucket+1.
type bucketEntry struct {
	onDone    func()
	keys      typedef.PartitionKeys
	readyAtNs int64
	bucket    int
}

// ringBuf is a growable circular FIFO buffer of bucketEntry — the private
// storage primitive behind window. It is NOT safe for concurrent use and has no
// lock of its own: every method is only ever called by window while holding
// window.mu. Nothing outside this file may touch its fields.
//
// Append at the tail, drain the soonest from the head, evict the newest from the
// tail — all O(1), with head/tail wrapping so no element is ever moved (no s[1:]
// reshuffle, no sift). It grows lazily (doubling) and shrinks when idle, so
// memory tracks the live count rather than any fixed cap.
type ringBuf struct {
	buf  []bucketEntry
	head int // index of the oldest entry
	n    int // number of live entries
}

func (r *ringBuf) len() int { return r.n }

func (r *ringBuf) pushBack(e bucketEntry) {
	if r.n == len(r.buf) {
		r.grow()
	}
	idx := r.head + r.n
	if idx >= len(r.buf) {
		idx -= len(r.buf)
	}
	r.buf[idx] = e
	r.n++
}

func (r *ringBuf) popFront() (bucketEntry, bool) {
	if r.n == 0 {
		return bucketEntry{}, false
	}
	e := r.buf[r.head]
	r.buf[r.head] = bucketEntry{} // release references for GC
	r.head++
	if r.head == len(r.buf) {
		r.head = 0
	}
	r.n--
	return e, true
}

func (r *ringBuf) popBack() (bucketEntry, bool) {
	if r.n == 0 {
		return bucketEntry{}, false
	}
	idx := r.head + r.n - 1
	if idx >= len(r.buf) {
		idx -= len(r.buf)
	}
	e := r.buf[idx]
	r.buf[idx] = bucketEntry{} // release references for GC
	r.n--
	return e, true
}

// peekFront returns a copy of the oldest entry (soonest readyAt) without
// removing it. A copy — never a pointer — so callers cannot mutate ring state.
func (r *ringBuf) peekFront() (bucketEntry, bool) {
	if r.n == 0 {
		return bucketEntry{}, false
	}
	return r.buf[r.head], true
}

// backReadyNs returns the readyAtNs of the newest entry. Caller must ensure
// n > 0.
func (r *ringBuf) backReadyNs() int64 {
	idx := r.head + r.n - 1
	if idx >= len(r.buf) {
		idx -= len(r.buf)
	}
	return r.buf[idx].readyAtNs
}

func (r *ringBuf) shrinkIfIdle() {
	if r.n == 0 {
		r.buf = nil
		r.head = 0
		return
	}
	if r.n >= len(r.buf)/4 || len(r.buf) <= minRingCap {
		return
	}
	newCap := r.n * 2
	if newCap < minRingCap {
		newCap = minRingCap
	}
	r.reseat(newCap)
}

func (r *ringBuf) grow() {
	newCap := len(r.buf) * 2
	if newCap < minRingCap {
		newCap = minRingCap
	}
	r.reseat(newCap)
}

// reseat copies the n live entries into a fresh array of newCap in logical
// (head-first) order, resetting head to 0.
func (r *ringBuf) reseat(newCap int) {
	nb := make([]bucketEntry, newCap)
	for i := range r.n {
		src := r.head + i
		if src >= len(r.buf) {
			src -= len(r.buf)
		}
		nb[i] = r.buf[src]
	}
	r.buf = nb
	r.head = 0
}

// window is the sliding-window tracker for deleted partitions: one ringBuf per
// time bucket plus the global live count and hard cap, all guarded by a single
// mutex it owns. It is fully self-contained and the ONLY way to touch the
// underlying rings — callers interact exclusively through its methods, which
// return entries by value and never expose internal pointers or state.
type window struct {
	rings        []ringBuf
	buckets      []time.Duration
	mu           sync.Mutex
	nextReady    atomic.Int64 // lock-free fast-path snapshot of the soonest readyAt; 0 = nothing tracked
	live         int
	maxItems     int
	lastShrinkNs int64
}

func newWindow(buckets []time.Duration, maxItems int, nowNs int64) *window {
	return &window{
		rings:        make([]ringBuf, len(buckets)),
		buckets:      buckets,
		maxItems:     maxItems,
		lastShrinkNs: nowNs,
	}
}

// nextReadyNs returns the soonest readyAt across all buckets without locking, or
// 0 when nothing is tracked. Used for the lock-free fast path.
func (w *window) nextReadyNs() int64 { return w.nextReady.Load() }

// len returns the number of partitions currently tracked.
func (w *window) len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.live
}

// push appends one entry to bucket 0, enforces the global cap, and refreshes the
// next-ready snapshot. It returns any entries evicted by the cap so the caller
// can run their onDone callbacks outside any lock.
func (w *window) push(e bucketEntry) []bucketEntry {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.rings[0].pushBack(e)
	w.live++
	evicted := w.evictLocked()
	w.refreshNextReadyLocked()
	return evicted
}

// pushBulk appends many entries to bucket 0 under a single lock acquisition.
func (w *window) pushBulk(entries []bucketEntry) []bucketEntry {
	w.mu.Lock()
	defer w.mu.Unlock()
	for _, e := range entries {
		w.rings[0].pushBack(e)
		w.live++
	}
	evicted := w.evictLocked()
	w.refreshNextReadyLocked()
	return evicted
}

// drainReady emits entries whose readyAt has passed (as of nowNs), up to
// batchSize, via send. send reports whether the entry was accepted by the
// consumer; when it returns false (e.g. the channel is full) draining stops.
// Entries that complete their final bucket have their onDone collected and
// returned for the caller to run after the lock is released; entries with
// buckets remaining are re-scheduled into the next bucket. The returned duration
// is how long until the next entry is ready (a small value means keep draining).
func (w *window) drainReady(nowNs int64, batchSize int, send func(typedef.PartitionKeys) bool) ([]func(), time.Duration) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.maybeShrinkLocked(nowNs)

	var onDones []func()
	processed := 0
	for processed < batchSize {
		ri := w.soonestLocked()
		if ri < 0 {
			break // nothing tracked
		}

		front, _ := w.rings[ri].peekFront()
		if nowNs < front.readyAtNs {
			w.nextReady.Store(front.readyAtNs)
			return onDones, time.Duration(front.readyAtNs - nowNs)
		}

		if !send(front.keys) {
			// Consumer not ready (channel full); retry shortly.
			w.refreshNextReadyLocked()
			return onDones, 10 * time.Millisecond
		}

		processed++
		e, _ := w.rings[ri].popFront()

		if e.bucket+1 >= len(w.buckets) {
			// Final emission done; the original keys are released once, later.
			w.live--
			if e.onDone != nil {
				onDones = append(onDones, e.onDone)
			}
			continue
		}

		// Re-schedule into the next bucket with jitter to prevent cohort
		// alignment ("thundering herd"). The entry moves between rings, so
		// live is unchanged.
		e.bucket++
		next := w.buckets[e.bucket]
		e.readyAtNs = nowNs + int64(next) + int64(jitterDuration(next))
		w.rings[e.bucket].pushBack(e)
	}

	// Publish the next ready time, or signal keep-draining if more is due.
	if ri := w.soonestLocked(); ri >= 0 {
		front, _ := w.rings[ri].peekFront()
		w.nextReady.Store(front.readyAtNs)
		if front.readyAtNs <= nowNs {
			return onDones, time.Millisecond
		}
		return onDones, time.Duration(front.readyAtNs - nowNs)
	}

	w.nextReady.Store(0)
	return onDones, 0
}

// evictLocked enforces the global hard cap: while the total live count exceeds
// maxItems it evicts the newest entry (largest readyAt across all ring tails),
// preserving the soonest-ready entries (ring heads are never touched). Each
// eviction is O(1) plus an O(#buckets) scan to pick the ring. MUST hold w.mu.
func (w *window) evictLocked() []bucketEntry {
	if w.maxItems <= 0 {
		return nil
	}
	var evicted []bucketEntry
	for w.live > w.maxItems {
		best := -1
		var bestNs int64
		for i := range w.rings {
			if w.rings[i].len() == 0 {
				continue
			}
			ns := w.rings[i].backReadyNs()
			if best < 0 || ns > bestNs {
				best = i
				bestNs = ns
			}
		}
		if best < 0 {
			break
		}
		e, ok := w.rings[best].popBack()
		if !ok {
			break
		}
		evicted = append(evicted, e)
		w.live--
	}
	return evicted
}

// soonestLocked returns the index of the ring whose head has the smallest
// readyAtNs, or -1 if every ring is empty. MUST hold w.mu.
func (w *window) soonestLocked() int {
	best := -1
	var bestNs int64
	for i := range w.rings {
		front, ok := w.rings[i].peekFront()
		if !ok {
			continue
		}
		if best < 0 || front.readyAtNs < bestNs {
			best = i
			bestNs = front.readyAtNs
		}
	}
	return best
}

// refreshNextReadyLocked recomputes the soonest ready time and publishes it for
// the lock-free fast path. Stores 0 when nothing is tracked. MUST hold w.mu.
func (w *window) refreshNextReadyLocked() {
	if ri := w.soonestLocked(); ri >= 0 {
		front, _ := w.rings[ri].peekFront()
		w.nextReady.Store(front.readyAtNs)
		return
	}
	w.nextReady.Store(0)
}

// maybeShrinkLocked reclaims idle ring memory at most once per shrinkInterval.
// MUST hold w.mu.
func (w *window) maybeShrinkLocked(nowNs int64) {
	if nowNs-w.lastShrinkNs <= shrinkInterval {
		return
	}
	for i := range w.rings {
		w.rings[i].shrinkIfIdle()
	}
	w.lastShrinkNs = nowNs
}
