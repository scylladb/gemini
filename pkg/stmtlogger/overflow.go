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

package stmtlogger

// maxOverflowItems is the back-pressure threshold for the producer-side
// overflow buffer: the number of parked items at which LogStmt starts blocking
// producers instead of accepting more (see Logger.LogStmt). It also bounds the
// ring's memory. The downstream committer (the _logs Scylla cluster) being
// stalled for a sustained period is what drives the ring to this size; rather
// than dropping anything, the logger then rate-limits the producers to the
// committer's drain rate. 131072 items keeps a large in-memory buffer (so brief
// committer hiccups never block producers) while preventing the unbounded
// growth (→ OOM) that an append-only slice suffered under a long stall.
//
// Tests inject a smaller per-ring limit via overflowRing.maxItems rather than
// touching this default, so it stays a const.
const maxOverflowItems = 1 << 17

// overflowRing is a bounded FIFO ring buffer of Items used as the producer-side
// spill area when the committer channel is full. It grows lazily (doubling) up
// to maxOverflowItems so idle memory stays small. The Logger never lets push be
// called once the ring is at maxOverflowItems (it blocks the producer instead),
// so the drop-oldest path below is only a defensive backstop — in practice the
// ring never drops.
//
// All operations are O(1) (amortised for push during the lazy-grow phase) and
// allocation-free in steady state — this matters because push runs from gocql
// observer callbacks on the mutation/validation hot path. It is NOT safe for
// concurrent use; the Logger guards it with overflowMu.
type overflowRing struct {
	buf      []Item
	head     int    // index of the oldest item
	count    int    // number of items currently stored
	dropped  uint64 // cumulative items dropped due to the hard cap
	maxItems int    // per-ring cap; 0 means use the package default maxOverflowItems
}

// limit returns the effective cap for this ring (the injected maxItems when set,
// otherwise the package default). Used by both the ring and the Logger's
// back-pressure condition so they always agree.
func (r *overflowRing) limit() int {
	if r.maxItems > 0 {
		return r.maxItems
	}
	return maxOverflowItems
}

// push appends item to the back of the ring. When the ring is already at the
// hard cap the oldest item is overwritten (dropped). It returns true when an
// item was dropped to make room.
func (r *overflowRing) push(item Item) bool {
	n := len(r.buf)

	if r.count == n {
		if n >= r.limit() {
			// At cap: overwrite the oldest item in place.
			r.buf[r.head] = item
			r.head++
			if r.head == n {
				r.head = 0
			}
			r.dropped++
			return true
		}
		r.grow()
		n = len(r.buf)
	}

	idx := r.head + r.count
	if idx >= n {
		idx -= n
	}
	r.buf[idx] = item
	r.count++
	return false
}

// pushFront returns an item to the FRONT of the ring (used to restore an item
// that was popped for a send which then failed). If the ring is full the item
// is dropped — practically unreachable, since this is only used during
// shutdown after producers have stopped pushing.
func (r *overflowRing) pushFront(item Item) {
	if len(r.buf) == 0 {
		r.grow()
	}
	n := len(r.buf)
	if r.count == n {
		r.dropped++
		return
	}
	r.head--
	if r.head < 0 {
		r.head = n - 1
	}
	r.buf[r.head] = item
	r.count++
}

// pop removes and returns the oldest item. The second return is false when the
// ring is empty.
func (r *overflowRing) pop() (Item, bool) {
	if r.count == 0 {
		return Item{}, false
	}
	item := r.buf[r.head]
	r.buf[r.head] = Item{} // release references so the GC can reclaim them
	r.head++
	if r.head == len(r.buf) {
		r.head = 0
	}
	r.count--
	return item, true
}

// reset discards every stored item and releases the backing array.
func (r *overflowRing) reset() {
	r.buf = nil
	r.head = 0
	r.count = 0
}

// grow doubles the backing array (capped at maxOverflowItems), re-laying items
// in logical order so head resets to zero.
func (r *overflowRing) grow() {
	newCap := len(r.buf) * 2
	if newCap < 64 {
		newCap = 64
	}
	// Always clamp to the cap last so the invariant len(buf) <= limit() holds
	// even when the limit is < 64 (only reachable when tests shrink it).
	if limit := r.limit(); newCap > limit {
		newCap = limit
	}

	nb := make([]Item, newCap)
	for i := range r.count {
		src := r.head + i
		if src >= len(r.buf) {
			src -= len(r.buf)
		}
		nb[i] = r.buf[src]
	}
	r.buf = nb
	r.head = 0
}
