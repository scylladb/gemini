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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

// --- test-only accessors (methods on the types themselves, so the sliding
// window is still the only thing touching its internals) ---------------------

func (r *ringBuf) cap() int { return len(r.buf) }

func (r *ringBuf) snapshot() []bucketEntry {
	out := make([]bucketEntry, 0, r.n)
	for i := range r.n {
		idx := r.head + i
		if idx >= len(r.buf) {
			idx -= len(r.buf)
		}
		out = append(out, r.buf[idx])
	}
	return out
}

func (w *window) peekSoonestForTest() (bucketEntry, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	ri := w.soonestLocked()
	if ri < 0 {
		return bucketEntry{}, false
	}
	return w.rings[ri].peekFront()
}

func (w *window) entriesForTest() []bucketEntry {
	w.mu.Lock()
	defer w.mu.Unlock()
	var out []bucketEntry
	for i := range w.rings {
		out = append(out, w.rings[i].snapshot()...)
	}
	return out
}

func (w *window) totalCapForTest() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	c := 0
	for i := range w.rings {
		c += w.rings[i].cap()
	}
	return c
}

func (d *deletedPartitions) peekSoonestForTest() (bucketEntry, bool) {
	return d.win.peekSoonestForTest()
}

func (d *deletedPartitions) entriesForTest() []bucketEntry { return d.win.entriesForTest() }

func (d *deletedPartitions) totalRingCapForTest() int { return d.win.totalCapForTest() }

// --- ringBuf unit tests -----------------------------------------------------

func ent(ns int64) bucketEntry { return bucketEntry{readyAtNs: ns} }

func TestRingBuf_FIFOOrder(t *testing.T) {
	t.Parallel()
	var r ringBuf
	for i := range int64(10) {
		r.pushBack(ent(i))
	}
	require.Equal(t, 10, r.len())
	for i := range int64(10) {
		e, ok := r.popFront()
		require.True(t, ok)
		assert.Equal(t, i, e.readyAtNs)
	}
	_, ok := r.popFront()
	assert.False(t, ok)
}

func TestRingBuf_PopBackEvictsNewest(t *testing.T) {
	t.Parallel()
	var r ringBuf
	for i := range int64(5) {
		r.pushBack(ent(i)) // 0,1,2,3,4 — newest is 4
	}
	e, ok := r.popBack()
	require.True(t, ok)
	assert.Equal(t, int64(4), e.readyAtNs, "popBack must remove the newest (largest readyAt)")
	assert.Equal(t, 4, r.len())
	front, _ := r.peekFront()
	assert.Equal(t, int64(0), front.readyAtNs, "head (soonest) must be untouched")
}

func TestRingBuf_WrapAround(t *testing.T) {
	t.Parallel()
	var r ringBuf
	for i := range int64(minRingCap) {
		r.pushBack(ent(i))
	}
	for range minRingCap / 2 {
		_, _ = r.popFront()
	}
	for i := range int64(minRingCap / 2) {
		r.pushBack(ent(minRingCap + i))
	}
	require.Equal(t, minRingCap, r.len())
	prev := int64(-1)
	for r.len() > 0 {
		e, _ := r.popFront()
		assert.Greater(t, e.readyAtNs, prev, "FIFO order must hold across wrap")
		prev = e.readyAtNs
	}
}

func TestRingBuf_GrowAndShrink(t *testing.T) {
	t.Parallel()
	var r ringBuf
	const n = 10_000
	for i := range int64(n) {
		r.pushBack(ent(i))
	}
	require.Equal(t, n, r.len())
	grownCap := r.cap()
	assert.GreaterOrEqual(t, grownCap, n)

	for range n - 10 {
		_, _ = r.popFront()
	}
	r.shrinkIfIdle()
	assert.Less(t, r.cap(), grownCap, "shrinkIfIdle must reclaim after heavy drain")
	assert.Equal(t, 10, r.len())

	for r.len() > 0 {
		_, _ = r.popFront()
	}
	r.shrinkIfIdle()
	assert.Equal(t, 0, r.cap(), "empty ring releases its backing array")
}

func TestRingBuf_EmptyOps(t *testing.T) {
	t.Parallel()
	var r ringBuf
	_, ok := r.popFront()
	assert.False(t, ok)
	_, ok = r.popBack()
	assert.False(t, ok)
	_, ok = r.peekFront()
	assert.False(t, ok)
	assert.Equal(t, 0, r.len())
}

// --- window unit tests ------------------------------------------------------

func TestWindow_CapEvictsNewestKeepsSoonest(t *testing.T) {
	t.Parallel()
	// cap = 3, single bucket. Push 5 (readyAt 100..104); the window must trim to
	// 3, evicting the two newest (103, 104) and keeping the soonest (100).
	w := newWindow([]time.Duration{time.Minute}, 3, 0)
	allEvicted := make([]bucketEntry, 0, 2)
	for i := range int64(5) {
		allEvicted = append(allEvicted, w.push(bucketEntry{readyAtNs: 100 + i})...)
	}
	assert.Equal(t, 3, w.len(), "window must be trimmed to the cap")

	front, ok := w.peekSoonestForTest()
	require.True(t, ok)
	assert.Equal(t, int64(100), front.readyAtNs, "soonest-ready entry must survive trimming")

	require.Len(t, allEvicted, 2)
	assert.ElementsMatch(t, []int64{103, 104},
		[]int64{allEvicted[0].readyAtNs, allEvicted[1].readyAtNs},
		"the two newest entries must be the ones evicted")
}

func TestWindow_DrainEmitsAndReschedules(t *testing.T) {
	t.Parallel()
	// Two buckets: an entry due in bucket 0 is emitted, then re-scheduled into
	// bucket 1 (still tracked, not finalized).
	w := newWindow([]time.Duration{time.Millisecond, time.Hour}, 0, 0)
	w.push(bucketEntry{readyAtNs: 50, bucket: 0})

	var got []typedef.PartitionKeys
	send := func(k typedef.PartitionKeys) bool { got = append(got, k); return true }

	onDones, _ := w.drainReady(100, 64, send) // now=100 > 50 → ready
	assert.Len(t, got, 1, "entry should be emitted")
	assert.Empty(t, onDones, "not final yet — bucket 1 remains")
	assert.Equal(t, 1, w.len(), "entry stays tracked, moved to the next bucket")

	front, ok := w.peekSoonestForTest()
	require.True(t, ok)
	assert.Equal(t, 1, front.bucket, "entry advanced to bucket 1")
	assert.Greater(t, front.readyAtNs, int64(100), "rescheduled readyAt is in the future")
}

func TestWindow_DrainFinalizesLastBucket(t *testing.T) {
	t.Parallel()
	// Single bucket: a due entry is emitted and finalized (onDone returned).
	w := newWindow([]time.Duration{time.Millisecond}, 0, 0)
	called := false
	w.push(bucketEntry{readyAtNs: 50, bucket: 0, onDone: func() { called = true }})

	send := func(typedef.PartitionKeys) bool { return true }
	onDones, _ := w.drainReady(100, 64, send)

	require.Len(t, onDones, 1, "final-bucket entry must return its onDone")
	assert.Equal(t, 0, w.len(), "finalized entry is removed")
	onDones[0]()
	assert.True(t, called, "returned callback is the entry's onDone")
}

func TestWindow_DrainStopsWhenSendFull(t *testing.T) {
	t.Parallel()
	// When send reports the consumer is full, draining stops and nothing is lost.
	w := newWindow([]time.Duration{time.Millisecond}, 0, 0)
	w.push(bucketEntry{readyAtNs: 50, bucket: 0})

	send := func(typedef.PartitionKeys) bool { return false } // consumer full
	onDones, wait := w.drainReady(100, 64, send)
	assert.Empty(t, onDones)
	assert.Equal(t, 1, w.len(), "entry must remain tracked when the consumer is full")
	assert.Positive(t, wait, "should ask to retry shortly")
}
