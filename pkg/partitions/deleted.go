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
	"context"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
)

// deletedPartitions tracks deleted partitions awaiting re-validation.
//
// Storage and all locking live in the encapsulated window (one ringBuf per time
// bucket, see window.go); deletedPartitions itself holds NO mutex over the
// tracked entries — it only orchestrates admission sampling, the emit channel,
// lifetime counters, and the background drainer goroutine, delegating every
// storage operation to the window's methods.
//
// A freshly-deleted partition is admitted (subject to sampling) into bucket 0;
// when its readyAt fires it is emitted to the channel and, if more buckets
// remain, moved to the next bucket. Admission sampling (rateEstimator/sampleRate)
// is the primary memory bound; the window's hard-cap eviction is only a backstop.
type (
	// rateEstimator computes a smoothed estimate of how many partitions per
	// second are being DELETED — i.e. the incoming delete rate, measured before
	// admission sampling (observe() is called on every Delete/DeleteBulk, ahead
	// of the sampleRate()/admit() decision). sampleRate() then derives the
	// admission probability from this pre-sampling rate. It uses a fixed
	// accounting window plus an exponential moving average so the estimate is
	// stable under bursty traffic. All state is atomic; no external lock is
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
		win          *window
		buckets      []time.Duration
		rate         rateEstimator
		deleted      atomic.Uint64
		evicted      atomic.Uint64
		sampledOut   atomic.Uint64
		residenceSec float64
		batchSize    int
		closeOnce    sync.Once
	}
)

const (
	// rateWindowNs is the length of the accounting window over which the
	// incoming delete rate is measured before being folded into the EWMA.
	rateWindowNs = int64(time.Second)

	// rateEWMAAlpha is the smoothing factor for the delete-rate EWMA. A
	// higher value reacts faster to changes; a lower value is steadier.
	rateEWMAAlpha = 0.3
)

// observe records that n partitions were deleted at nowNs (counted before
// admission sampling) and rolls the accounting window into the EWMA when it has
// elapsed. It is lock-free and safe for concurrent use.
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

	var ch chan typedef.PartitionKeys
	if len(timeBuckets) > 0 {
		ch = make(chan typedef.PartitionKeys, 50_000)
	} else {
		// When disabled, leave channel nil; reads from it are non-blocking in
		// select with default and nobody should range over it in this mode.
		ch = nil
	}

	d := &deletedPartitions{
		ch:      ch,
		ctx:     ctx,
		cancel:  cancel,
		win:     newWindow(timeBuckets, maxHeapSize, time.Now().UnixNano()),
		buckets: timeBuckets,
		// Larger batch so the emitter drains cohorts faster without tight loops.
		batchSize:    64,
		residenceSec: residenceSeconds(timeBuckets),
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

// start processes deleted partitions and sends them to the channel when ready.
// Adaptive check interval based on the next ready time.
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

// trySend performs a non-blocking send of keys to the emit channel.
func (d *deletedPartitions) trySend(keys typedef.PartitionKeys) bool {
	select {
	case d.ch <- keys:
		return true
	default:
		return false
	}
}

// processReady drains entries whose readyAt has passed to the channel. It uses
// the window's lock-free next-ready snapshot for the fast path and delegates all
// storage work to the window.
func (d *deletedPartitions) processReady() time.Duration {
	nowNs := time.Now().UnixNano()

	// Fast path: nothing ready yet, return the wait without touching the window
	// lock.
	if nextNs := d.win.nextReadyNs(); nextNs > 0 && nowNs < nextNs {
		return time.Duration(nextNs - nowNs)
	}

	onDones, wait := d.win.drainReady(nowNs, d.batchSize, d.trySend)
	// Run final-release callbacks outside the window lock.
	for _, done := range onDones {
		if done != nil {
			done()
		}
	}
	return wait
}

// residenceSeconds returns the time, in seconds, a partition is expected to
// occupy the tracker: the SUM of every bucket delay. A deleted partition is
// re-scheduled through each bucket in turn before it is finally removed, so it
// stays tracked for the total of all bucket durations — not just the largest
// one. Using only the max would under-estimate residence for multi-bucket
// configs, under-sample, and lean on hard eviction. Returns 0 when no buckets
// are configured.
func residenceSeconds(buckets []time.Duration) float64 {
	var total time.Duration
	for _, b := range buckets {
		total += b
	}
	return total.Seconds()
}

// sampleRate returns the probability in (0, 1] with which a freshly-deleted
// partition should be admitted into the tracker. The goal is to keep the
// steady-state live count near maxHeapSize: over one residence window the
// tracker admits roughly admissionRate * residenceSeconds partitions, so
// admitting a maxHeapSize/(admissionRate*residenceSeconds) fraction leaves about
// maxHeapSize entries once partitions start aging out. residenceSeconds is the
// SUM of all bucket delays (a partition passes through every bucket).
//
// Example: at 2000 deletes/s, a single 1h bucket (3600s total residence) and a
// 1M cap, sampleRate = 1e6 / (2000*3600) ≈ 0.139 (≈14%).
//
// Returns 1.0 (admit everything) when sampling is disabled (no cap configured),
// when the admission rate is not yet known, or when the projected inflow is
// already within the cap.
func (d *deletedPartitions) sampleRate() float64 {
	if d.win.maxItems <= 0 || d.residenceSec <= 0 {
		return 1.0
	}

	rate := d.rate.perSec()
	if rate <= 0 {
		return 1.0
	}

	expected := rate * d.residenceSec
	if expected <= float64(d.win.maxItems) {
		return 1.0
	}

	return float64(d.win.maxItems) / expected
}

// admit reports whether a single partition should be kept given a sampling rate
// previously computed by sampleRate. A rate >= 1.0 always admits.
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

// Delete adds a deleted partition to the tracking system.
func (d *deletedPartitions) Delete(keys typedef.PartitionKeys) {
	// If no buckets configured, just count the deletion.
	if len(d.buckets) == 0 {
		d.deleted.Add(1)
		if keys.Release != nil {
			keys.Release()
		}
		return
	}

	now := time.Now()
	nowNs := now.UnixNano()

	// Measure the admission rate and sample before touching the window so that
	// under high DELETE throughput we never let the tracker balloon past its cap.
	d.rate.observe(1, nowNs)
	if rate := d.sampleRate(); !admit(rate) {
		d.dropSampled(keys)
		return
	}

	keys.DeletedAtNS = uint64(nowNs)
	readyAtNs := nowNs + int64(d.buckets[0]) + int64(jitterDuration(d.buckets[0]))

	// Defer release until the final pop; prevent double-release.
	onDone := keys.Release
	keys.Release = nil

	evicted := d.win.push(bucketEntry{onDone: onDone, keys: keys, readyAtNs: readyAtNs, bucket: 0})

	// Run onDone callbacks outside the window lock.
	d.runEvicted(evicted)

	d.deleted.Add(1)
}

// DeleteBulk adds multiple deleted partitions under a single window operation.
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
	nowNs := now.UnixNano()

	// Measure the admission rate over the whole batch, then sample per-key so
	// the kept fraction matches the configured cap under high throughput.
	d.rate.observe(int64(len(keys)), nowNs)
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

	readyAtNs := nowNs + int64(d.buckets[0]) + int64(jitterDuration(d.buckets[0]))
	deletedAtNS := uint64(nowNs)

	entries := make([]bucketEntry, len(keys))
	for i := range keys {
		keys[i].DeletedAtNS = deletedAtNS
		onDone := keys[i].Release
		keys[i].Release = nil
		entries[i] = bucketEntry{onDone: onDone, keys: keys[i], readyAtNs: readyAtNs, bucket: 0}
	}

	evicted := d.win.pushBulk(entries)
	d.runEvicted(evicted)

	d.deleted.Add(uint64(len(keys)))
}

// runEvicted invokes onDone callbacks for entries evicted by the hard cap and
// updates metrics. MUST be called WITHOUT any window lock held.
func (d *deletedPartitions) runEvicted(evicted []bucketEntry) {
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

// Close stops the background processor.
func (d *deletedPartitions) Close() {
	d.cancel()
}

// Len returns the number of partitions waiting for validation.
func (d *deletedPartitions) Len() int {
	return d.win.len()
}
