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
	"context"
	"math"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

func TestResidenceSeconds(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		buckets []time.Duration
		want    float64
	}{
		"empty":          {nil, 0},
		"single":         {[]time.Duration{time.Hour}, 3600},
		"sum_of_buckets": {[]time.Duration{time.Minute, 10 * time.Minute, time.Hour}, 60 + 600 + 3600},
		"order_agnostic": {[]time.Duration{time.Hour, time.Minute}, 3600 + 60},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			assert.InDelta(t, tc.want, residenceSeconds(tc.buckets), 1e-9)
		})
	}
}

// TestRateEstimatorConverges drives the estimator with a steady stream and
// checks the smoothed estimate converges to the true rate. Timestamps are
// injected so the test is fully deterministic.
func TestRateEstimatorConverges(t *testing.T) {
	t.Parallel()

	var e rateEstimator
	const ratePerSec = int64(2000)
	t0 := time.Now().UnixNano()

	// Feed one window's worth of events per second for 20 seconds.
	for k := range int64(20) {
		e.observe(ratePerSec, t0+k*int64(time.Second))
	}

	assert.InDelta(t, float64(ratePerSec), e.perSec(), float64(ratePerSec)*0.05,
		"EWMA should converge to within 5%% of the true admission rate")
}

func TestRateEstimatorNoDataYet(t *testing.T) {
	t.Parallel()

	var e rateEstimator
	assert.Zero(t, e.perSec(), "no observations should yield a zero rate")

	// A single anchoring observation does not roll the window, so the rate
	// remains unknown (0) until at least one full window elapses.
	e.observe(100, time.Now().UnixNano())
	assert.Zero(t, e.perSec())
}

// TestSampleRateMatchesSpecExample verifies the worked example from the issue:
// 2000 deletes/s, 1h residence window, 1M cap -> ~14% admission rate.
func TestSampleRate(t *testing.T) {
	t.Parallel()

	newWithRate := func(maxHeap int, residenceSec, ratePerSec float64) *deletedPartitions {
		d := &deletedPartitions{
			maxHeapSize:  maxHeap,
			residenceSec: residenceSec,
		}
		d.rate.ratePerSecBits.Store(math.Float64bits(ratePerSec))
		return d
	}

	t.Run("spec_example_2kops_1h_1M", func(t *testing.T) {
		t.Parallel()
		d := newWithRate(1_000_000, 3600, 2000)
		assert.InDelta(t, 0.13888, d.sampleRate(), 1e-4)
	})

	t.Run("disabled_when_no_cap", func(t *testing.T) {
		t.Parallel()
		d := newWithRate(-1, 3600, 2000)
		assert.Equal(t, 1.0, d.sampleRate())
	})

	t.Run("admit_all_when_rate_unknown", func(t *testing.T) {
		t.Parallel()
		d := newWithRate(1_000_000, 3600, 0)
		assert.Equal(t, 1.0, d.sampleRate())
	})

	t.Run("admit_all_when_inflow_within_cap", func(t *testing.T) {
		t.Parallel()
		// 10 deletes/s over 1h = 36k, well below a 1M cap.
		d := newWithRate(1_000_000, 3600, 10)
		assert.Equal(t, 1.0, d.sampleRate())
	})

	t.Run("admit_all_without_buckets", func(t *testing.T) {
		t.Parallel()
		d := newWithRate(1_000_000, 0, 2000)
		assert.Equal(t, 1.0, d.sampleRate())
	})
}

// TestAdmitDistribution checks that admit() keeps roughly the requested
// fraction of partitions over a large sample.
func TestAdmitDistribution(t *testing.T) {
	t.Parallel()

	const (
		n    = 200_000
		rate = 0.1388
	)

	kept := 0
	for range n {
		if admit(rate) {
			kept++
		}
	}

	got := float64(kept) / float64(n)
	assert.InDelta(t, rate, got, 0.01, "kept fraction should track the sample rate")
}

func TestAdmitAlwaysWhenRateGEOne(t *testing.T) {
	t.Parallel()

	for range 1000 {
		require.True(t, admit(1.0))
		require.True(t, admit(2.0))
	}
}

// TestDeleteBulkSamplingDropsAndReleases verifies that when the admission rate
// is set high enough to force sampling, DeleteBulk drops a portion of the
// batch, releases the dropped keys, and never pushes more than it admitted.
func TestDeleteBulkSamplingDropsAndReleases(t *testing.T) {
	t.Parallel()

	// start=false: no background drain, so the heap reflects exactly what was
	// admitted.
	d := newDeleted(t.Context(), []time.Duration{time.Hour}, 1_000_000, false)
	t.Cleanup(d.Close)

	// Force an aggressive sample rate: 1e7 deletes/s over a 1h window vastly
	// exceeds the 1M cap, so sampleRate is tiny.
	d.rate.ratePerSecBits.Store(math.Float64bits(1e7))

	const batch = 50_000
	releaseCount := 0
	keys := make([]typedef.PartitionKeys, batch)
	for i := range keys {
		keys[i] = typedef.PartitionKeys{
			Values:  typedef.NewValues(1),
			Release: func() { releaseCount++ },
		}
	}

	d.DeleteBulk(keys)

	admitted := d.Len()
	dropped := int(d.sampledOut.Load())

	assert.Positive(t, dropped, "sampling should drop a large fraction of the batch")
	assert.Equal(t, batch, admitted+dropped, "every key is either admitted or dropped")
	assert.Equal(t, dropped, releaseCount, "every dropped key must be released exactly once")
	assert.Less(t, admitted, batch/10, "admitted count should be a small fraction at this rate")
}

// TestSamplingBoundsHeapUnderSustainedOverloadIntegration is an end-to-end test
// that runs the real tracker (background drain goroutine included) under a
// sustained DELETE stream ~10x above what the heap cap can hold over one
// residence window. It verifies that, once the live admission-rate estimate has
// warmed up, the *admission sampler* — not the hard-cap eviction fallback — is
// what keeps the heap near its configured size. That is the property the issue
// asks for: bound memory by sampling on the way in, not by evicting after the
// fact.
//
// It runs inside a testing/synctest bubble so the time package uses a fake
// clock that only advances when every goroutine is durably blocked. The whole
// scenario — warmup, measurement window, and the per-tick producer/drain timers
// — therefore plays out deterministically and instantly, with no real
// wall-clock sleeps and no scheduler-timing flakiness. (The only non-determinism
// left is the random admit() draw, which the large sample size averages out.)
func TestSamplingBoundsHeapUnderSustainedOverloadIntegration(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const (
			residence   = 500 * time.Millisecond // single bucket -> entries age out after ~500ms
			heapCap     = 2000                   // heap cap
			tick        = 5 * time.Millisecond
			batchSize   = 200                     // 200 every 5ms => 40_000 deletes/s
			warmup      = 1500 * time.Millisecond // let the rate EWMA settle
			steps       = 15                      // measurement samples
			sampleEvery = 100 * time.Millisecond  // fake time between heap-length samples
		)
		// Sustained insert rate (~40k/s) is ~10x the heap's capacity over one
		// residence window (cap/residence = 2000/0.5s = 4000/s), so the
		// steady-state admission rate must settle near 10%.

		ctx, cancel := context.WithCancel(t.Context())
		d := newDeleted(ctx, []time.Duration{residence}, heapCap)

		// Drain consumer: keep the channel flowing so the background processor
		// can emit ready cohorts and age entries out of the heap. Exits when the
		// tracker is closed and d.ch is closed.
		consumerDone := make(chan struct{})
		go func() {
			defer close(consumerDone)
			//nolint:revive // intentional empty body: drain d.ch until it is closed
			for range d.ch {
			}
		}()

		// Producer: hammer DeleteBulk at a fixed (fake-clock) rate until stopped.
		var total atomic.Uint64
		stop := make(chan struct{})
		producerDone := make(chan struct{})
		go func() {
			defer close(producerDone)
			ticker := time.NewTicker(tick)
			defer ticker.Stop()
			for {
				select {
				case <-stop:
					return
				case <-ticker.C:
					batch := make([]typedef.PartitionKeys, batchSize)
					for i := range batch {
						batch[i] = typedef.PartitionKeys{Values: typedef.NewValues(1)}
					}
					d.DeleteBulk(batch)
					total.Add(batchSize)
				}
			}
		}()

		// Let the admission-rate estimate warm up before measuring. synctest.Wait
		// then blocks until the producer and drain goroutines have caught up to
		// the current fake time, giving a clean, settled snapshot.
		time.Sleep(warmup)
		synctest.Wait()

		// Snapshot counters at the start of the steady-state window. After
		// synctest.Wait() the producer is durably blocked, so these are stable.
		totalStart := total.Load()
		sampledStart := d.sampledOut.Load()
		evictedStart := d.evicted.Load()

		// Sample heap length across the steady-state window.
		lens := make([]int, 0, steps)
		for range steps {
			time.Sleep(sampleEvery)
			synctest.Wait()
			lens = append(lens, d.Len())
		}

		steadyInserted := total.Load() - totalStart
		sampledDelta := d.sampledOut.Load() - sampledStart
		evictedDelta := d.evicted.Load() - evictedStart

		// Tear down all bubble goroutines before the root returns: stop the
		// producer, then close the tracker (which closes d.ch and ends the
		// consumer). synctest requires every goroutine to have exited.
		close(stop)
		<-producerDone
		cancel()
		<-consumerDone

		// Mean steady-state heap length.
		require.NotEmpty(t, lens)
		sum := 0
		maxLen := 0
		for _, l := range lens {
			sum += l
			if l > maxLen {
				maxLen = l
			}
		}
		meanLen := float64(sum) / float64(len(lens))

		t.Logf("total inserted=%d steadyInserted=%d sampledOut=%d evicted=%d | steady: ΔsampledOut=%d Δevicted=%d meanHeap=%.0f maxHeap=%d cap=%d",
			total.Load(), steadyInserted, d.sampledOut.Load(), d.evicted.Load(),
			sampledDelta, evictedDelta, meanLen, maxLen, heapCap)

		// 1. In steady state the admission sampler drops the large majority of
		//    incoming deletes (~90% at 10x overload).
		require.Positive(t, steadyInserted)
		assert.Greater(t, float64(sampledDelta), float64(steadyInserted)*0.7,
			"the admission sampler should drop most deletes under 10x overload")

		// 2. In steady state, admission sampling is the dominant drop mechanism —
		//    far more deletes are rejected at the door than evicted after landing.
		//    This is the core claim: memory is bounded on the way IN, not by the
		//    hard-cap eviction fallback.
		assert.Greater(t, sampledDelta, evictedDelta,
			"sampling should drop more than eviction in steady state")
		assert.Less(t, evictedDelta, sampledDelta/4,
			"eviction should be a small minority of drops once the rate estimate has settled")

		// 3. The heap stabilizes near the configured cap (not far below, not
		//    pinned at the hard cap by constant eviction).
		assert.InDelta(t, float64(heapCap), meanLen, float64(heapCap)*0.9,
			"steady-state heap length should hover near the cap")
	})
}
