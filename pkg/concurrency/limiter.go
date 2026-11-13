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

// Package concurrency sets how many of gemini's workers run at a time.
//
// Every worker pool starts its maximum number of workers. A worker whose index
// is at or above the current limit parks until the limit grows. The limit
// follows operation latency: a worker count past what the clusters can absorb
// only queues requests, so latency rises long before throughput falls.
package concurrency

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	// growRatio and shrinkRatio bound the latency, as a multiple of the
	// baseline, that the limit aims for. Below growRatio the clusters have
	// headroom, so the limit grows. Above shrinkRatio requests queue so much
	// that more workers no longer add throughput, so the limit shrinks. The
	// band is wide on purpose: throughput comes first, and latency between
	// the two ratios still buys throughput.
	growRatio   = 2.5
	shrinkRatio = 4.0
	// growStep is the share the limit grows by, at least one worker.
	growStep = 0.10
	// shrinkFactor is what the limit keeps when it shrinks.
	shrinkFactor = 0.8
	// baselineWindows is how many intervals the baseline looks back. The
	// baseline is the lowest mean latency of those intervals. It covers two
	// minutes, longer than a typical slowdown of a node, so the limit stays
	// low while the clusters are degraded instead of treating the degraded
	// latency as the new normal.
	baselineWindows = 120
	// minSamples is the fewest operations an interval needs to count.
	minSamples = 10
	// parkCheck is how often a parked worker re-checks whether it must stop.
	parkCheck = 100 * time.Millisecond
)

// Limiter holds the current worker limit and adjusts it from latency.
type Limiter struct {
	changed  atomic.Pointer[chan struct{}]
	onChange func(limit int)
	recent   []float64
	limit    atomic.Int64
	sumNs    atomic.Int64
	count    atomic.Int64
	minimum  int
	maximum  int
	next     int
	mu       sync.Mutex
}

// New returns a limiter that starts at initial and stays within [1, maximum].
// onChange, when not nil, receives every new limit, for example to export it as
// a metric.
func New(initial, maximum int, onChange func(limit int)) *Limiter {
	maximum = max(maximum, 1)
	l := &Limiter{
		minimum:  1,
		maximum:  maximum,
		recent:   make([]float64, 0, baselineWindows),
		onChange: onChange,
	}
	ch := make(chan struct{})
	l.changed.Store(&ch)
	l.setLimit(min(max(initial, 1), maximum))

	return l
}

// Limit returns the current number of workers that may run per pool.
func (l *Limiter) Limit() int {
	return int(l.limit.Load())
}

// Wait returns nil when the worker with index idx may run its next operation.
// It parks while idx is at or above the limit. It returns ctx.Err() when ctx
// ends, and nil when stopped reports true, so the caller's loop sees the stop.
func (l *Limiter) Wait(ctx context.Context, idx int, stopped func() bool) error {
	if int64(idx) < l.limit.Load() {
		return nil
	}

	ticker := time.NewTicker(parkCheck)
	defer ticker.Stop()

	for {
		// Load the channel before the limit. A limit that grows after this
		// load closes this channel, so the select below cannot miss it.
		ch := *l.changed.Load()
		if int64(idx) < l.limit.Load() || stopped() {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		case <-ticker.C:
		}
	}
}

// Observe records the latency of one operation.
func (l *Limiter) Observe(d time.Duration) {
	l.sumNs.Add(int64(d))
	l.count.Add(1)
}

// Run adjusts the limit every interval until ctx ends.
func (l *Limiter) Run(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			l.adjust()
		}
	}
}

// adjust takes the mean latency of the interval and moves the limit toward
// the latency band [growRatio, shrinkRatio] times the baseline.
func (l *Limiter) adjust() {
	count := l.count.Swap(0)
	sum := l.sumNs.Swap(0)
	if count < minSamples {
		return
	}

	mean := float64(sum) / float64(count)

	l.mu.Lock()
	defer l.mu.Unlock()

	if len(l.recent) < baselineWindows {
		l.recent = append(l.recent, mean)
	} else {
		l.recent[l.next] = mean
		l.next = (l.next + 1) % baselineWindows
	}

	baseline := math.Inf(1)
	for _, v := range l.recent {
		baseline = min(baseline, v)
	}

	limit := l.Limit()
	switch ratio := mean / baseline; {
	case ratio < growRatio:
		limit += max(1, int(float64(limit)*growStep))
	case ratio > shrinkRatio:
		limit = int(float64(limit) * shrinkFactor)
	default:
		return
	}

	l.setLimit(min(max(limit, l.minimum), l.maximum))
}

func (l *Limiter) setLimit(limit int) {
	old := l.limit.Swap(int64(limit))
	if old == int64(limit) {
		return
	}

	if int64(limit) > old {
		ch := make(chan struct{})
		close(*l.changed.Swap(&ch))
	}

	if l.onChange != nil {
		l.onChange(limit)
	}
}
