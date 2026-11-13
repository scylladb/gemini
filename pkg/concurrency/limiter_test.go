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

package concurrency

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func observeInterval(l *Limiter, latency time.Duration) {
	for range minSamples {
		l.Observe(latency)
	}
	l.adjust()
}

func TestLimiter_StartsWithinBounds(t *testing.T) {
	t.Parallel()

	require.Equal(t, 8, New(8, 16, nil).Limit())
	require.Equal(t, 16, New(40, 16, nil).Limit(), "the start never exceeds the maximum")
	require.Equal(t, 1, New(0, 16, nil).Limit(), "the start is at least one worker")
}

func TestLimiter_GrowsWhileLatencyIsFlat(t *testing.T) {
	t.Parallel()

	l := New(10, 100, nil)
	for range 5 {
		observeInterval(l, time.Millisecond)
	}

	require.Greater(t, l.Limit(), 10)
}

func TestLimiter_StopsAtTheMaximum(t *testing.T) {
	t.Parallel()

	l := New(10, 12, nil)
	for range 20 {
		observeInterval(l, time.Millisecond)
	}

	require.Equal(t, 12, l.Limit())
}

func TestLimiter_ShrinksWhenLatencyQueues(t *testing.T) {
	t.Parallel()

	l := New(50, 100, nil)
	observeInterval(l, time.Millisecond)
	grown := l.Limit()

	observeInterval(l, 5*time.Millisecond)
	require.Less(t, l.Limit(), grown, "5x the baseline means requests queue")
}

func TestLimiter_HoldsInsideTheBand(t *testing.T) {
	t.Parallel()

	l := New(50, 100, nil)
	observeInterval(l, time.Millisecond)
	held := l.Limit()

	observeInterval(l, 3*time.Millisecond)
	require.Equal(t, held, l.Limit(), "3x the baseline is inside the band")
}

func TestLimiter_IgnoresIntervalsWithFewSamples(t *testing.T) {
	t.Parallel()

	l := New(10, 100, nil)
	l.Observe(time.Millisecond)
	l.adjust()

	require.Equal(t, 10, l.Limit())
}

func TestLimiter_ReportsChanges(t *testing.T) {
	t.Parallel()

	var last atomic.Int64
	l := New(10, 100, func(limit int) { last.Store(int64(limit)) })
	require.EqualValues(t, 10, last.Load())

	observeInterval(l, time.Millisecond)
	require.EqualValues(t, l.Limit(), last.Load())
}

func TestLimiter_WaitParksUntilTheLimitGrows(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		l := New(2, 10, nil)
		require.NoError(t, l.Wait(t.Context(), 1, func() bool { return false }), "index 1 is below the limit")

		var released atomic.Bool
		go func() {
			_ = l.Wait(t.Context(), 2, func() bool { return false })
			released.Store(true)
		}()

		synctest.Wait()
		require.False(t, released.Load(), "index 2 parks at limit 2")

		observeInterval(l, time.Millisecond)
		synctest.Wait()
		require.True(t, released.Load(), "a larger limit releases the worker")
	})
}

func TestLimiter_WaitReturnsOnStopAndCancel(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		l := New(1, 10, nil)

		var stop atomic.Bool
		stopped := make(chan error, 1)
		go func() { stopped <- l.Wait(t.Context(), 5, stop.Load) }()

		stop.Store(true)
		time.Sleep(parkCheck)
		require.NoError(t, <-stopped, "a stopped worker leaves without an error")

		ctx, cancel := context.WithCancel(t.Context())
		canceled := make(chan error, 1)
		go func() { canceled <- l.Wait(ctx, 5, func() bool { return false }) }()

		cancel()
		require.ErrorIs(t, <-canceled, context.Canceled)
	})
}
