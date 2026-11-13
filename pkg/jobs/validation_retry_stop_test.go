// Copyright 2026 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package jobs

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
)

func newRetryStopValidation(flag *stop.Flag, checks *atomic.Int32) *Validation {
	return &Validation{
		table: &typedef.Table{Name: "retry_stop"},
		store: &mockStore{
			checkFunc: func(context.Context, *typedef.Table, *typedef.Stmt, int) (int, store.Rows, error) {
				checks.Add(1)
				return 1, nil, nil
			},
		},
		status:      status.NewGlobalStatus(10),
		stopFlag:    flag,
		maxAttempts: 3,
	}
}

func newQueuedRetries(t *testing.T, n int) *retryQueue {
	t.Helper()

	rq := newRetryQueue(3, 20*time.Millisecond, 10*time.Millisecond)
	for range n {
		require.Equal(t, scheduleAccepted, rq.Schedule(&typedef.Stmt{}, 0))
	}

	return rq
}

func TestFinishPendingRetries_RunsQueuedRetriesAfterSoftStop(t *testing.T) {
	t.Parallel()

	flag := stop.NewFlag(t.Name())
	flag.SetSoft(false)

	var checks atomic.Int32
	v := newRetryStopValidation(flag, &checks)
	rq := newQueuedRetries(t, 2)

	err := v.finishPendingRetries(t.Context(), rq, prometheus.NewCounter(prometheus.CounterOpts{Name: "t"}))

	require.NoError(t, err)
	require.Equal(t, int32(2), checks.Load(), "every queued retry must run after a soft stop")
	require.Zero(t, rq.Len())
}

func TestFinishPendingRetries_HardStopLeavesRetries(t *testing.T) {
	t.Parallel()

	flag := stop.NewFlag(t.Name())
	flag.SetHard(false)

	var checks atomic.Int32
	v := newRetryStopValidation(flag, &checks)
	rq := newQueuedRetries(t, 2)
	t.Cleanup(func() { rq.Drain(releaseKeys) })

	err := v.finishPendingRetries(t.Context(), rq, prometheus.NewCounter(prometheus.CounterOpts{Name: "t"}))

	require.NoError(t, err)
	require.Zero(t, checks.Load(), "a hard stop must not run more checks")
	require.Equal(t, 2, rq.Len(), "the drain in Do counts what is left")
}

// TestCheckOnce_RetriesAnAttemptDeadlineWhileContextLives returns a deadline
// error from one attempt while the worker's context is alive. That is a failed
// attempt, not the end of the worker, so it must be retried.
func TestCheckOnce_RetriesAnAttemptDeadlineWhileContextLives(t *testing.T) {
	t.Parallel()

	v := &Validation{
		table: &typedef.Table{Name: "attempt_deadline"},
		store: &mockStore{
			checkFunc: func(context.Context, *typedef.Table, *typedef.Stmt, int) (int, store.Rows, error) {
				return 0, nil, context.DeadlineExceeded
			},
		},
		status: status.NewGlobalStatus(10),
	}

	result := v.checkOnce(t.Context(), &typedef.Stmt{}, 0, prometheus.NewCounter(prometheus.CounterOpts{Name: "t"}))
	require.True(t, result.needsRetry)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	result = v.checkOnce(ctx, &typedef.Stmt{}, 0, prometheus.NewCounter(prometheus.CounterOpts{Name: "t"}))
	require.False(t, result.needsRetry, "an ended worker context stops the retries")
}
