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

package jobs

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/concurrency"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
)

// TestParkUntilAllowed_RunsDueRetriesWhileParked pins that a worker that the
// limiter parks still runs its due retries. A worker that waits in the limiter
// before it looks at the retry queue leaves its retries, and their partition
// keys, waiting until the limit grows.
func TestParkUntilAllowed_RunsDueRetriesWhileParked(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		var checks atomic.Int32
		v := &Validation{
			table: &typedef.Table{Name: "parked"},
			store: &mockStore{
				checkFunc: func(context.Context, *typedef.Table, *typedef.Stmt, int) (int, store.Rows, error) {
					checks.Add(1)
					return 1, nil, nil
				},
			},
			status:      status.NewGlobalStatus(10),
			stopFlag:    stop.NewFlag("parked"),
			maxAttempts: 3,
			// The limit is 1, so the worker with index 1 is parked.
			limiter:   concurrency.New(1, 2, nil),
			workerIdx: 1,
		}

		rq := newRetryQueue(3, time.Second, 100*time.Millisecond)
		require.Equal(t, scheduleAccepted, rq.Schedule(&typedef.Stmt{}, 0, attemptHistory{}))

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() {
			done <- v.parkUntilAllowed(ctx, rq, prometheus.NewCounter(prometheus.CounterOpts{Name: "t"}))
		}()

		time.Sleep(time.Second)
		synctest.Wait()

		require.Equal(t, int32(1), checks.Load(), "the due retry runs while the worker is parked")
		require.Zero(t, rq.Len())
		select {
		case err := <-done:
			t.Fatalf("parkUntilAllowed returned while the worker is parked: %v", err)
		default:
		}

		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
	})
}
