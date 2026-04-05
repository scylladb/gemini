// Copyright 2025 ScyllaDB
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
	"time"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

// pendingRetry holds a validation statement waiting for its backoff timer to fire.
type pendingRetry struct {
	stmt    *typedef.Stmt
	attempt int
	timer   *time.Timer
}

// retryQueue manages pending validation retries with backoff timers.
// It is used by a single goroutine (the validation worker) and is NOT
// safe for concurrent use.
type retryQueue struct {
	items       []pendingRetry
	maxAttempts int
	maxDelay    time.Duration
	minDelay    time.Duration
}

func newRetryQueue(maxAttempts int, maxDelay, minDelay time.Duration) *retryQueue {
	return &retryQueue{
		items:       make([]pendingRetry, 0, 8),
		maxAttempts: maxAttempts,
		maxDelay:    maxDelay,
		minDelay:    minDelay,
	}
}

// Schedule adds a failed statement to the retry queue with a backoff timer.
// Returns false if the statement has exhausted all retry attempts.
func (q *retryQueue) Schedule(stmt *typedef.Stmt, attempt int) bool {
	if attempt+1 >= q.maxAttempts {
		return false
	}

	delay := utils.Backoff(utils.ExponentialBackoffStrategy, attempt, q.maxDelay, q.minDelay)
	timer := utils.GetTimer(delay)

	q.items = append(q.items, pendingRetry{
		stmt:    stmt,
		attempt: attempt + 1,
		timer:   timer,
	})

	metrics.ValidationRetriesScheduled.Inc()
	metrics.ValidationRetriesPending.Set(float64(len(q.items)))
	return true
}

// Ready returns the index of the first retry whose timer has fired, or -1.
// This is a non-blocking check.
func (q *retryQueue) Ready() int {
	for i := range q.items {
		select {
		case <-q.items[i].timer.C:
			return i
		default:
		}
	}
	return -1
}

// Take removes and returns the retry at the given index.
func (q *retryQueue) Take(idx int) pendingRetry {
	item := q.items[idx]
	// Put back the timer (already fired, channel drained by Ready)
	utils.PutTimer(item.timer)
	item.timer = nil

	// Swap-remove for O(1)
	last := len(q.items) - 1
	q.items[idx] = q.items[last]
	q.items[last] = pendingRetry{} // zero out for GC
	q.items = q.items[:last]
	metrics.ValidationRetriesPending.Set(float64(len(q.items)))

	return item
}

// Len returns the number of pending retries.
func (q *retryQueue) Len() int {
	return len(q.items)
}

// Drain cancels all pending timers and releases all statements using the
// provided release function. Called during shutdown.
func (q *retryQueue) Drain(release func(*typedef.Stmt)) {
	for i := range q.items {
		utils.PutTimer(q.items[i].timer)
		if release != nil {
			release(q.items[i].stmt)
		}
		q.items[i] = pendingRetry{}
	}
	q.items = q.items[:0]
}

// EarliestDeadline returns the earliest timer channel among all pending
// retries, or nil if the queue is empty. This can be used in a select
// statement to wait for the next retry without busy-polling.
func (q *retryQueue) EarliestTimer() <-chan time.Time {
	if len(q.items) == 0 {
		return nil
	}

	// Return the first item's timer. Since timers fire in roughly the order
	// they were scheduled (FIFO with exponential delays), the first item
	// is usually the earliest.
	return q.items[0].timer.C
}

// TakeFirst removes and returns the first retry (index 0).
// Caller must ensure the queue is non-empty.
func (q *retryQueue) TakeFirst() pendingRetry {
	return q.Take(0)
}
