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
	"time"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

// maxPendingRetries caps how many statements a single validation worker's
// retryQueue holds before backpressure kicks in. Without a cap, a sustained
// cluster fault causes every new SELECT to fail and schedule another retry,
// pinning an unbounded number of partition keys and delaying soft-stop until
// the whole backlog drains.
const maxPendingRetries = 1024

// pendingRetry holds a validation statement waiting for its backoff timer to fire.
type pendingRetry struct {
	stmt    *typedef.Stmt
	timer   *time.Timer
	attempt int
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

// scheduleOutcome distinguishes why Schedule did or did not enqueue a retry, so
// the caller can tell a genuine attempt-exhaustion (a validation failure) apart
// from a transient backpressure drop (NOT a failure).
type scheduleOutcome int

const (
	// scheduleAccepted: the retry was enqueued; the queue now owns the stmt.
	scheduleAccepted scheduleOutcome = iota
	// scheduleExhausted: all retry attempts are used up — treat as final failure.
	scheduleExhausted
	// scheduleSaturated: the queue is at its pending cap — the retry was dropped
	// for backpressure; the caller must release the stmt but must NOT report a
	// validation failure.
	scheduleSaturated
)

// Schedule adds a failed statement to the retry queue with a backoff timer.
// It returns scheduleExhausted when the statement has used all its retry
// attempts, scheduleSaturated when the queue has reached its pending-retry cap
// (see maxPendingRetries), and scheduleAccepted when the retry was enqueued.
func (q *retryQueue) Schedule(stmt *typedef.Stmt, attempt int) scheduleOutcome {
	if attempt+1 >= q.maxAttempts {
		return scheduleExhausted
	}
	if len(q.items) >= maxPendingRetries {
		return scheduleSaturated
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
	return scheduleAccepted
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

// Saturated reports whether the queue has reached its pending-retry cap.
// Callers should stop generating new work until it drains.
func (q *retryQueue) Saturated() bool {
	return q.Len() >= maxPendingRetries
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
