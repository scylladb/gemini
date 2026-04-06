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
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

func makeStmt(query string) *typedef.Stmt {
	return &typedef.Stmt{
		Query:     query,
		QueryType: typedef.SelectStatementType,
	}
}

func TestNewRetryQueue(t *testing.T) {
	t.Parallel()

	q := newRetryQueue(5, time.Second, 10*time.Millisecond)
	require.NotNil(t, q)
	require.Equal(t, 5, q.maxAttempts)
	require.Equal(t, time.Second, q.maxDelay)
	require.Equal(t, 10*time.Millisecond, q.minDelay)
	require.Equal(t, 0, q.Len())
}

func TestRetryQueue_Len(t *testing.T) {
	t.Parallel()

	q := newRetryQueue(10, time.Second, time.Millisecond)
	require.Equal(t, 0, q.Len())

	stmt := makeStmt("SELECT 1")
	q.Schedule(stmt, 0)
	require.Equal(t, 1, q.Len())

	q.Schedule(makeStmt("SELECT 2"), 1)
	require.Equal(t, 2, q.Len())
}

func TestRetryQueue_Schedule_ReturnsFalseWhenExhausted(t *testing.T) {
	t.Parallel()

	maxAttempts := 3
	q := newRetryQueue(maxAttempts, time.Second, time.Millisecond)
	stmt := makeStmt("SELECT exhausted")

	// attempt + 1 >= maxAttempts → returns false
	ok := q.Schedule(stmt, maxAttempts-1) // attempt=2, 2+1=3 >= 3 → false
	require.False(t, ok)
	require.Equal(t, 0, q.Len(), "exhausted schedule should not enqueue")
}

func TestRetryQueue_Schedule_ReturnsTrueWhenNotExhausted(t *testing.T) {
	t.Parallel()

	q := newRetryQueue(5, time.Second, time.Millisecond)
	stmt := makeStmt("SELECT ok")

	ok := q.Schedule(stmt, 0)
	require.True(t, ok)
	require.Equal(t, 1, q.Len())
}

func TestRetryQueue_Ready_EmptyQueueReturnsMinusOne(t *testing.T) {
	t.Parallel()

	q := newRetryQueue(5, time.Second, time.Millisecond)
	require.Equal(t, -1, q.Ready())
}

func TestRetryQueue_Ready_NoFiredTimerReturnsMinusOne(t *testing.T) {
	t.Parallel()

	// Use a very long delay so the timer won't fire during the test.
	q := newRetryQueue(5, 10*time.Second, time.Second)
	q.Schedule(makeStmt("SELECT 1"), 0)
	q.Schedule(makeStmt("SELECT 2"), 1)

	// Timers have not fired — Ready should return -1.
	require.Equal(t, -1, q.Ready())

	// Drain to release timers.
	q.Drain(nil)
}

func TestRetryQueue_Ready_FiredTimerReturnsIndex(t *testing.T) {
	t.Parallel()

	// Use a zero delay so the timer fires immediately.
	q := newRetryQueue(5, 0, 0)
	q.Schedule(makeStmt("SELECT immediate"), 0)

	// Wait long enough for the timer to fire.
	time.Sleep(10 * time.Millisecond)

	idx := q.Ready()
	require.GreaterOrEqual(t, idx, 0, "Ready() should return >= 0 when a timer has fired")
}

func TestRetryQueue_Take(t *testing.T) {
	t.Parallel()

	q := newRetryQueue(5, 0, 0)
	stmt := makeStmt("SELECT take")
	q.Schedule(stmt, 0)

	// Wait for timer to fire.
	time.Sleep(10 * time.Millisecond)

	idx := q.Ready()
	require.GreaterOrEqual(t, idx, 0)

	item := q.Take(idx)
	require.Equal(t, stmt, item.stmt)
	require.Equal(t, 1, item.attempt) // attempt was 0, Schedule increments to 1
	require.Nil(t, item.timer)        // Take nils the timer
	require.Equal(t, 0, q.Len())
}

func TestRetryQueue_Take_SwapRemove(t *testing.T) {
	t.Parallel()

	// Schedule multiple items so we can verify swap-remove works.
	q := newRetryQueue(10, 0, 0)
	s1 := makeStmt("S1")
	s2 := makeStmt("S2")
	s3 := makeStmt("S3")

	q.Schedule(s1, 0)
	q.Schedule(s2, 1)
	q.Schedule(s3, 2)

	require.Equal(t, 3, q.Len())

	// Wait for all timers to fire.
	time.Sleep(20 * time.Millisecond)

	// Take item at index 0.
	item := q.Take(0)
	require.Equal(t, s1, item.stmt)
	require.Equal(t, 2, q.Len(), "after Take(0), len should be 2")
}

func TestRetryQueue_TakeFirst(t *testing.T) {
	t.Parallel()

	q := newRetryQueue(5, 0, 0)
	stmt := makeStmt("SELECT first")
	q.Schedule(stmt, 0)

	time.Sleep(10 * time.Millisecond)

	item := q.TakeFirst()
	require.Equal(t, stmt, item.stmt)
	require.Equal(t, 0, q.Len())
}

func TestRetryQueue_EarliestTimer_EmptyReturnsNil(t *testing.T) {
	t.Parallel()

	q := newRetryQueue(5, time.Second, time.Millisecond)
	require.Nil(t, q.EarliestTimer())
}

func TestRetryQueue_EarliestTimer_NonEmptyReturnsChannel(t *testing.T) {
	t.Parallel()

	q := newRetryQueue(5, 10*time.Second, time.Second)
	q.Schedule(makeStmt("SELECT et"), 0)

	ch := q.EarliestTimer()
	require.NotNil(t, ch)

	q.Drain(nil)
}

func TestRetryQueue_Drain_Empty(t *testing.T) {
	t.Parallel()

	q := newRetryQueue(5, time.Second, time.Millisecond)
	// Should not panic on empty queue.
	q.Drain(nil)
	require.Equal(t, 0, q.Len())
}

func TestRetryQueue_Drain_ReleasesStatements(t *testing.T) {
	t.Parallel()

	q := newRetryQueue(10, time.Second, 10*time.Millisecond)
	stmts := []*typedef.Stmt{
		makeStmt("S1"),
		makeStmt("S2"),
		makeStmt("S3"),
	}
	for _, s := range stmts {
		q.Schedule(s, 0)
	}
	require.Equal(t, 3, q.Len())

	released := make([]*typedef.Stmt, 0, 3)
	q.Drain(func(s *typedef.Stmt) {
		released = append(released, s)
	})

	require.Equal(t, 0, q.Len())
	require.Len(t, released, 3)
	for _, s := range stmts {
		require.Contains(t, released, s)
	}
}

func TestRetryQueue_Drain_NilRelease(t *testing.T) {
	t.Parallel()

	q := newRetryQueue(5, time.Second, time.Millisecond)
	q.Schedule(makeStmt("S1"), 0)
	q.Schedule(makeStmt("S2"), 2)

	// nil release function must not panic.
	q.Drain(nil)
	require.Equal(t, 0, q.Len())
}
