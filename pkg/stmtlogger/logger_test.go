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

package stmtlogger

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

type dummyCloser struct {
	err    error
	calls  int
	mu     sync.Mutex
	closed bool
}

func (d *dummyCloser) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	d.closed = true
	return d.err
}

func TestLogger_LogStmt_FiltersTypes(t *testing.T) {
	t.Parallel()

	ch := make(chan Item, 16)
	l, err := NewLogger(WithChannel(ch), WithLogger(&dummyCloser{}, nil))
	require.NoError(t, err)

	// Select should be ignored
	_ = l.LogStmt(Item{StatementType: typedef.SelectStatementType})
	// Schema change (Alter) should be ignored
	_ = l.LogStmt(Item{StatementType: typedef.AlterColumnStatementType})
	// Non-select mutation should be logged
	want := Item{StatementType: typedef.InsertStatementType}
	_ = l.LogStmt(want)

	select {
	case got := <-ch:
		require.Equal(t, want.StatementType, got.StatementType)
	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for enqueued item")
	}

	require.NoError(t, l.Close())
}

func TestLogger_LogStmt_NoCloser_NoSend(t *testing.T) {
	t.Parallel()

	ch := make(chan Item, 1)
	l, err := NewLogger(WithChannel(ch)) // no closer
	require.NoError(t, err)

	// Even for insert, logger with nil closer should not enqueue
	_ = l.LogStmt(Item{StatementType: typedef.InsertStatementType})

	// Ensure channel still empty
	select {
	case <-ch:
		t.Fatal("unexpected item enqueued when closer is nil")
	default:
	}

	// Close is a no-op without closer
	require.NoError(t, l.Close())
}

func TestLogger_Close_IdempotentAndClosesInner(t *testing.T) {
	t.Parallel()

	dc := &dummyCloser{}
	l, err := NewLogger(WithLogger(dc, nil))
	require.NoError(t, err)

	// Close multiple times — must not panic and inner closer called once.
	require.NoError(t, l.Close())
	require.NoError(t, l.Close())

	dc.mu.Lock()
	calls := dc.calls
	dc.mu.Unlock()
	require.Equal(t, 1, calls)
}

func TestWithLogger_ErrorPropagates(t *testing.T) {
	t.Parallel()

	expected := errors.New("boom")
	_, err := NewLogger(WithLogger(nil, expected))
	require.ErrorIs(t, err, expected)
}

func TestWithChannel_MultipleOptions_CloseFirstChannel(t *testing.T) {
	t.Parallel()

	ch1 := make(chan Item, 1)
	ch2 := make(chan Item, 1)

	l, err := NewLogger(WithChannel(ch1), WithChannel(ch2), WithLogger(&dummyCloser{}, nil))
	require.NoError(t, err)

	// ch1 must be closed by the second WithChannel option inside NewLogger
	select {
	case _, ok := <-ch1:
		require.False(t, ok, "first channel is expected to be closed")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting to observe closed first channel")
	}

	// Ensure the logger uses ch2
	want := Item{StatementType: typedef.InsertStatementType}
	_ = l.LogStmt(want)
	select {
	case got := <-ch2:
		require.Equal(t, want.StatementType, got.StatementType)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for item in second channel")
	}

	require.NoError(t, l.Close())
}

func TestLogger_ConcurrentSendAndClose_NoDrop_NoPanic(t *testing.T) {
	t.Parallel()

	// Use a buffered channel to avoid sender blocking due to capacity
	ch := make(chan Item, 4096)
	dc := &dummyCloser{}
	l, err := NewLogger(WithChannel(ch), WithLogger(dc, nil))
	require.NoError(t, err)

	const producers = 16
	const perProducer = 200

	var wg sync.WaitGroup
	wg.Add(producers)

	start := make(chan struct{})
	for i := 0; i < producers; i++ {
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < perProducer; j++ {
				_ = l.LogStmt(Item{StatementType: typedef.InsertStatementType})
			}
		}()
	}

	// Start producers roughly at the same time
	close(start)

	// Give them a brief time slice to enqueue a lot
	time.Sleep(10 * time.Millisecond)

	// Close after producers finished to assert no drops
	wg.Wait()
	require.NoError(t, l.Close())

	// Drain channel and count items
	got := 0
	for range ch {
		got++
	}

	require.Equal(t, producers*perProducer, got)
	dc.mu.Lock()
	require.True(t, dc.closed)
	dc.mu.Unlock()
}

func TestLogger_BlocksUntilReceiverReads(t *testing.T) {
	t.Parallel()

	// Unbuffered channel — send blocks until a receiver is ready.
	ch := make(chan Item)
	l, err := NewLogger(WithChannel(ch), WithLogger(&dummyCloser{}, nil))
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		_ = l.LogStmt(Item{StatementType: typedef.InsertStatementType})
		close(done)
	}()

	// Read the item — this unblocks the sender.
	select {
	case got := <-ch:
		require.Equal(t, typedef.InsertStatementType, got.StatementType)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for item")
	}

	// Sender should now complete.
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("LogStmt did not return after receiver read the item")
	}

	require.NoError(t, l.Close())
}

// TestLogger_BackPressure_BlocksAndNeverDrops verifies the never-drop contract:
// once the bounded channel AND the overflow ring are saturated, LogStmt blocks
// the producer (rather than dropping), and every item is delivered intact once
// the consumer catches up.
//
// It injects a tiny per-ring overflow cap so the back-pressure path is reached
// with a handful of items instead of 131072.
func TestLogger_BackPressure_BlocksAndNeverDrops(t *testing.T) {
	t.Parallel()

	ch := make(chan Item, 2) // small channel, no reader yet
	dc := &dummyCloser{}
	l, err := NewLogger(WithChannel(ch), WithLogger(dc, nil))
	require.NoError(t, err)

	// Shrink this logger's overflow ring so it saturates quickly. Set under
	// overflowMu since the drainer goroutine reads the ring concurrently.
	l.overflowMu.Lock()
	l.overflow.maxItems = 4
	l.overflowMu.Unlock()

	const total = 50
	sent := make(chan struct{})
	go func() {
		for range total {
			_ = l.LogStmt(Item{StatementType: typedef.InsertStatementType})
		}
		close(sent)
	}()

	// Effective capacity is channel(2) + overflow(4) + ~1 in-flight in the
	// drainer ≈ 7, far below total — so the producer MUST block.
	select {
	case <-sent:
		t.Fatal("LogStmt did not block under back-pressure (items were dropped or buffer unbounded)")
	case <-time.After(150 * time.Millisecond):
		// expected: producer is blocked waiting for drain
	}

	// Drain the channel; the producer must unblock and deliver every item.
	got := 0
	deadline := time.After(5 * time.Second)
	for got < total {
		select {
		case <-ch:
			got++
		case <-deadline:
			t.Fatalf("only received %d/%d items — items were dropped", got, total)
		}
	}
	require.Equal(t, total, got)

	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("producer did not finish after the consumer caught up")
	}

	require.NoError(t, l.Close())
}

// TestLogger_LogStmtRacesClose_NoPanic hammers LogStmt from many goroutines
// while Close runs concurrently. It is the regression guard for the
// send-on-closed-channel panic: producers must serialize against close(l.ch)
// via sendMu, so this must never panic regardless of interleaving. Run it with
// -race and a high count.
func TestLogger_LogStmtRacesClose_NoPanic(t *testing.T) {
	t.Parallel()

	ch := make(chan Item, 8)
	l, err := NewLogger(WithChannel(ch), WithLogger(&dummyCloser{}, nil))
	require.NoError(t, err)

	// Drain the channel concurrently so producers keep moving (and so the
	// committer-side close ordering is exercised).
	drained := make(chan struct{})
	go func() {
		got := 0
		for range ch {
			got++
		}
		_ = got
		close(drained)
	}()

	const producers = 32
	var wg sync.WaitGroup
	wg.Add(producers)
	for range producers {
		go func() {
			defer wg.Done()
			for range 500 {
				_ = l.LogStmt(Item{StatementType: typedef.InsertStatementType})
			}
		}()
	}

	// Close while producers are still firing — the dangerous interleaving.
	time.Sleep(time.Millisecond)
	require.NoError(t, l.Close())

	wg.Wait()
	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("channel was not closed/drained after Close")
	}
}

func TestLogger_LogAfterClose_NoSend(t *testing.T) {
	t.Parallel()

	ch := make(chan Item, 1)
	l, err := NewLogger(WithChannel(ch), WithLogger(&dummyCloser{}, nil))
	require.NoError(t, err)
	require.NoError(t, l.Close())

	// Attempt to log after close must not enqueue
	_ = l.LogStmt(Item{StatementType: typedef.InsertStatementType})

	select {
	case _, ok := <-ch:
		// Channel is closed, drain until closed
		if ok {
			t.Fatal("unexpected item after close")
		}
	default:
		// If not closed yet, ensure no items and then ensure closure by closing explicitly
		// since Close already called, channel should be closed, but buffered read may be empty.
		// Give it a brief moment and verify closure by non-blocking check again.
		time.Sleep(10 * time.Millisecond)
		select {
		case _, ok := <-ch:
			require.False(t, ok)
		default:
			t.Fatal("expected channel to be closed after Close")
		}
	}
}

func TestItem_MemoryFootprint_Sane(t *testing.T) {
	t.Parallel()

	it := Item{}
	base := it.MemoryFootprint()
	require.NotZero(t, base)

	it2 := Item{Statement: "INSERT INTO t (a) VALUES (?)", Host: "127.0.0.1"}
	bigger := it2.MemoryFootprint()
	require.Greater(t, bigger, base)
}
