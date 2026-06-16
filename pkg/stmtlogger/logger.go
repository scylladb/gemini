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
	"io"
	"sync"
	"time"
	"unsafe"

	"github.com/samber/mo"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

const (
	defaultChanSize = 8192
)

const (
	TypeOracle Type = "oracle"
	TypeTest   Type = "test"
)

type (
	Type string

	Item struct {
		Start          Time                     `json:"s"`
		Error          mo.Either[error, string] `json:"e,omitempty"`
		Type           Type                     `json:"-"`
		Statement      string                   `json:"q"`
		Host           string                   `json:"h"`
		Values         mo.Either[[]any, []byte] `json:"v"`
		RecentSuccess  []uint64                 `json:"rs,omitempty"`
		PartitionKeys  typedef.PartitionKeys    `json:"partitionKeys"`
		Duration       Duration                 `json:"d"`
		FirstSuccessNS uint64                   `json:"fs,omitempty"`
		LastSuccessNS  uint64                   `json:"ls,omitempty"`
		LastFailureNS  uint64                   `json:"lf,omitempty"`
		Attempt        int                      `json:"d_a"`
		GeminiAttempt  int                      `json:"g_a"`
		StatementType  typedef.StatementType    `json:"-"`
	}

	Duration struct {
		Duration time.Duration
	}

	Time struct {
		Time time.Time
	}

	ValidationHuman struct {
		First   string   `json:"first,omitempty"`
		Last    string   `json:"last,omitempty"`
		Failure string   `json:"failure,omitempty"`
		Recent  []string `json:"recent,omitempty"`
	}

	// Logger is a statement logger that never drops items. When the bounded
	// channel is full, items spill into a bounded overflow ring buffer (see
	// overflow.go). A background drainer pump moves overflow items into the
	// channel as space opens. When the overflow ring is ALSO at its cap, LogStmt
	// BLOCKS (back-pressure) until the drainer frees a slot — it rate-limits the
	// producers (gocql observer callbacks, and thus the mutation/validation
	// workers) to the committer's drain rate rather than dropping anything or
	// growing memory without bound. This is safe from a permanent stall because
	// the committer always consumes from the channel (even a failed or
	// timed-out batch removes its items), so a ring slot always eventually
	// frees. On Close(), any remaining overflow items are forwarded to the
	// channel (bounded by flushTotalTimeout) and any producer blocked on a full
	// ring is woken via overflowCond and returns.
	Logger struct {
		closer       io.Closer
		logger       *zap.Logger
		ch           chan Item
		done         chan struct{}
		overflowSig  chan struct{}
		overflowCond *sync.Cond
		overflow     overflowRing
		drainerWG    sync.WaitGroup
		closeOnce    sync.Once
		overflowMu   sync.Mutex
		// sendMu guards sends to l.ch against Close() closing it. Producers take
		// the read lock for the duration of a non-blocking enqueue; Close takes
		// the write lock before close(l.ch). This makes the logger self-defending
		// against a "send on closed channel" panic if a LogStmt call ever races
		// Close, instead of relying on callers to drain all producers first.
		sendMu sync.RWMutex
	}

	options struct {
		channel     chan Item
		innerLogger io.Closer
		zapLogger   *zap.Logger
	}

	Option func(*options) error
)

func WithChannel(ch chan Item) Option {
	return func(o *options) error {
		if o.channel != nil {
			close(o.channel)
		}
		o.channel = ch
		return nil
	}
}

func WithLogger(logger io.Closer, err error) Option {
	return func(o *options) error {
		if err != nil {
			return err
		}

		o.innerLogger = logger
		return nil
	}
}

func WithZapLogger(l *zap.Logger) Option {
	return func(o *options) error {
		o.zapLogger = l
		return nil
	}
}

func NewLogger(opts ...Option) (*Logger, error) {
	o := options{
		channel: make(chan Item, defaultChanSize),
	}

	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}

	if o.zapLogger == nil {
		o.zapLogger = zap.NewNop()
	}

	l := &Logger{
		closer:      o.innerLogger,
		logger:      o.zapLogger,
		ch:          o.channel,
		done:        make(chan struct{}),
		overflowSig: make(chan struct{}, 1),
	}
	l.overflowCond = sync.NewCond(&l.overflowMu)

	l.drainerWG.Add(1)
	go l.drainOverflow()

	return l, nil
}

func (l *Logger) LogStmt(item Item) error {
	if item.StatementType.IsSchema() {
		return nil
	}
	if item.StatementType.IsSelect() && item.FirstSuccessNS == 0 && item.LastSuccessNS == 0 && item.LastFailureNS == 0 {
		return nil
	}

	if l.closer == nil {
		return nil
	}

	// Step 1: try a non-blocking send into the bounded channel — fast path
	// when the committer is keeping up. enqueued/closed are decided under
	// sendMu so this can never race Close() closing l.ch.
	switch enqueued, closed := l.trySend(item); {
	case enqueued:
		metrics.StatementLoggerItems.Inc()
		metrics.StatementLoggerEnqueuedTotal.Inc()
		return nil
	case closed:
		// Logger already closed — discard quietly.
		return nil
	}

	// Step 2: channel is full. Park the item in the bounded overflow ring. If
	// the ring is also at its cap we BLOCK here until the drainer frees a slot —
	// nothing is ever dropped; the producers (gocql observer callbacks, and so
	// the mutation/validation workers) are simply rate-limited to the
	// committer's drain rate. overflowCond.Wait atomically releases overflowMu
	// while parked and re-acquires it on wake, so the Close() broadcast can
	// never be missed. The committer always consumes from the channel (even a
	// failed/timed-out batch removes its items), so a slot always eventually
	// frees and this never deadlocks.
	l.overflowMu.Lock()
	for l.overflow.count >= l.overflow.limit() {
		// Bail out promptly if we are shutting down.
		select {
		case <-l.done:
			l.overflowMu.Unlock()
			return nil
		default:
		}
		// Ensure the drainer is awake to free a slot, then park.
		l.signalDrainer()
		l.overflowCond.Wait()
	}
	// Re-check shutdown before pushing. The loop body's done-check is skipped
	// when the ring was never full, and a producer woken by the drainer exits
	// the loop without re-checking; in either case Close() may have already run
	// (drainer exited, l.ch closed). Pushing here would strand the item in the
	// ring forever, so bail instead — shutdown is the one time a not-yet-queued
	// item is allowed to be dropped.
	select {
	case <-l.done:
		l.overflowMu.Unlock()
		return nil
	default:
	}
	l.overflow.push(item)
	depth := l.overflow.count
	l.overflowMu.Unlock()

	metrics.StatementLoggerItems.Inc()
	metrics.StatementLoggerEnqueuedTotal.Inc()
	metrics.StatementLoggerOverflowTotal.Inc()
	metrics.StatementLoggerOverflowItems.Set(float64(depth))

	// Best-effort wake of the drainer; the channel has capacity 1 so a
	// pending signal is sufficient.
	l.signalDrainer()

	return nil
}

// trySend attempts a single non-blocking enqueue into l.ch, held under
// sendMu.RLock so it cannot race Close()'s close(l.ch). It returns
// (enqueued=true) when the item was accepted, (closed=true) when the logger is
// shutting down (caller should stop), or both false when the channel is
// momentarily full (caller falls through to the overflow ring). It never
// blocks: the inner select has a default branch.
func (l *Logger) trySend(item Item) (enqueued, closed bool) {
	l.sendMu.RLock()
	defer l.sendMu.RUnlock()

	select {
	case <-l.done:
		return false, true
	default:
	}

	select {
	case l.ch <- item:
		return true, false
	case <-l.done:
		return false, true
	default:
		return false, false
	}
}

// signalDrainer wakes the overflow drainer without blocking. It touches no
// mutex-guarded state (overflowSig is an independent buffered channel), so it is
// safe to call with or without overflowMu held.
func (l *Logger) signalDrainer() {
	select {
	case l.overflowSig <- struct{}{}:
	default:
	}
}

// drainOverflow continuously moves items from the overflow buffer into the
// bounded channel as soon as the committer makes room. It is the reason the
// statement logger can keep its "never drop" guarantee while LogStmt remains
// non-blocking.
//
// When l.done is closed, drainOverflow does NOT exit immediately: it first
// forwards all remaining overflow items to l.ch with blocking sends (safe
// because the inner committer — l.closer — is still alive at that point),
// then exits. Close() therefore only needs to wait for this goroutine and
// can close l.ch immediately after.
func (l *Logger) drainOverflow() {
	defer l.drainerWG.Done()

	// Use a short ticker as a safety net in case a wakeup signal is missed
	// during a tight race between LogStmt and the select below.  The ticker
	// fires unconditionally every 50ms, but the inner loop exits immediately
	// when the overflow queue is empty, so idle cost is negligible.
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-l.done:
			// LogStmt has been shut down. Drain whatever remains in the
			// overflow buffer before signalling the drainer is done.
			// Sends are blocking here: the inner committer is still alive
			// because Close() calls l.closer.Close() only after we return.
			l.flushRemainingOverflow()
			return
		case <-l.overflowSig:
		case <-ticker.C:
		}

		l.drainOverflowStep()
	}
}

// drainOverflowStep forwards items from the overflow buffer to l.ch during
// normal (non-shutdown) operation.  It exits as soon as either the overflow
// queue is empty or l.ch is full (the next ticker/signal will retry).
func (l *Logger) drainOverflowStep() {
	for {
		l.overflowMu.Lock()
		next, ok := l.overflow.pop()
		depth := l.overflow.count
		if ok {
			// We freed a ring slot — wake one producer that may be blocked in
			// LogStmt waiting for space (back-pressure release).
			l.overflowCond.Signal()
		}
		l.overflowMu.Unlock()
		if !ok {
			return
		}

		select {
		case l.ch <- next:
			metrics.StatementLoggerOverflowItems.Set(float64(depth))
		case <-l.done:
			// Shutdown fired while we were waiting for channel space.
			// Return the item we popped to the front so it is not lost;
			// flushRemainingOverflow will forward it. (Producers have
			// already stopped pushing once l.done is closed.)
			l.overflowMu.Lock()
			l.overflow.pushFront(next)
			l.overflowMu.Unlock()
			return
		}
	}
}

// flushRemainingOverflow forwards every item still in the overflow buffer to
// l.ch using blocking sends.  It is called only from drainOverflow after
// l.done is closed, so LogStmt is no longer enqueueing new items and the
// inner committer (l.closer) is still running.
//
// Theoretical TOCTOU note: there is a narrow window between LogStmt's check
// of <-l.done and the append to l.overflow where a late item may land in the
// overflow buffer after close(l.done) is called but before drainOverflow
// reaches flushRemainingOverflow.  This is fine: flushRemainingOverflow
// holds overflowMu on each iteration and drains whatever is present, so
// those late items will be picked up.  No items are silently dropped.
//
// The entire flush is bounded by a single total-budget deadline (30 s).
// A per-item timer would multiply: 10 s × N items can hold workload.Close
// for many minutes when the overflow has accumulated and gocql is dropping
// writes. With a shared deadline the worst case is always 30 s regardless
// of overflow depth.
const flushTotalTimeout = 30 * time.Second

func (l *Logger) flushRemainingOverflow() {
	deadline := time.NewTimer(flushTotalTimeout)
	defer deadline.Stop()

	for {
		l.overflowMu.Lock()
		next, ok := l.overflow.pop()
		depth := l.overflow.count
		l.overflowMu.Unlock()
		if !ok {
			metrics.StatementLoggerOverflowItems.Set(0)
			return
		}

		metrics.StatementLoggerOverflowItems.Set(float64(depth))

		select {
		case l.ch <- next:
		case <-deadline.C:
			l.overflowMu.Lock()
			dropped := 1 + l.overflow.count
			l.overflow.reset()
			l.overflowMu.Unlock()
			metrics.StatementLoggerOverflowItems.Set(0)
			l.logger.Warn("flushRemainingOverflow: downstream committer stalled, dropping remaining overflow items",
				zap.Int("dropped", dropped),
				zap.Duration("budget", flushTotalTimeout),
			)
			return
		}
	}
}

// Close shuts the logger down and closes the inner committer. It is idempotent.
//
// Close is self-defending: it takes sendMu.Lock before closing l.ch, so a
// LogStmt call that races Close can never send on a closed channel (it either
// completes its enqueue first or observes l.done under sendMu and returns).
// Callers therefore do NOT need to guarantee all producers have drained before
// calling Close, though in gemini the store still closes its in-flight barrier
// first as defense in depth.
func (l *Logger) Close() error {
	var innerErr error

	l.closeOnce.Do(func() {
		// Signal all in-flight and future LogStmt calls to bail out.
		// The drainer (drainOverflow) keeps running after this: it will
		// forward every remaining overflow item to l.ch with blocking
		// sends before it exits.
		close(l.done)

		// Wake any producer blocked on a full overflow ring so it observes
		// l.done and returns instead of waiting forever. Acquiring overflowMu
		// before broadcasting closes the lost-wakeup window: a producer either
		// already holds the lock (and will see l.done on its next check) or is
		// parked in Wait (and will be woken here).
		l.overflowMu.Lock()
		l.overflowCond.Broadcast()
		l.overflowMu.Unlock()

		// Wait for the drainer to finish draining all overflow.
		// Only after this point is it safe to close l.ch, because the
		// drainer is the sole remaining sender.
		l.drainerWG.Wait()

		// Close the data channel so downstream consumers see EOF. Taking
		// sendMu.Lock first guarantees no producer is mid fast-path send: any
		// trySend either finished (RUnlocked) or will observe l.done before it
		// reaches the send. Producers parked in the overflow ring do NOT hold
		// sendMu, so they cannot deadlock this — they were already woken above.
		l.sendMu.Lock()
		if l.ch != nil {
			close(l.ch)
		}
		l.sendMu.Unlock()

		if l.closer != nil {
			innerErr = l.closer.Close()
		}
	})

	return innerErr
}

func (i Item) MemoryFootprint() uint64 {
	size := uint64(unsafe.Sizeof(Item{})) + utils.Sizeof(i.Statement) +
		utils.Sizeof(i.Host)

	return size
}
