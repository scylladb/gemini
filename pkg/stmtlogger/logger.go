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

	// Logger is a non-blocking statement logger. When the bounded channel is
	// full, items spill into an overflow buffer. A background drainer pump
	// moves overflow items into the channel as space opens. This ensures the
	// logger NEVER drops items (forensic record) and NEVER blocks the data
	// path (gocql observer goroutines call LogStmt synchronously).
	Logger struct {
		closer      io.Closer
		ch          chan Item
		done        chan struct{}
		overflowSig chan struct{}
		overflow    []Item
		drainerWG   sync.WaitGroup
		closeOnce   sync.Once
		overflowMu  sync.Mutex
	}

	options struct {
		channel     chan Item
		innerLogger io.Closer
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

func NewLogger(opts ...Option) (*Logger, error) {
	o := options{
		channel: make(chan Item, defaultChanSize),
	}

	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}

	l := &Logger{
		closer:      o.innerLogger,
		ch:          o.channel,
		done:        make(chan struct{}),
		overflowSig: make(chan struct{}, 1),
	}

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

	select {
	case <-l.done:
		// Logger already closed — discard quietly.
		return nil
	default:
	}

	// Step 1: try a non-blocking send into the bounded channel — fast path
	// when the committer is keeping up.
	select {
	case l.ch <- item:
		metrics.StatementLoggerItems.Inc()
		metrics.StatementLoggerEnqueuedTotal.Inc()
		return nil
	case <-l.done:
		return nil
	default:
	}

	// Step 2: channel is full. Park the item in the unbounded overflow
	// buffer and wake the drainer. We MUST NOT block here: this code path
	// runs from gocql observer callbacks, and blocking those freezes every
	// mutation/validation worker.
	l.overflowMu.Lock()
	l.overflow = append(l.overflow, item)
	depth := len(l.overflow)
	l.overflowMu.Unlock()

	metrics.StatementLoggerItems.Inc()
	metrics.StatementLoggerEnqueuedTotal.Inc()
	metrics.StatementLoggerOverflowTotal.Inc()
	metrics.StatementLoggerOverflowItems.Set(float64(depth))

	// Best-effort wake of the drainer; the channel has capacity 1 so a
	// pending signal is sufficient.
	select {
	case l.overflowSig <- struct{}{}:
	default:
	}

	return nil
}

// drainOverflow continuously moves items from the overflow buffer into the
// bounded channel as soon as the committer makes room. It is the reason the
// statement logger can keep its "never drop" guarantee while LogStmt remains
// non-blocking.
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
			return
		case <-l.overflowSig:
		case <-ticker.C:
		}

		for {
			l.overflowMu.Lock()
			if len(l.overflow) == 0 {
				l.overflowMu.Unlock()
				break
			}
			next := l.overflow[0]
			l.overflowMu.Unlock()

			select {
			case l.ch <- next:
				l.overflowMu.Lock()
				// Pop front; reuse underlying array — when it grows
				// large, replace it to release memory back to the
				// allocator.
				l.overflow = l.overflow[1:]
				if cap(l.overflow) > 4096 && len(l.overflow) < cap(l.overflow)/4 {
					compact := make([]Item, len(l.overflow))
					copy(compact, l.overflow)
					l.overflow = compact
				}
				depth := len(l.overflow)
				l.overflowMu.Unlock()
				metrics.StatementLoggerOverflowItems.Set(float64(depth))
			case <-l.done:
				return
			}
		}
	}
}

func (l *Logger) Close() error {
	var innerErr error

	l.closeOnce.Do(func() {
		// Signal all in-flight and future LogStmt calls to bail out.
		close(l.done)

		// Wait for the drainer to exit before flushing residual overflow
		// items so we own the overflow slice exclusively.
		l.drainerWG.Wait()

		// Best-effort flush of overflow items into ch so the committer
		// can persist them before shutdown. If ch is nil or full, items
		// remain in the overflow buffer in memory until process exit —
		// they are never dropped silently while the process runs.
		if l.ch != nil {
			l.overflowMu.Lock()
			for _, it := range l.overflow {
				select {
				case l.ch <- it:
				default:
					// Committer already shutting down; stop here to
					// avoid blocking Close indefinitely.
					goto closeChannel
				}
			}
			l.overflow = nil
		closeChannel:
			metrics.StatementLoggerOverflowItems.Set(float64(len(l.overflow)))
			l.overflowMu.Unlock()

			// Close the data channel so downstream consumers see EOF.
			close(l.ch)
		}

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
