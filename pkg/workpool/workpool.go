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

package workpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/samber/mo"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/metrics"
)

const ChannelSizeMultiplier = 4

type (
	cb         func(context.Context) (any, error)
	cbNoResult func(context.Context)

	item struct {
		ctx context.Context
		ch  chan<- mo.Result[any]
		cb  mo.Either[cb, cbNoResult]
	}
	Pool struct {
		chPool sync.Pool
		ch     atomic.Pointer[chan item]
		logger *zap.Logger
		mu     sync.RWMutex
		closed atomic.Bool
	}
)

func New(count int) *Pool {
	if count < 1 {
		panic("count must be at least 1")
	}

	logger := zap.L().Named("workpool")
	logger.Debug("creating workpool",
		zap.Int("worker_count", count),
		zap.Int("channel_size", count*ChannelSizeMultiplier),
	)

	metrics.GeminiInformation.WithLabelValues("io_thread_pool").Set(float64(count))

	ch := make(chan item, count*ChannelSizeMultiplier)

	w := &Pool{
		chPool: sync.Pool{
			New: func() any {
				return make(chan mo.Result[any], 1)
			},
		},
		logger: logger,
	}

	w.ch.Store(&ch)

	execute := func(it item) {
		if it.cb.IsRight() {
			it.cb.MustRight()(it.ctx)
			return
		}

		i, err := it.cb.MustLeft()(it.ctx)

		if it.ch == nil {
			return
		}

		// Try to send result, but don't block if context is cancelled
		// The channel is buffered (size 1), so this should normally not block
		// But if the receiver already gave up, we shouldn't block the worker
		result := mo.Err[any](err)
		if err == nil {
			result = mo.Ok[any](i)
		}

		if it.ctx != nil {
			select {
			case it.ch <- result:
			case <-it.ctx.Done():
				// Context cancelled, receiver likely gone, don't block
			}
		} else {
			// No context, always send (channel is buffered)
			it.ch <- result
		}
	}

	for i := range count {
		workerID := i
		go func() {
			logger.Debug("worker started", zap.Int("worker_id", workerID))
			for {
				it, more := <-ch
				if !more {
					logger.Debug("worker shutting down", zap.Int("worker_id", workerID))
					break
				}

				execute(it)
			}
		}()
	}

	logger.Debug("workpool created successfully", zap.Int("workers", count))
	return w
}

func (w *Pool) SendWithoutResult(ctx context.Context, callback func(context.Context)) error {
	if callback == nil {
		panic("cb must not be nil")
	}

	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.closed.Load() {
		w.logger.Warn("attempt to send to closed workpool")
		return errors.New("workpool is closed")
	}

	ch := w.ch.Load()
	if ch == nil {
		w.logger.Error("workpool channel is nil")
		return errors.New("workpool channel is nil")
	}

	// Check if context is already cancelled before attempting to send
	if ctx != nil {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}

	if ctx == nil {
		*ch <- item{
			cb:  mo.Right[cb, cbNoResult](callback),
			ctx: ctx,
		}
		return nil
	}

	select {
	case *ch <- item{
		cb:  mo.Right[cb, cbNoResult](callback),
		ctx: ctx,
	}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Pool) Send(ctx context.Context, callback func(context.Context) (any, error)) chan mo.Result[any] {
	if callback == nil {
		panic("cb must not be nil")
	}

	ch := w.chPool.Get().(chan mo.Result[any])

	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.closed.Load() {
		// Pool is closed, return an error result
		w.logger.Warn("attempt to send to closed workpool")
		ch <- mo.Err[any](errors.New("workpool is closed"))
		return ch
	}

	sendCh := w.ch.Load()
	if sendCh == nil {
		w.logger.Error("workpool channel is nil")
		ch <- mo.Err[any](errors.New("workpool channel is nil"))
		return ch
	}

	it := item{
		ch:  ch,
		cb:  mo.Left[cb, cbNoResult](callback),
		ctx: ctx,
	}

	// Always use select to prevent blocking, even with nil context
	if ctx == nil {
		*sendCh <- it
	} else {
		select {
		case *sendCh <- it:
		case <-ctx.Done():
			ch <- mo.Err[any](ctx.Err())
		}
	}

	return ch
}

func (w *Pool) Release(ch chan mo.Result[any]) {
	if ch == nil {
		return
	}

	select {
	case <-ch:
	default:
	}

	w.chPool.Put(ch)
}

func (w *Pool) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed.Load() {
		w.logger.Debug("workpool already closed")
		return nil
	}

	w.logger.Debug("closing workpool")
	w.closed.Store(true)
	ch := w.ch.Swap(nil)
	if ch != nil {
		close(*ch)
		w.logger.Debug("workpool channel closed, workers will shutdown")
	}

	w.logger.Debug("workpool closed successfully")
	return nil
}
