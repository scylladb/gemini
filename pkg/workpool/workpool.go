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
		chPool    sync.Pool
		ch        atomic.Pointer[chan item]
		closed    atomic.Bool
		closeOnce sync.Once
		mu        sync.RWMutex
	}
)

func New(count int) *Pool {
	if count < 2 {
		panic("count must be greater than 1 (minimum 2 workers required to prevent shutdown deadlocks)")
	}

	metrics.GeminiInformation.WithLabelValues("io_thread_pool").Set(float64(count))

	ch := make(chan item, count*ChannelSizeMultiplier)

	w := &Pool{
		chPool: sync.Pool{
			New: func() any {
				return make(chan mo.Result[any], 1)
			},
		},
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

		if err != nil {
			it.ch <- mo.Err[any](err)
		} else {
			it.ch <- mo.Ok[any](i)
		}
	}

	for range count {
		go func() {
			for {
				it, more := <-ch
				if !more {
					break
				}

				execute(it)
			}
		}()
	}

	return w
}

func (w *Pool) SendWithoutResult(ctx context.Context, callback func(context.Context)) {
	if callback == nil {
		panic("cb must not be nil")
	}

	w.mu.RLock()
	defer w.mu.RUnlock()

	if w.closed.Load() {
		return
	}

	ch := w.ch.Load()
	if ch == nil {
		return
	}

	select {
	case *ch <- item{
		cb:  mo.Right[cb, cbNoResult](callback),
		ctx: ctx,
	}:
	case <-ctx.Done():
		return
	default:
		// Channel is full or closed, skip
		return
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
		ch <- mo.Err[any](errors.New("workpool is closed"))
		return ch
	}

	sendCh := w.ch.Load()
	if sendCh == nil {
		ch <- mo.Err[any](errors.New("workpool channel is nil"))
		return ch
	}

	if ctx == nil {
		*sendCh <- item{
			ch:  ch,
			cb:  mo.Left[cb, cbNoResult](callback),
			ctx: ctx,
		}
	} else {
		select {
		case *sendCh <- item{
			ch:  ch,
			cb:  mo.Left[cb, cbNoResult](callback),
			ctx: ctx,
		}:
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
	w.closeOnce.Do(func() {
		w.mu.Lock()
		defer w.mu.Unlock()

		w.closed.Store(true)
		ch := w.ch.Swap(nil)
		if ch != nil {
			close(*ch)
		}
	})

	return nil
}
