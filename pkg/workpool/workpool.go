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
	"sync"
	"sync/atomic"

	"github.com/samber/mo"
)

const ChannelSizeMultiplier = 4

type (
	item struct {
		ctx context.Context
		ch  chan<- mo.Result[any]
		cb  func(context.Context) (any, error)
	}
	Pool struct {
		chPool sync.Pool
		ch     atomic.Pointer[chan item]
		wg     *sync.WaitGroup
		mu     sync.RWMutex
	}
)

func New(count int) *Pool {
	if count < 1 {
		panic("count must be greater than 0")
	}

	ch := make(chan item, count*ChannelSizeMultiplier)

	wg := &sync.WaitGroup{}
	w := &Pool{
		wg: wg,
		chPool: sync.Pool{
			New: func() any {
				return make(chan mo.Result[any], 1)
			},
		},
	}

	w.ch.Store(&ch)

	execute := func(it item) {
		i, err := it.cb(it.ctx)

		if it.ch == nil {
			return
		}

		if err != nil {
			it.ch <- mo.Err[any](err)
		} else {
			it.ch <- mo.Ok[any](i)
		}
	}

	wg.Add(count)

	for range count {
		go func() {
			defer wg.Done()

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

func (w *Pool) SendWithoutResult(ctx context.Context, cb func(context.Context)) {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if cb == nil {
		panic("cb must not be nil")
	}

	if ch := w.ch.Load(); ch != nil {
		*ch <- item{
			cb: func(ctx context.Context) (any, error) {
				cb(ctx)
				return nil, nil
			},
			ctx: ctx,
		}
	}
}

func (w *Pool) Send(ctx context.Context, cb func(context.Context) (any, error)) chan mo.Result[any] {
	w.mu.RLock()
	defer w.mu.RUnlock()

	if cb == nil {
		panic("cb must not be nil")
	}

	ch := w.chPool.Get().(chan mo.Result[any])

	if sendCh := w.ch.Load(); sendCh != nil {
		*sendCh <- item{
			ch:  ch,
			cb:  cb,
			ctx: ctx,
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
	ch := w.ch.Swap(nil)
	w.mu.Lock()
	close(*ch)
	w.mu.Unlock()

	w.wg.Wait()

	return nil
}
