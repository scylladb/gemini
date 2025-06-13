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

package store

import (
	"context"
	"sync"

	"github.com/samber/mo"
)

type (
	item struct {
		ctx context.Context
		ch  chan<- mo.Result[Rows]
		cb  func(context.Context) (Rows, error)
	}

	workers struct {
		chPool sync.Pool
		ch     chan item
	}
)

func newWorkers(count int) *workers {
	if count < 1 {
		panic("count must be greater than 0")
	}

	w := &workers{
		chPool: sync.Pool{
			New: func() any {
				return make(chan mo.Result[Rows], 1)
			},
		},
		ch: make(chan item, count*2),
	}

	for range count {
		go func() {
			for it := range w.ch {
				rows, err := it.cb(it.ctx)

				if err != nil {
					it.ch <- mo.Err[Rows](err)
				} else {
					it.ch <- mo.Ok[Rows](rows)
				}
			}
		}()
	}

	return w
}

func (w *workers) Send(ctx context.Context, cb func(context.Context) (Rows, error)) chan mo.Result[Rows] {
	if cb == nil {
		panic("cb must not be nil")
	}

	ch := w.chPool.Get().(chan mo.Result[Rows])

	w.ch <- item{
		ch:  ch,
		cb:  cb,
		ctx: ctx,
	}

	return ch
}

func (w *workers) Release(ch chan mo.Result[Rows]) {
	if ch == nil {
		return
	}

	select {
	case <-ch:
	default:
	}

	w.chPool.Put(ch)
}

func (w *workers) Close() error {
	close(w.ch)
	return nil
}
