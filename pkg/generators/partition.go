// Copyright 2019 ScyllaDB
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

package generators

import (
	"context"
	"sync/atomic"

	"go.uber.org/multierr"

	"github.com/scylladb/gemini/pkg/typedef"
)

type (
	Partition struct {
		values    atomic.Pointer[chan typedef.PartitionKeys]
		oldValues atomic.Pointer[chan typedef.PartitionKeys]
		wakeup    chan<- struct{}
		// inFlight  inflight.InFlight
		isStale atomic.Bool
	}
)

func (p *Partition) MarkStale() error {
	p.isStale.Store(true)
	return p.Close()
}

func (p *Partition) Stale() bool {
	return p.isStale.Load()
}

// get returns a new value and ensures that it's corresponding token
// is not already in-flight.
func (p *Partition) get(ctx context.Context) typedef.PartitionKeys {
	for {
		v := p.pick(ctx)
		if v.Token != 0 {
			return v
		}
	}
}

// getOld returns a previously used value and token or a new if
// the old queue is empty.
func (p *Partition) getOld(ctx context.Context) (typedef.PartitionKeys, bool) {
	if ch := p.oldValues.Load(); ch != nil {
		select {
		case v := <-*ch:
			return v, true
		case <-ctx.Done():
			return typedef.PartitionKeys{}, false
		}
	}

	return typedef.PartitionKeys{}, false
}

// giveOld returns the supplied value for later reuse unless the value
// is empty, in which case it removes the corresponding token from the
// in-flight tracking.
func (p *Partition) giveOld(ctx context.Context, v typedef.PartitionKeys) bool {
	if ch := p.oldValues.Load(); ch != nil {
		select {
		case <-ctx.Done():
			clear(v.Values)
			return false
		case *ch <- v:
			return true
		default:
			clear(v.Values)
			return false
		}
	}

	clear(v.Values)
	return false
}

func (p *Partition) push(v typedef.PartitionKeys) bool {
	if ch := p.values.Load(); ch != nil {
		select {
		case *ch <- v:
			return true
		default:
			clear(v.Values)
			return false
		}
	}

	return false
}

// releaseToken removes the corresponding token from the in-flight tracking.
func (p *Partition) releaseToken(_ uint32) {
	// p.inFlight.Delete(token)
}

func (p *Partition) wakeUp() {
	select {
	case p.wakeup <- struct{}{}:
	default:
	}
}

func (p *Partition) pick(ctx context.Context) typedef.PartitionKeys {
	if chPtr := p.values.Load(); chPtr != nil {
		ch := *chPtr
		select {
		case v := <-ch:
			if len(ch) <= cap(ch)/4 {
				p.wakeUp() // channel at 25% capacity, trigger generator
			}
			return v
		case <-ctx.Done():
			return typedef.PartitionKeys{}
		default:
			p.wakeUp()
			return <-ch
		}
	}

	return typedef.PartitionKeys{}
}

func (p *Partition) Close() error {
	oldValues := p.oldValues.Swap(nil)
	values := p.values.Swap(nil)

	close(*oldValues)
	close(*values)

	return nil
}

type Partitions []Partition

func (p Partitions) Close() error {
	var err error

	for i := range len(p) {
		err = multierr.Append(err, p[i].Close())
	}

	return err
}

func (p Partitions) FullValues() uint64 {
	full := uint64(0)

	for i := range len(p) {
		if p[i].Stale() {
			continue
		}

		if ch := p[i].values.Load(); ch != nil {
			full += uint64(len(*ch))
		}
	}

	return full
}

func (p Partitions) MaxValuesStored() uint64 {
	full := uint64(0)
	for i := range len(p) {
		if p[i].Stale() {
			continue
		}

		if ch := p[i].values.Load(); ch != nil {
			full += uint64(cap(*ch))
		}
	}

	return full
}

func NewPartitions(count int32, pkBufferSize uint64, wakeup chan<- struct{}) Partitions {
	partitions := make(Partitions, count)

	for i := range len(partitions) {
		values := make(chan typedef.PartitionKeys, pkBufferSize)
		oldValues := make(chan typedef.PartitionKeys, pkBufferSize)

		partitions[i] = Partition{
			wakeup: wakeup,
			// inFlight: inflight.New(),
		}

		partitions[i].values.Store(&values)
		partitions[i].oldValues.Store(&oldValues)
	}

	return partitions
}
