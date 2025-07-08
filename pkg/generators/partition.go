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
	"sync"
	"sync/atomic"

	"go.uber.org/multierr"

	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

type (
	Partition struct {
		values    chan typedef.PartitionKeys
		oldValues chan typedef.PartitionKeys
		wakeup    chan<- struct{}
		isStale   atomic.Bool
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
func (p *Partition) get(ctx context.Context) (typedef.PartitionKeys, error) {
	for {
		v, err := p.pick(ctx)
		if v.Token != 0 {
			return v, nil
		}

		if err != nil {
			return typedef.PartitionKeys{}, err
		}
	}
}

// getOld returns a previously used value and token or a new if
// the old queue is empty.
func (p *Partition) getOld(ctx context.Context) (typedef.PartitionKeys, error) {
	select {
	case v := <-p.oldValues:
		return v, nil
	case <-ctx.Done():
		return typedef.PartitionKeys{}, context.Canceled
	default:
		return typedef.PartitionKeys{}, utils.ErrNoPartitionKeyValues
	}
}

// giveOld returns the supplied value for later reuse unless the value
// is empty, in which case it removes the corresponding token from the
// in-flight tracking.
func (p *Partition) giveOld(ctx context.Context, v typedef.PartitionKeys) bool {
	select {
	case p.oldValues <- v:
		return true
	case <-ctx.Done():
		return false
	default:
		return false
	}
}

func (p *Partition) push(v typedef.PartitionKeys) bool {
	select {
	case p.values <- v:
		return true
	default:
		return false
	}
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

func (p *Partition) pick(ctx context.Context) (typedef.PartitionKeys, error) {
	select {
	case v := <-p.values:
		if len(p.values) <= cap(p.values)/4 {
			p.wakeUp() // channel at 25% capacity, trigger generator
		}
		return v, nil
	case <-ctx.Done():
		return typedef.PartitionKeys{}, context.Canceled
	default:
		p.wakeUp()
		return <-p.values, nil
	}
}

//go:norace
func (p *Partition) Close() error {
	close(p.oldValues)
	close(p.values)

	return nil
}

type Partitions struct {
	parts []Partition
	mu    sync.Mutex
}

var closePartitions sync.Once

func (p *Partitions) Close() error {
	var err error
	closePartitions.Do(func() {
		p.mu.Lock()
		defer p.mu.Unlock()

		for i := range len(p.parts) {
			err = multierr.Append(err, p.parts[i].Close())
		}
	})

	return err
}

func (p *Partitions) Get(token int) *Partition {
	idx := token % len(p.parts)
	return &p.parts[idx]
}

func (p *Partitions) Len() int {
	return len(p.parts)
}

func NewPartitions(count, pkBufferSize int, wakeup chan<- struct{}) *Partitions {
	partitions := make([]Partition, count)

	for i := range len(partitions) {
		partitions[i] = Partition{
			values:    make(chan typedef.PartitionKeys, pkBufferSize),
			oldValues: make(chan typedef.PartitionKeys, pkBufferSize),
			wakeup:    wakeup,
			isStale:   atomic.Bool{},
		}
	}

	return &Partitions{
		parts: partitions,
	}
}
