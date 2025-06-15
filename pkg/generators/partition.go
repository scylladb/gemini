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
	"sync/atomic"

	"go.uber.org/multierr"

	"github.com/scylladb/gemini/pkg/inflight"
	"github.com/scylladb/gemini/pkg/typedef"
)

type Partition struct {
	values    chan typedef.ValueWithToken
	oldValues chan typedef.ValueWithToken
	inFlight  inflight.InFlight
	closed    atomic.Bool
	isStale   atomic.Bool
}

func (p *Partition) MarkStale() error {
	p.isStale.Store(true)
	return p.Close()
}

func (p *Partition) Stale() bool {
	return p.isStale.Load()
}

// get returns a new value and ensures that it's corresponding token
// is not already in-flight.
func (p *Partition) get() typedef.ValueWithToken {
	for {
		v := p.pick()
		if v.Token != 0 || p.inFlight.AddIfNotPresent(v.Token) {
			return v
		}
	}
}

// getOld returns a previously used value and token or a new if
// the old queue is empty.
func (p *Partition) getOld() (typedef.ValueWithToken, bool) {
	select {
	case v := <-p.oldValues:
		return v, true
	default:
		return p.get(), false
	}
}

// giveOld returns the supplied value for later reuse unless the value
// is empty, in which case it removes the corresponding token from the
// in-flight tracking.
func (p *Partition) giveOld(v typedef.ValueWithToken) bool {
	if p.closed.Load() {
		// Since only giveOld could have been potentially called after partition is closed
		// we need to protect it against writing to closed channel
		return false
	}

	select {
	case p.oldValues <- v:
		return true
	default:
		clear(v.Value)
		return false
		// Old partition buffer is full, just drop the value
	}
}

func (p *Partition) push(v typedef.ValueWithToken) struct{ Full bool } {
	if p.closed.Load() {
		// Since only giveOld could have been potentially called after partition is closed
		// we need to protect it against writing to a closed channel
		return struct{ Full bool }{Full: true}
	}

	select {
	case p.values <- v:
		if cap(p.values) == len(p.values) {
			return struct{ Full bool }{Full: true}
		}

		return struct{ Full bool }{Full: false}
	default:
		clear(v.Value)
		return struct{ Full bool }{Full: true}
	}
}

// releaseToken removes the corresponding token from the in-flight tracking.
func (p *Partition) releaseToken(token uint64) {
	p.inFlight.Delete(token)
}

func (p *Partition) pick() typedef.ValueWithToken {
	return <-p.values
}

func (p *Partition) Close() error {
	p.closed.Store(true)
	close(p.values)
	close(p.oldValues)

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
	c := cap(p[0].values)

	full := uint64(0)
	for i := range len(p) {
		if c-len(p[i].values) <= 0 {
			full++
		}
	}

	return full
}

func NewPartitions(count, pkBufferSize uint64) Partitions {
	partitions := make(Partitions, count)

	for i := range len(partitions) {
		values := make(chan typedef.ValueWithToken, pkBufferSize)
		oldValues := make(chan typedef.ValueWithToken, pkBufferSize)

		partitions[i] = Partition{
			values:    values,
			oldValues: oldValues,
			inFlight:  inflight.New(),
		}
	}

	return partitions
}
