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
	values       chan *typedef.ValueWithToken
	oldValues    chan *typedef.ValueWithToken
	inFlight     inflight.InFlight
	wakeUpSignal chan<- struct{} // wakes up generator
	closed       atomic.Bool
	isStale      atomic.Bool
}

func (s *Partition) MarkStale() error {
	s.isStale.Store(true)
	return s.Close()
}

func (s *Partition) Stale() bool {
	return s.isStale.Load()
}

// get returns a new value and ensures that it's corresponding token
// is not already in-flight.
func (s *Partition) get() *typedef.ValueWithToken {
	for {
		v := s.pick()
		if v == nil || s.inFlight.AddIfNotPresent(v.Token) {
			return v
		}
	}
}

// getOld returns a previously used value and token or a new if
// the old queue is empty.
func (s *Partition) getOld() *typedef.ValueWithToken {
	select {
	case v := <-s.oldValues:
		return v
	default:
		return s.get()
	}
}

// giveOld returns the supplied value for later reuse unless the value
// is empty in which case it removes the corresponding token from the
// in-flight tracking.
func (s *Partition) giveOld(v *typedef.ValueWithToken) {
	ch := s.safelyGetOldValuesChannel()
	if ch == nil {
		return
	}
	select {
	case ch <- v:
	default:
		// Old partition buffer is full, just drop the value
	}
}

// releaseToken removes the corresponding token from the in-flight tracking.
func (s *Partition) releaseToken(token uint64) {
	s.inFlight.Delete(token)
}

func (s *Partition) wakeUp() {
	select {
	case s.wakeUpSignal <- struct{}{}:
	default:
	}
}

func (s *Partition) pick() *typedef.ValueWithToken {
	select {
	case val := <-s.values:
		if len(s.values) <= cap(s.values)/4 {
			s.wakeUp() // channel at 25% capacity, trigger generator
		}
		return val
	default:
		s.wakeUp() // channel empty, need to wait for new values
		return <-s.values
	}
}

func (s *Partition) safelyGetOldValuesChannel() chan *typedef.ValueWithToken {
	if s.closed.Load() {
		// Since only giveOld could have been potentially called after partition is closed
		// we need to protect it against writing to closed channel
		return nil
	}

	return s.oldValues
}

func (s *Partition) Close() error {
	for !s.closed.CompareAndSwap(false, true) {
	}

	close(s.values)
	close(s.oldValues)

	return nil
}

type Partitions []*Partition

func (p Partitions) Close() error {
	var err error
	for _, part := range p {
		err = multierr.Append(err, part.Close())
	}

	return err
}

func NewPartitions(count, pkBufferSize int, wakeUpSignal chan struct{}) Partitions {
	partitions := make(Partitions, count)
	for i := 0; i < len(partitions); i++ {
		partitions[i] = &Partition{
			values:       make(chan *typedef.ValueWithToken, pkBufferSize),
			oldValues:    make(chan *typedef.ValueWithToken, pkBufferSize),
			inFlight:     inflight.New(),
			wakeUpSignal: wakeUpSignal,
		}
	}
	return partitions
}
