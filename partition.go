// Copyright 2019 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gemini

import (
	"context"

	"github.com/scylladb/gemini/inflight"
)

type Partition struct {
	ctx       context.Context
	values    chan ValueWithToken
	oldValues chan ValueWithToken
	inFlight  inflight.InFlight
}

// get returns a new value and ensures that it's corresponding token
// is not already in-flight.
func (s *Partition) get() (ValueWithToken, bool) {
	for {
		v := s.pick()
		if s.inFlight.AddIfNotPresent(v.Token) {
			return v, true
		}
	}
}

var emptyValueWithToken = ValueWithToken{}

// getOld returns a previously used value and token or a new if
// the old queue is empty.
func (s *Partition) getOld() (ValueWithToken, bool) {
	select {
	case <-s.ctx.Done():
		return emptyValueWithToken, false
	case v, ok := <-s.oldValues:
		return v, ok
	default:
		return s.get()
	}
}

// giveOld returns the supplied value for later reuse unless the value
// is empty in which case it removes the corresponding token from the
// in-flight tracking.
func (s *Partition) giveOld(v ValueWithToken) {
	if len(v.Value) == 0 {
		s.inFlight.Delete(v.Token)
		return
	}
	select {
	case s.oldValues <- v:
	default:
		// Old partition buffer is full, just drop the value
	}
}

func (s *Partition) pick() ValueWithToken {
	return <-s.values
}
