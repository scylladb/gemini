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

package random

import (
	crand "crypto/rand"
	"encoding/binary"
	"math/bits"
	"math/rand/v2"
	"time"
)

var Source rand.Source

type crandSource struct{}

func (c *crandSource) Uint64() uint64 {
	var out [8]byte
	_, _ = crand.Read(out[:])
	return binary.LittleEndian.Uint64(out[:])
}

func (c *crandSource) Seed(_ uint64) {}

type TimeSource struct {
	source rand.Source
}

func NewTimeSource() *TimeSource {
	now := time.Now()
	val := uint64(now.Nanosecond() * now.Second())

	return &TimeSource{
		source: rand.NewPCG(val, val),
	}
}

func (c *TimeSource) Uint64() uint64 {
	now := time.Now()
	val := c.source.Uint64()
	return bits.RotateLeft64(val^uint64(now.Nanosecond()*now.Second()), -int(val>>58))
}

func (c *TimeSource) Seed(_ uint64) {}

func init() {
	var b [8]byte
	_, err := crand.Read(b[:])
	if err == nil {
		Source = &crandSource{}
	} else {
		Source = NewTimeSource()
	}
}
