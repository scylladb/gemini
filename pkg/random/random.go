// Copyright 2025 ScyllaDB
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
	"math/rand/v2"
	"sync"
)

type GoRoutineSafeRandom struct {
	random rand.Rand
	mu     sync.Mutex
}

func NewGoRoutineSafeRandom(seed rand.Source) *GoRoutineSafeRandom {
	return &GoRoutineSafeRandom{
		random: *rand.New(seed),
	}
}

func (g *GoRoutineSafeRandom) Uint32() uint32 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.random.Uint32()
}

func (g *GoRoutineSafeRandom) IntN(p0 int) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.random.IntN(p0)
}

func (g *GoRoutineSafeRandom) Int64() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.random.Int64()
}

func (g *GoRoutineSafeRandom) Uint64() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.random.Uint64()
}

func (g *GoRoutineSafeRandom) Uint64N(p0 uint64) uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.random.Uint64N(p0)
}

func (g *GoRoutineSafeRandom) Int64N(p0 int64) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.random.Int64N(p0)
}

func (g *GoRoutineSafeRandom) Float64() float64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.random.Float64()
}
