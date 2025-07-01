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

package testutils

import "github.com/scylladb/gemini/pkg/utils"

type NonRandSource uint64

var _ utils.Random = (*MockRandom)(nil)

func (s NonRandSource) Uint64() uint64 {
	return uint64(s)
}

func (s NonRandSource) Seed(uint64) {
}

type MockRandom struct {
	values []int
	index  int
	seed   uint64
}

func NewMockRandom(values ...int) *MockRandom {
	return &MockRandom{
		values: values,
		index:  0,
		seed:   1,
	}
}

// NewMockRandomWithSeed creates a new MockRandom with a seed for deterministic behavior
func NewMockRandomWithSeed(seed uint64) *MockRandom {
	return &MockRandom{
		values: nil,
		index:  0,
		seed:   seed,
	}
}

// Uint32 returns a pseudo-random uint32
func (m *MockRandom) Uint32() uint32 {
	if len(m.values) > 0 {
		if m.index >= len(m.values) {
			m.index = 0
		}
		val := uint32(m.values[m.index])
		m.index++
		return val
	}

	// Use a simple LCG (Linear Congruential Generator) for deterministic behavior
	m.seed = (m.seed*1103515245 + 12345) & 0x7fffffff
	return uint32(m.seed)
}

// IntN returns a non-negative pseudo-random number in [0,n)
func (m *MockRandom) IntN(n int) int {
	if n <= 0 {
		return 0
	}

	if len(m.values) > 0 {
		if m.index >= len(m.values) {
			m.index = 0
		}
		val := m.values[m.index] % n
		if val < 0 {
			val = -val % n
		}
		m.index++
		return val
	}

	return int(m.Uint32()) % n
}

// Int64 returns a pseudo-random int64
func (m *MockRandom) Int64() int64 {
	if len(m.values) > 0 {
		if m.index >= len(m.values) {
			m.index = 0
		}
		val := int64(m.values[m.index])
		m.index++
		return val
	}

	// Combine two Uint32 calls to get full int64 range
	high := int64(m.Uint32()) << 32
	low := int64(m.Uint32())
	return high | low
}

// Uint64 returns a pseudo-random uint64
func (m *MockRandom) Uint64() uint64 {
	if len(m.values) > 0 {
		if m.index >= len(m.values) {
			m.index = 0
		}
		val := uint64(m.values[m.index])
		m.index++
		return val
	}

	// Combine two Uint32 calls to get full uint64 range
	high := uint64(m.Uint32()) << 32
	low := uint64(m.Uint32())
	return high | low
}

// Uint64N returns a pseudo-random number in [0,n)
func (m *MockRandom) Uint64N(n uint64) uint64 {
	if n == 0 {
		return 0
	}

	if len(m.values) > 0 {
		if m.index >= len(m.values) {
			m.index = 0
		}
		val := uint64(m.values[m.index])
		m.index++
		return val % n
	}

	return m.Uint64() % n
}

// Int64N returns a pseudo-random number in [0,n)
func (m *MockRandom) Int64N(n int64) int64 {
	if n <= 0 {
		return 0
	}

	if len(m.values) > 0 {
		if m.index >= len(m.values) {
			m.index = 0
		}
		val := int64(m.values[m.index]) % n
		if val < 0 {
			val = -val % n
		}
		m.index++
		return val
	}

	return m.Int64() % n
}

// Reset resets the mock to start from the beginning of values
func (m *MockRandom) Reset() {
	m.index = 0
}

// SetValues updates the predefined values
func (m *MockRandom) SetValues(values ...int) {
	m.values = values
	m.index = 0
}

// SetSeed updates the seed for deterministic generation
func (m *MockRandom) SetSeed(seed uint64) {
	m.seed = seed
}

// GetCurrentIndex returns the current index in the values array
func (m *MockRandom) GetCurrentIndex() int {
	return m.index
}
