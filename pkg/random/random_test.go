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

package random_test

import (
	"math/rand/v2"
	"sync"
	"testing"

	"github.com/scylladb/gemini/pkg/random"
)

func newRandom() *random.GoRoutineSafeRandom {
	return random.NewGoRoutineSafeRandom(rand.NewPCG(42, 0))
}

func TestGoRoutineSafeRandom_Uint32(t *testing.T) {
	t.Parallel()
	r := newRandom()
	for range 100 {
		_ = r.Uint32()
	}
}

func TestGoRoutineSafeRandom_IntN(t *testing.T) {
	t.Parallel()
	r := newRandom()
	n := 1000
	for range 500 {
		v := r.IntN(n)
		if v < 0 || v >= n {
			t.Fatalf("IntN(%d) = %d: out of range [0, %d)", n, v, n)
		}
	}
}

func TestGoRoutineSafeRandom_Int64(t *testing.T) {
	t.Parallel()
	r := newRandom()
	for range 100 {
		v := r.Int64()
		if v < 0 {
			t.Fatalf("Int64() = %d: expected non-negative", v)
		}
	}
}

func TestGoRoutineSafeRandom_Uint64(t *testing.T) {
	t.Parallel()
	r := newRandom()
	// Just verify it runs without panic.
	for range 100 {
		_ = r.Uint64()
	}
}

func TestGoRoutineSafeRandom_Uint64N(t *testing.T) {
	t.Parallel()
	r := newRandom()
	var n uint64 = 1_000_000
	for range 500 {
		v := r.Uint64N(n)
		if v >= n {
			t.Fatalf("Uint64N(%d) = %d: out of range [0, %d)", n, v, n)
		}
	}
}

func TestGoRoutineSafeRandom_Int64N(t *testing.T) {
	t.Parallel()
	r := newRandom()
	var n int64 = 1_000_000
	for range 500 {
		v := r.Int64N(n)
		if v < 0 || v >= n {
			t.Fatalf("Int64N(%d) = %d: out of range [0, %d)", n, v, n)
		}
	}
}

func TestGoRoutineSafeRandom_Float64(t *testing.T) {
	t.Parallel()
	r := newRandom()
	for range 500 {
		v := r.Float64()
		if v < 0.0 || v >= 1.0 {
			t.Fatalf("Float64() = %v: expected [0.0, 1.0)", v)
		}
	}
}

// TestGoRoutineSafeRandom_ConcurrentSafety verifies no data races under concurrent access.
func TestGoRoutineSafeRandom_ConcurrentSafety(t *testing.T) {
	t.Parallel()
	r := newRandom()
	const goroutines = 50
	const iterations = 200

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for range iterations {
				r.Uint32()
				r.IntN(100)
				r.Int64()
				r.Uint64()
				r.Uint64N(100)
				r.Int64N(100)
				r.Float64()
			}
		}()
	}
	wg.Wait()
}

func TestTimeSource_ProducesValues(t *testing.T) {
	t.Parallel()
	src := random.NewTimeSource()
	// Verify Uint64 can be called without panicking and returns uint64.
	for range 20 {
		_ = src.Uint64()
	}
}

func TestSource_IsInitialized(t *testing.T) {
	t.Parallel()
	if random.Source == nil {
		t.Fatal("package-level Source must be non-nil after init()")
	}
	// Verify it works as a rand.Source.
	for range 20 {
		_ = random.Source.Uint64()
	}
}
