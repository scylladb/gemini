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

package inflight

import (
	"cmp"
	"reflect"
	"runtime"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
)

func FuncName(fn any) string {
	if fn == nil {
		return "<nil>"
	}
	// reflect.Value.Pointer gives us the entry PC for the function.
	pc := reflect.ValueOf(fn).Pointer()
	if pc == 0 {
		return "<unknown>"
	}
	return runtime.FuncForPC(pc).Name()
}

func TestBloomBasic(t *testing.T) {
	t.Parallel()

	const (
		mBits     = 1 << 24 // 16 Mi bits  ➜ 2 MiB – keeps FPR extremely low
		nInserts  = 1_000   // load factor ≈ 0.06 %
		threshold = 5       // acceptable # of false positives in the test
	)

	hashFns := []func(uint64) uint64{murmurMix64}

	for _, hashFn := range hashFns {
		t.Run(FuncName(hashFn), func(t *testing.T) {
			t.Parallel()
			bloom := NewBloom(mBits)
			for i := range nInserts {
				added := bloom.AddIfNotPresent(uint64(i))
				if !added {
					t.Fatalf("value %d reported as duplicate on first add", i)
				}
				if bloom.AddIfNotPresent(uint64(i)) {
					t.Fatalf("value %d reported as new on second add", i)
				}
			}

			// every inserted value must be reported present
			for i := range nInserts {
				if !bloom.Has(uint64(i)) {
					t.Fatalf("false negative for value %d", i)
				}
			}

			// measure false-positive count on a disjoint range
			fp := 0
			for i := nInserts; i < 2*nInserts; i++ {
				if bloom.Has(uint64(i)) {
					fp++
				}
			}
			if fp > threshold {
				t.Fatalf("too many false positives: got %d want ≤ %d", fp, threshold)
			}
		})
	}
}

func TestBloomDelete(t *testing.T) {
	t.Parallel()

	const (
		mBits    = 1 << 24
		nSamples = 256
	)

	hashFns := []func(uint64) uint64{murmurMix64}

	for _, hashFn := range hashFns {
		t.Run(FuncName(hashFn), func(t *testing.T) {
			t.Parallel()
			bloom := NewBloom(mBits)

			for i := range nSamples {
				v := uint64(i)
				if !bloom.AddIfNotPresent(v) {
					t.Fatalf("initial add failed for %d", v)
				}
				bloom.Delete(v)
				// *Most* of the time the value should now be reported absent.
				// On a rare hash collision it may still be present, so we re-add
				// and assert that the filter reports it as old afterwards.
				added := bloom.AddIfNotPresent(v)
				if !added {
					t.Fatalf("value %d was not recognised as new after delete", v)
				}
				if bloom.AddIfNotPresent(v) {
					t.Fatalf("value %d still considered new after re-insert", v)
				}
			}
		})
	}
}

func BenchmarkBloom(b *testing.B) {
	const mBits = 1 << 20 // 1 MiB of bits (~128 KiB)

	hashSet := []func(uint64) uint64{murmurMix64} // k = 2

	for _, hash := range hashSet {
		b.Run(FuncName(hash), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			filter := NewBloom(mBits)

			b.RunParallel(func(pb *testing.PB) {
				for i := 0; pb.Next(); i++ {
					filter.AddIfNotPresent(uint64(i))
				}
			})

			b.RunParallel(func(pb *testing.PB) {
				for i := 0; pb.Next(); i++ {
					filter.Has(uint64(i))
				}
			})
		})
	}
}

func TestBloomFalsePositiveRate(t *testing.T) {
	t.Parallel()

	const (
		mBits   = 1 << 20
		nInsert = 128     // realistic load
		nProbes = 100_000 // disjoint probe range
	)

	hashFns := []func(uint64) uint64{murmurMix64}

	type stat struct {
		name string
		fp   int
	}
	results := make([]stat, 0, len(hashFns))

	for _, h := range hashFns {
		name := FuncName(h)
		bloom := NewBloom(mBits)

		// populate ---------------------------------------------------------
		for i := range nInsert {
			bloom.AddIfNotPresent(uint64(i))
		}

		// probe disjoint range --------------------------------------------
		fp := 0
		for i := nInsert; i < nInsert+nProbes; i++ {
			if bloom.Has(uint64(i)) {
				fp++
			}
		}
		t.Logf("%s: %d / %d false-positives (%.3f%%)",
			name, fp, nProbes, 100*float64(fp)/float64(nProbes))

		results = append(results, stat{name, fp})
	}

	// find the best performer ---------------------------------------------
	slices.SortFunc(results, func(a, b stat) int { return cmp.Compare(a.fp, b.fp) })
	best := results[0]
	t.Logf("BEST hash ‑ lowest FP: %s (%d)", best.name, best.fp)

	// sanity-check: ensure the best really is no worse than the others
	for _, r := range results {
		if r.fp < best.fp {
			t.Fatalf("internal sorting error")
		}
	}
}

func BenchmarkBloomHashFunctions(b *testing.B) {
	const mBits = 1 << 20 // 128 KiB

	hashFns := []func(uint64) uint64{murmurMix64}

	for _, h := range hashFns {
		b.Run(FuncName(h), func(b *testing.B) {
			filter := NewBloom(mBits)

			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				v := uint64(i)
				filter.AddIfNotPresent(v)
				_ = filter.Has(v + 123456) // mix reads & writes
			}
		})
	}
}

func TestBloomConcurrent(t *testing.T) {
	t.Parallel()

	const (
		mBits       = 1 << 20 // 128 KiB of bits
		workers     = 8       // number of goroutines hammering the filter
		perWorker   = 75_000  // unique keys each goroutine will insert
		totalValues = workers * perWorker
	)

	filter := NewBloom(mBits)

	// Each worker inserts a disjoint range [offset, offset+perWorker).
	wg := sync.WaitGroup{}
	wg.Add(workers)
	for w := range workers {
		go func() {
			defer wg.Done()
			start := uint64(w * perWorker)
			end := start + perWorker
			for v := start; v < end; v++ {
				filter.AddIfNotPresent(v)
				// Extra read to stress concurrent loads
				_ = filter.Has(v)
			}
		}()
	}
	wg.Wait()

	// After all goroutines have finished, every key must be present.
	for v := uint64(0); v < uint64(totalValues); v++ {
		if !filter.Has(v) {
			t.Fatalf("value %d missing after concurrent inserts", v)
		}
	}
}

func BenchmarkBloomConcurrent(b *testing.B) {
	const mBits = 1 << 20 // 128 KiB of bits

	filter := NewBloom(mBits)
	var counter uint64 // provides unique numbers across goroutines

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			v := atomic.AddUint64(&counter, 1)
			filter.AddIfNotPresent(v)
			_ = filter.Has(v + 123456) // mixed read
		}
	})
}
