// Copyright 2026 ScyllaDB
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

package partitions

import (
	"encoding/binary"
	"math/rand/v2"
	"sync/atomic"
	"testing"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/typedef"
)

const benchPoolSize = 10_000

func newBenchPartitions(b *testing.B) *Partitions {
	b.Helper()
	src, fn := distributions.New(distributions.Uniform, benchPoolSize, 1, 0, 0)
	parts := New(b.Context(), rand.New(src), fn, createTestTable(), typedef.PartitionRangeConfig{}, benchPoolSize, 0)
	b.Cleanup(parts.Close)
	return parts
}

func releaseBenchKeys(keys typedef.PartitionKeys) {
	if keys.Release != nil {
		keys.Release()
	}
}

// BenchmarkPartitionsNextParallel measures the validation pick: up to
// writtenPickAttempts slot reads, the invalid check, and one borrow. Half of
// the pool is written, so a pick usually draws more than once.
func BenchmarkPartitionsNextParallel(b *testing.B) {
	parts := newBenchPartitions(b)
	for i := range uint64(benchPoolSize / 2) {
		parts.markWritten(i)
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			releaseBenchKeys(parts.Next())
		}
	})
}

// BenchmarkPartitionsNextForWriteParallel measures the mutation pick.
func BenchmarkPartitionsNextForWriteParallel(b *testing.B) {
	parts := newBenchPartitions(b)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			releaseBenchKeys(parts.NextForWrite())
		}
	})
}

// BenchmarkPartitionsBorrowParallel measures one borrow of a fixed slot, so
// every goroutine contends on the same partition's reference count.
func BenchmarkPartitionsBorrowParallel(b *testing.B) {
	parts := newBenchPartitions(b)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			releaseBenchKeys(parts.borrow(0))
		}
	})
}

// BenchmarkPartitionsNextWithReplaceParallel measures the validation pick
// while one goroutine replaces partitions, so borrows race slot swaps.
func BenchmarkPartitionsNextWithReplaceParallel(b *testing.B) {
	parts := newBenchPartitions(b)
	for i := range uint64(benchPoolSize) {
		parts.markWritten(i)
	}

	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				releaseBenchKeys(parts.ReplaceNext())
			}
		}
	}()
	b.Cleanup(func() {
		close(stop)
		<-done
	})

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			releaseBenchKeys(parts.Next())
		}
	})
}

// BenchmarkPartitionsBorrowSerial measures one borrow without contention.
func BenchmarkPartitionsBorrowSerial(b *testing.B) {
	parts := newBenchPartitions(b)

	b.ReportAllocs()
	for b.Loop() {
		releaseBenchKeys(parts.borrow(1))
	}
}

// BenchmarkPartitionsNextWithInvalidParallel measures the validation pick when
// a tenth of the pool is invalid, so every draw reads the invalid flag.
func BenchmarkPartitionsNextWithInvalidParallel(b *testing.B) {
	src, fn := distributions.New(distributions.Uniform, benchPoolSize, 1, 0, 0)
	parts := New(b.Context(), rand.New(src), fn, createTestTable(), typedef.PartitionRangeConfig{}, benchPoolSize, benchPoolSize)
	b.Cleanup(parts.Close)
	for i := range uint64(benchPoolSize) {
		parts.markWritten(i)
		if i%10 == 0 {
			keys := parts.Get(i)
			parts.MarkInvalid(&keys)
			releaseBenchKeys(keys)
		}
	}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			releaseBenchKeys(parts.Next())
		}
	})
}

// newBenchWorker returns a worker view of parts with its own uniform index
// function, as every gemini worker has.
func newBenchWorker(parts *Partitions, seed uint64) *Worker {
	factory := distributions.NewFactory(distributions.Uniform, benchPoolSize, 0, 0)

	var s [32]byte
	binary.LittleEndian.PutUint64(s[:], seed)

	return parts.ForWorker(factory(s))
}

// BenchmarkPartitionsWorkerNextParallel is BenchmarkPartitionsNextParallel
// with one index function per goroutine.
func BenchmarkPartitionsWorkerNextParallel(b *testing.B) {
	parts := newBenchPartitions(b)
	for i := range uint64(benchPoolSize / 2) {
		parts.markWritten(i)
	}

	var seeds atomic.Uint64

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		w := newBenchWorker(parts, seeds.Add(1))
		for pb.Next() {
			releaseBenchKeys(w.Next())
		}
	})
}

// BenchmarkPartitionsWorkerNextForWriteParallel is
// BenchmarkPartitionsNextForWriteParallel with one index function per
// goroutine.
func BenchmarkPartitionsWorkerNextForWriteParallel(b *testing.B) {
	parts := newBenchPartitions(b)

	var seeds atomic.Uint64

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		w := newBenchWorker(parts, seeds.Add(1))
		for pb.Next() {
			releaseBenchKeys(w.NextForWrite())
		}
	})
}
