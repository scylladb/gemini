// Copyright 2025 ScyllaDB
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
	"strconv"
	"testing"
	"time"

	"github.com/scylladb/gemini/pkg/typedef"
)

// BenchmarkWindowRing measures the core ring ops; all must be O(1), allocation
// free in steady state, and flat across the live count.
func BenchmarkWindowRing(b *testing.B) {
	b.Run("pushBack_popFront", func(b *testing.B) {
		var r ringBuf
		// Pre-fill so we exercise wrap-around, not just growth.
		for i := range int64(1024) {
			r.pushBack(bucketEntry{readyAtNs: i})
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			r.pushBack(bucketEntry{readyAtNs: int64(i)})
			_, _ = r.popFront()
		}
	})

	b.Run("pushBack_popBack", func(b *testing.B) {
		var r ringBuf
		for i := range int64(1024) {
			r.pushBack(bucketEntry{readyAtNs: i})
		}
		b.ReportAllocs()
		b.ResetTimer()
		for i := range b.N {
			r.pushBack(bucketEntry{readyAtNs: int64(i)})
			_, _ = r.popBack()
		}
	})
}

// BenchmarkEvictAtCap guards the O(1) hard-cap eviction. It measures the
// steady-state cost of one Delete (push) followed by one backstop eviction
// while the tracker sits at its cap, across cap sizes. The ns/op must stay FLAT
// across cap: the previous heap eviction scanned leaves (O(n)) and grew linearly
// with the live count, collapsing Delete throughput under a long residence
// window (e.g. a single 1h bucket). The per-bucket ring drops it to O(1).
func BenchmarkEvictAtCap(b *testing.B) {
	for _, capSize := range []int{1_000, 10_000, 100_000} {
		b.Run(strconv.Itoa(capSize), func(b *testing.B) {
			var r ringBuf
			base := int64(0)
			for i := range capSize {
				r.pushBack(bucketEntry{readyAtNs: base + int64(i)})
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := range b.N {
				r.pushBack(bucketEntry{readyAtNs: base + int64(capSize+i)})
				_, _ = r.popBack()
			}
		})
	}
}

// BenchmarkBulkOperations tests the bulk delete path.
func BenchmarkBulkOperations(b *testing.B) {
	b.Run("delete_single", func(b *testing.B) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(b.Context(), buckets, 0)
		defer d.Close()

		b.ResetTimer()
		for range b.N {
			values := typedef.NewValues(1)
			d.Delete(typedef.PartitionKeys{Values: values})
		}
	})

	b.Run("delete_bulk_10", func(b *testing.B) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(b.Context(), buckets, 0)
		defer d.Close()

		b.ResetTimer()
		for range b.N {
			batch := make([]typedef.PartitionKeys, 10)
			for i := range batch {
				batch[i] = typedef.PartitionKeys{Values: typedef.NewValues(1)}
			}
			d.DeleteBulk(batch)
		}
	})

	b.Run("delete_bulk_50", func(b *testing.B) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(b.Context(), buckets, 0)
		defer d.Close()

		b.ResetTimer()
		for range b.N {
			batch := make([]typedef.PartitionKeys, 50)
			for i := range batch {
				batch[i] = typedef.PartitionKeys{Values: typedef.NewValues(1)}
			}
			d.DeleteBulk(batch)
		}
	})

	b.Run("delete_bulk_100", func(b *testing.B) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(b.Context(), buckets, 0)
		defer d.Close()

		b.ResetTimer()
		for range b.N {
			batch := make([]typedef.PartitionKeys, 100)
			for i := range batch {
				batch[i] = typedef.PartitionKeys{Values: typedef.NewValues(1)}
			}
			d.DeleteBulk(batch)
		}
	})
}

// BenchmarkFastPath tests the atomic fast-path optimization.
func BenchmarkFastPath(b *testing.B) {
	b.Run("fast_path_check", func(b *testing.B) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(b.Context(), buckets, 0)
		defer d.Close()

		d.Delete(typedef.PartitionKeys{Values: typedef.NewValues(1)})
		time.Sleep(10 * time.Millisecond) // Let it settle

		b.ResetTimer()
		for range b.N {
			// This should hit the fast path (no lock needed)
			_ = d.win.nextReadyNs()
		}
	})

	b.Run("process_ready_fast_path", func(b *testing.B) {
		buckets := []time.Duration{1 * time.Second} // Far in future
		d := newDeleted(b.Context(), buckets, 0)
		defer d.Close()

		for range 100 {
			d.Delete(typedef.PartitionKeys{Values: typedef.NewValues(1)})
		}
		time.Sleep(10 * time.Millisecond)

		b.ResetTimer()
		for range b.N {
			d.processReady()
		}
	})
}

// BenchmarkUnixNanoComparison tests time comparison optimization.
func BenchmarkUnixNanoComparison(b *testing.B) {
	now := time.Now()
	future := now.Add(1 * time.Second)

	b.Run("time_before", func(b *testing.B) {
		for range b.N {
			_ = now.Before(future)
		}
	})

	b.Run("unixnano_comparison", func(b *testing.B) {
		nowNs := now.UnixNano()
		futureNs := future.UnixNano()
		for range b.N {
			_ = nowNs < futureNs
		}
	})
}

// BenchmarkConcurrentBulk tests concurrent bulk operations.
func BenchmarkConcurrentBulk(b *testing.B) {
	b.Run("concurrent_bulk_delete", func(b *testing.B) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(b.Context(), buckets, 0)
		defer d.Close()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				batch := make([]typedef.PartitionKeys, 10)
				for i := range batch {
					batch[i] = typedef.PartitionKeys{Values: typedef.NewValues(1)}
				}
				d.DeleteBulk(batch)
			}
		})
	})
}
