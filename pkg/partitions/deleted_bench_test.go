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
	"container/heap"
	"testing"
	"time"

	"github.com/scylladb/gemini/pkg/typedef"
)

// BenchmarkHeapOperations benchmarks the optimized heap vs standard library
//
//nolint:gocyclo
func BenchmarkHeapOperations(b *testing.B) {
	b.Run("pushInline", func(b *testing.B) {
		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 1024),
			len:  0,
		}
		now := time.Now()

		b.ResetTimer()
		for i := range b.N {
			h.pushInline(deletedPartition{
				values:  typedef.NewValues(1),
				readyAt: now.Add(time.Duration(i) * time.Millisecond),
			})
			if h.len >= 1000 {
				h.len = 0 // Reset to avoid overflow
			}
		}
	})

	b.Run("stdlib_heap_push", func(b *testing.B) {
		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 1024),
			len:  0,
		}
		heap.Init(h)
		now := time.Now()

		b.ResetTimer()
		for i := range b.N {
			heap.Push(h, deletedPartition{
				values:  typedef.NewValues(1),
				readyAt: now.Add(time.Duration(i) * time.Millisecond),
			})
			if h.len >= 1000 {
				for h.len > 0 {
					heap.Pop(h)
				}
			}
		}
	})

	b.Run("popInline", func(b *testing.B) {
		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 1024),
			len:  0,
		}
		now := time.Now()

		// Pre-fill
		for i := range 500 {
			h.pushInline(deletedPartition{
				values:  typedef.NewValues(1),
				readyAt: now.Add(time.Duration(i) * time.Millisecond),
			})
		}

		b.ResetTimer()
		for range b.N {
			if h.len == 0 {
				// Refill
				for i := range 500 {
					h.pushInline(deletedPartition{
						values:  typedef.NewValues(1),
						readyAt: now.Add(time.Duration(i) * time.Millisecond),
					})
				}
			}
			h.popInline()
		}
	})

	b.Run("stdlib_heap_pop", func(b *testing.B) {
		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 1024),
			len:  0,
		}
		heap.Init(h)
		now := time.Now()

		// Pre-fill
		for i := range 500 {
			heap.Push(h, deletedPartition{
				values:  typedef.NewValues(1),
				readyAt: now.Add(time.Duration(i) * time.Millisecond),
			})
		}

		b.ResetTimer()
		for range b.N {
			if h.len == 0 {
				// Refill
				for i := range 500 {
					heap.Push(h, deletedPartition{
						values:  typedef.NewValues(1),
						readyAt: now.Add(time.Duration(i) * time.Millisecond),
					})
				}
			}
			heap.Pop(h)
		}
	})

	b.Run("mixed_operations_inline", func(b *testing.B) {
		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 1024),
			len:  0,
		}
		now := time.Now()

		b.ResetTimer()
		for i := range b.N {
			// Push 3, pop 1
			h.pushInline(deletedPartition{
				values:  typedef.NewValues(1),
				readyAt: now.Add(time.Duration(i) * time.Millisecond),
			})
			h.pushInline(deletedPartition{
				values:  typedef.NewValues(1),
				readyAt: now.Add(time.Duration(i+1) * time.Millisecond),
			})
			h.pushInline(deletedPartition{
				values:  typedef.NewValues(1),
				readyAt: now.Add(time.Duration(i+2) * time.Millisecond),
			})
			if h.len > 0 {
				h.popInline()
			}
			if h.len >= 1000 {
				h.len = 100 // Keep some items
			}
		}
	})

	b.Run("mixed_operations_stdlib", func(b *testing.B) {
		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 1024),
			len:  0,
		}
		heap.Init(h)
		now := time.Now()

		b.ResetTimer()
		for i := range b.N {
			// Push 3, pop 1
			heap.Push(h, deletedPartition{
				values:  typedef.NewValues(1),
				readyAt: now.Add(time.Duration(i) * time.Millisecond),
			})
			heap.Push(h, deletedPartition{
				values:  typedef.NewValues(1),
				readyAt: now.Add(time.Duration(i+1) * time.Millisecond),
			})
			heap.Push(h, deletedPartition{
				values:  typedef.NewValues(1),
				readyAt: now.Add(time.Duration(i+2) * time.Millisecond),
			})
			if h.len > 0 {
				heap.Pop(h)
			}
			if h.len >= 1000 {
				for h.len > 100 {
					heap.Pop(h)
				}
			}
		}
	})

	b.Run("fixInline", func(b *testing.B) {
		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 1024),
			len:  0,
		}
		now := time.Now()

		// Pre-fill
		for i := range 500 {
			h.pushInline(deletedPartition{
				values:  typedef.NewValues(1),
				readyAt: now.Add(time.Duration(i) * time.Millisecond),
			})
		}

		b.ResetTimer()
		for i := range b.N {
			// Modify root and fix
			h.data[0].readyAt = now.Add(time.Duration(i+1000) * time.Millisecond)
			h.fixInline(0)
		}
	})

	b.Run("stdlib_heap_fix", func(b *testing.B) {
		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 1024),
			len:  0,
		}
		heap.Init(h)
		now := time.Now()

		// Pre-fill
		for i := range 500 {
			heap.Push(h, deletedPartition{
				values:  typedef.NewValues(1),
				readyAt: now.Add(time.Duration(i) * time.Millisecond),
			})
		}

		b.ResetTimer()
		for i := range b.N {
			// Modify root and fix
			h.data[0].readyAt = now.Add(time.Duration(i+1000) * time.Millisecond)
			heap.Fix(h, 0)
		}
	})
}

// BenchmarkHeapGrowth benchmarks heap growth patterns
func BenchmarkHeapGrowth(b *testing.B) {
	b.Run("small_to_large", func(b *testing.B) {
		now := time.Now()

		b.ResetTimer()
		for range b.N {
			h := &deletedPartitionHeap{
				data: make([]deletedPartition, 4),
				len:  0,
			}

			for i := range 1000 {
				h.pushInline(deletedPartition{
					values:  typedef.NewValues(1),
					readyAt: now.Add(time.Duration(i) * time.Millisecond),
				})
			}
		}
	})

	b.Run("preallocated", func(b *testing.B) {
		now := time.Now()

		b.ResetTimer()
		for range b.N {
			h := &deletedPartitionHeap{
				data: make([]deletedPartition, 1024),
				len:  0,
			}

			for i := range 1000 {
				h.pushInline(deletedPartition{
					values:  typedef.NewValues(1),
					readyAt: now.Add(time.Duration(i) * time.Millisecond),
				})
			}
		}
	})
}

// BenchmarkMemoryEfficiency tests memory allocations
func BenchmarkMemoryEfficiency(b *testing.B) {
	b.Run("pushInline_no_alloc", func(b *testing.B) {
		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 10000),
			len:  0,
		}
		now := time.Now()

		b.ResetTimer()
		b.ReportAllocs()
		for i := range b.N {
			h.pushInline(deletedPartition{
				values:  typedef.NewValues(1),
				readyAt: now.Add(time.Duration(i) * time.Millisecond),
			})
			if h.len >= 9000 {
				h.len = 0
			}
		}
	})

	b.Run("stdlib_push_with_alloc", func(b *testing.B) {
		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 10000),
			len:  0,
		}
		heap.Init(h)
		now := time.Now()

		b.ResetTimer()
		b.ReportAllocs()
		for i := range b.N {
			heap.Push(h, deletedPartition{
				values:  typedef.NewValues(1),
				readyAt: now.Add(time.Duration(i) * time.Millisecond),
			})
			if h.len >= 9000 {
				for h.len > 0 {
					heap.Pop(h)
				}
			}
		}
	})
}

// BenchmarkBulkOperations tests the new bulk delete optimization
func BenchmarkBulkOperations(b *testing.B) {
	b.Run("delete_single", func(b *testing.B) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(b.Context(), buckets)
		defer d.Close()

		b.ResetTimer()
		for range b.N {
			values := typedef.NewValues(1)
			d.Delete(values)
		}
	})

	b.Run("delete_bulk_10", func(b *testing.B) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(b.Context(), buckets)
		defer d.Close()

		b.ResetTimer()
		for range b.N {
			batch := make([]*typedef.Values, 10)
			for i := range batch {
				batch[i] = typedef.NewValues(1)
			}
			d.DeleteBulk(batch)
		}
	})

	b.Run("delete_bulk_50", func(b *testing.B) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(b.Context(), buckets)
		defer d.Close()

		b.ResetTimer()
		for range b.N {
			batch := make([]*typedef.Values, 50)
			for i := range batch {
				batch[i] = typedef.NewValues(1)
			}
			d.DeleteBulk(batch)
		}
	})

	b.Run("delete_bulk_100", func(b *testing.B) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(b.Context(), buckets)
		defer d.Close()

		b.ResetTimer()
		for range b.N {
			batch := make([]*typedef.Values, 100)
			for i := range batch {
				batch[i] = typedef.NewValues(1)
			}
			d.DeleteBulk(batch)
		}
	})
}

// BenchmarkFastPath tests the atomic fast-path optimization
func BenchmarkFastPath(b *testing.B) {
	b.Run("fast_path_check", func(b *testing.B) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(b.Context(), buckets)
		defer d.Close()

		// Add one item with future ready time
		d.Delete(typedef.NewValues(1))
		time.Sleep(10 * time.Millisecond) // Let it settle

		b.ResetTimer()
		for range b.N {
			// This should hit the fast path (no lock needed)
			_ = d.nextReadyNs.Load()
		}
	})

	b.Run("process_ready_fast_path", func(b *testing.B) {
		buckets := []time.Duration{1 * time.Second} // Far in future
		d := newDeleted(b.Context(), buckets)
		defer d.Close()

		// Add items that won't be ready
		for range 100 {
			d.Delete(typedef.NewValues(1))
		}
		time.Sleep(10 * time.Millisecond)

		b.ResetTimer()
		for range b.N {
			// Should hit fast path and return quickly
			d.processReady()
		}
	})
}

// BenchmarkHeapShrinking tests the memory shrinking optimization
func BenchmarkHeapShrinking(b *testing.B) {
	b.Run("shrink_after_spike", func(b *testing.B) {
		buckets := []time.Duration{1 * time.Millisecond}
		d := newDeleted(b.Context(), buckets)
		defer d.Close()

		b.ResetTimer()
		for range b.N {
			// Simulate spike
			for range 5000 {
				d.Delete(typedef.NewValues(1))
			}

			// Wait for processing
			time.Sleep(100 * time.Millisecond)

			// Trigger shrink check
			d.heap.shrinkIfNeeded()
		}
	})
}

// BenchmarkUnixNanoComparison tests time comparison optimization
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

// BenchmarkConcurrentBulk tests concurrent bulk operations
func BenchmarkConcurrentBulk(b *testing.B) {
	b.Run("concurrent_bulk_delete", func(b *testing.B) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(b.Context(), buckets)
		defer d.Close()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				batch := make([]*typedef.Values, 10)
				for i := range batch {
					batch[i] = typedef.NewValues(1)
				}
				d.DeleteBulk(batch)
			}
		})
	})
}
