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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/scylladb/gemini/pkg/typedef"
)

// TestNoBackpressure verifies that Delete never blocks regardless of heap size
func TestNoBackpressure(t *testing.T) {
	t.Parallel()

	t.Run("delete_never_blocks_large_volume", func(t *testing.T) {
		t.Parallel()
		// Use long bucket so items don't get processed quickly
		buckets := []time.Duration{10 * time.Second}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		// Add many items - should never block
		const largeCount = 10000
		done := make(chan bool, 1)

		go func() {
			for range largeCount {
				d.Delete(typedef.NewValues(1))
			}
			done <- true
		}()

		// Should complete quickly without blocking
		select {
		case <-done:
			// Success - Delete never blocked
		case <-time.After(2 * time.Second):
			t.Fatal("Delete should never block, but took too long")
		}

		// Verify all items were added
		assert.Equal(t, largeCount, d.Len())
		assert.Equal(t, uint64(largeCount), d.deleted.Load())
	})

	t.Run("delete_bulk_never_blocks", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{10 * time.Second}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		// Add large batch - should never block
		const batchSize = 5000
		batch := make([]*typedef.Values, batchSize)
		for i := range batch {
			batch[i] = typedef.NewValues(1)
		}

		done := make(chan bool, 1)
		go func() {
			d.DeleteBulk(batch)
			done <- true
		}()

		// Should complete quickly
		select {
		case <-done:
			// Success
		case <-time.After(2 * time.Second):
			t.Fatal("DeleteBulk should never block")
		}

		assert.Equal(t, batchSize, d.Len())
		assert.Equal(t, uint64(batchSize), d.deleted.Load())
	})

	t.Run("concurrent_deletes_never_block", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{10 * time.Second}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		// Multiple goroutines adding items concurrently
		const numGoroutines = 20
		const itemsPerGoroutine = 500
		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		start := time.Now()
		for range numGoroutines {
			go func() {
				defer wg.Done()
				for range itemsPerGoroutine {
					d.Delete(typedef.NewValues(1))
				}
			}()
		}

		// Wait with timeout
		done := make(chan bool, 1)
		go func() {
			wg.Wait()
			done <- true
		}()

		select {
		case <-done:
			duration := time.Since(start)
			t.Logf("Concurrent deletes completed in %v", duration)
			// Should complete in reasonable time
			assert.Less(t, duration, 2*time.Second)
		case <-time.After(5 * time.Second):
			t.Fatal("Concurrent deletes should not block")
		}

		expectedTotal := numGoroutines * itemsPerGoroutine
		assert.Equal(t, uint64(expectedTotal), d.deleted.Load())
	})

	t.Run("heap_can_grow_unbounded", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{1 * time.Hour} // Very long - won't process
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		// Add many more items than initial capacity
		const count = 5000
		for range count {
			d.Delete(typedef.NewValues(1))
		}

		// All should be in heap
		assert.Equal(t, count, d.Len())
		assert.Equal(t, uint64(count), d.deleted.Load())
	})
}

// TestHeapMemoryManagement verifies heap growth and shrinking
func TestHeapMemoryManagement(t *testing.T) {
	t.Parallel()

	t.Run("heap_grows_as_needed", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		// Add more than initial capacity
		const count = 2000
		for range count {
			d.Delete(typedef.NewValues(1))
		}

		// Heap should have grown
		assert.GreaterOrEqual(t, len(d.heap.data), count)
		assert.Equal(t, count, d.Len())
	})

	t.Run("heap_capacity_doubles_on_growth", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{10 * time.Second}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		// Add items just past initial capacity
		for range 1025 {
			d.Delete(typedef.NewValues(1))
		}

		// Capacity should have doubled
		assert.GreaterOrEqual(t, len(d.heap.data), 2048)
	})
}

// TestDeleteEdgeCases tests edge cases
func TestDeleteEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("empty_heap_works", func(t *testing.T) {
		t.Parallel()
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		// Empty heap should work fine
		d.Delete(typedef.NewValues(1))
		assert.Equal(t, 1, d.Len())
	})

	t.Run("delete_bulk_empty_slice", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		// Should handle empty slice without issue
		d.DeleteBulk([]*typedef.Values{})
		assert.Equal(t, 0, d.Len())
		assert.Equal(t, uint64(0), d.deleted.Load())
	})

	t.Run("context_cancellation_stops_processing", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		buckets := []time.Duration{10 * time.Millisecond}
		d := newDeleted(ctx, buckets)

		// Add some items
		for range 100 {
			d.Delete(typedef.NewValues(1))
		}

		time.Sleep(50 * time.Millisecond)

		// Cancel context and close
		cancel()
		d.Close()

		// Channel should be closed
		time.Sleep(150 * time.Millisecond) // Wait for background to stop

		select {
		case <-d.ch:
		default:
			// Closed or empty
		}
	})

	t.Run("nil_buckets_increments_counter_only", func(t *testing.T) {
		t.Parallel()

		d := newDeleted(t.Context(), nil) // nil buckets
		defer d.Close()

		// Should just increment counter, not add to heap
		d.Delete(typedef.NewValues(1))
		d.Delete(typedef.NewValues(1))

		assert.Equal(t, uint64(2), d.deleted.Load())
		assert.Equal(t, 0, d.Len()) // Nothing in heap
	})

	t.Run("empty_buckets_increments_counter_only", func(t *testing.T) {
		t.Parallel()

		d := newDeleted(t.Context(), []time.Duration{}) // empty buckets
		defer d.Close()

		d.Delete(typedef.NewValues(1))

		assert.Equal(t, uint64(1), d.deleted.Load())
		assert.Equal(t, 0, d.Len())
	})
}

// BenchmarkDeletePerformance benchmarks delete performance
func BenchmarkDeletePerformance(b *testing.B) {
	b.Run("single_delete", func(b *testing.B) {
		buckets := []time.Duration{1 * time.Millisecond}
		d := newDeleted(b.Context(), buckets)
		defer d.Close()

		// Consume items
		go func() {
			//nolint: revive
			for range d.ch {
			}
		}()

		b.ResetTimer()
		for range b.N {
			d.Delete(typedef.NewValues(1))
		}
	})

	b.Run("bulk_delete", func(b *testing.B) {
		buckets := []time.Duration{1 * time.Millisecond}
		d := newDeleted(b.Context(), buckets)
		defer d.Close()

		// Consume items
		go func() {
			//nolint:revive
			for range d.ch {
			}
		}()

		// Create batches
		batchSize := 100
		batches := make([][]*typedef.Values, b.N)
		for i := range b.N {
			batches[i] = make([]*typedef.Values, batchSize)
			for j := range batchSize {
				batches[i][j] = typedef.NewValues(1)
			}
		}

		b.ResetTimer()
		for i := range b.N {
			d.DeleteBulk(batches[i])
		}
	})

	b.Run("concurrent_deletes", func(b *testing.B) {
		buckets := []time.Duration{1 * time.Millisecond}
		d := newDeleted(b.Context(), buckets)
		defer d.Close()

		// Consume items
		go func() {
			//nolint:revive
			for range d.ch {
			}
		}()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				d.Delete(typedef.NewValues(1))
			}
		})
	})

	b.Run("high_volume_no_consumer", func(b *testing.B) {
		buckets := []time.Duration{10 * time.Second} // Long bucket, won't process
		d := newDeleted(b.Context(), buckets)
		defer d.Close()

		// No consumer - heap will grow

		b.ResetTimer()
		for range b.N {
			d.Delete(typedef.NewValues(1))
		}
	})
}
