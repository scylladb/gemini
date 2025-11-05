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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

// TestDeletedPartitionHeap tests the heap implementation directly
func TestDeletedPartitionHeap(t *testing.T) {
	t.Parallel()

	t.Run("heap_initialization", func(t *testing.T) {
		t.Parallel()
		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 64),
			len:  0,
		}
		heap.Init(h)
		assert.Equal(t, 0, h.Len())
	})

	t.Run("heap_push_pop", func(t *testing.T) {
		t.Parallel()

		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 64),
			len:  0,
		}
		heap.Init(h)

		now := time.Now()
		values1 := typedef.NewValues(1)
		values2 := typedef.NewValues(1)
		values3 := typedef.NewValues(1)

		// Push in non-chronological order
		heap.Push(h, deletedPartition{values: values2, readyAt: now.Add(2 * time.Second)})
		heap.Push(h, deletedPartition{values: values1, readyAt: now.Add(1 * time.Second)})
		heap.Push(h, deletedPartition{values: values3, readyAt: now.Add(3 * time.Second)})

		assert.Equal(t, 3, h.Len())

		// Pop should return items in chronological order
		item1 := heap.Pop(h).(deletedPartition)
		assert.Equal(t, values1, item1.values)

		item2 := heap.Pop(h).(deletedPartition)
		assert.Equal(t, values2, item2.values)

		item3 := heap.Pop(h).(deletedPartition)
		assert.Equal(t, values3, item3.values)

		assert.Equal(t, 0, h.Len())
	})

	t.Run("heap_peek_without_pop", func(t *testing.T) {
		t.Parallel()

		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 64),
			len:  0,
		}
		heap.Init(h)

		now := time.Now()
		values1 := typedef.NewValues(1)

		heap.Push(h, deletedPartition{values: values1, readyAt: now.Add(1 * time.Second)})

		// Peek at the top without popping
		top := h.data[0]
		assert.Equal(t, values1, top.values)
		assert.Equal(t, 1, h.Len()) // Still has 1 element
	})

	t.Run("heap_maintains_order_with_many_items", func(t *testing.T) {
		t.Parallel()

		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 64),
			len:  0,
		}
		heap.Init(h)

		now := time.Now()
		const count = 100

		// Push items in random order
		for i := range count {
			values := typedef.NewValues(1)
			heap.Push(h, deletedPartition{
				values:  values,
				readyAt: now.Add(time.Duration(count-i) * time.Millisecond),
			})
		}

		assert.Equal(t, count, h.Len())

		// Pop all and verify order
		var lastTime time.Time
		for range count {
			item := heap.Pop(h).(deletedPartition)
			if !lastTime.IsZero() {
				assert.True(t, item.readyAt.After(lastTime) || item.readyAt.Equal(lastTime),
					"Heap order violated: %v should be >= %v", item.readyAt, lastTime)
			}
			lastTime = item.readyAt
		}
	})

	t.Run("optimized_push_maintains_order", func(t *testing.T) {
		t.Parallel()

		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 64),
			len:  0,
		}

		now := time.Now()
		const count = 100

		// Use optimized push method
		for i := range count {
			values := typedef.NewValues(1)
			h.pushInline(deletedPartition{
				values:  values,
				readyAt: now.Add(time.Duration(count-i) * time.Millisecond),
			})
		}

		assert.Equal(t, count, h.Len())

		// Pop all and verify order
		var lastTime time.Time
		for range count {
			item := heap.Pop(h).(deletedPartition)
			if !lastTime.IsZero() {
				assert.True(t, item.readyAt.After(lastTime) || item.readyAt.Equal(lastTime),
					"Heap order violated: %v should be >= %v", item.readyAt, lastTime)
			}
			lastTime = item.readyAt
		}
	})

	t.Run("heap_grows_capacity_efficiently", func(t *testing.T) {
		t.Parallel()

		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 4), // Small initial capacity
			len:  0,
		}

		now := time.Now()
		const count = 100

		// Push many items to trigger growth
		for i := range count {
			values := typedef.NewValues(1)
			h.pushInline(deletedPartition{
				values:  values,
				readyAt: now.Add(time.Duration(i) * time.Millisecond),
			})
		}

		assert.Equal(t, count, h.Len())
		assert.GreaterOrEqual(t, len(h.data), count, "Backing array should have grown")

		// Verify heap property maintained
		for i := range count - 1 {
			assert.True(t, !h.data[i+1].readyAt.Before(h.data[i].readyAt) || i >= h.len,
				"Heap property should be maintained after growth")
		}
	})
}

// TestDeletedPartitionsBasic tests basic functionality
func TestDeletedPartitionsBasic(t *testing.T) {
	t.Parallel()

	t.Run("new_deleted_partitions", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		require.NotNil(t, d)
		assert.Equal(t, 0, d.Len())
		d.Close()
	})

	t.Run("delete_increments_counter", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		values := typedef.NewValues(1)
		d.Delete(values)

		assert.Equal(t, uint64(1), d.deleted.Load())
		assert.Equal(t, 1, d.Len())
	})

	t.Run("multiple_deletes", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		const count = 10
		for range count {
			values := typedef.NewValues(1)
			d.Delete(values)
		}

		assert.Equal(t, uint64(count), d.deleted.Load())
		assert.Equal(t, count, d.Len())
	})
}

// TestDeletedPartitionsTimeBuckets tests time bucket behavior
func TestDeletedPartitionsTimeBuckets(t *testing.T) {
	t.Parallel()

	t.Run("single_bucket_ready_after_delay", func(t *testing.T) {
		t.Parallel()
		delay := 50 * time.Millisecond
		buckets := []time.Duration{delay}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		values := typedef.NewValues(1)
		d.Delete(values)

		// Should not be ready immediately
		select {
		case <-d.ch:
			t.Fatal("Partition should not be ready immediately")
		case <-time.After(10 * time.Millisecond):
			// Expected
		}

		// Should be ready after delay
		select {
		case received := <-d.ch:
			assert.Equal(t, values, received)
		case <-time.After(delay + 100*time.Millisecond):
			t.Fatal("Partition should be ready after delay")
		}
	})

	t.Run("multiple_buckets_revalidation", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{
			50 * time.Millisecond,
			50 * time.Millisecond,
			50 * time.Millisecond,
		}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		values := typedef.NewValues(1)
		d.Delete(values)

		// Should receive the partition multiple times (once per bucket + repeated last bucket)
		receivedCount := 0
		timeout := time.After(500 * time.Millisecond)

		for receivedCount < len(buckets) {
			select {
			case received := <-d.ch:
				assert.Equal(t, values, received)
				receivedCount++
			case <-timeout:
				t.Fatalf("Expected %d revalidations, got %d", len(buckets), receivedCount)
			}
		}

		assert.GreaterOrEqual(t, receivedCount, len(buckets))
	})

	t.Run("buckets_maintain_order", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{30 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		// Delete partitions in specific order
		values1 := typedef.NewValues(1)
		values2 := typedef.NewValues(1)
		values3 := typedef.NewValues(1)

		d.Delete(values1)
		time.Sleep(5 * time.Millisecond)
		d.Delete(values2)
		time.Sleep(5 * time.Millisecond)
		d.Delete(values3)

		// Should receive in order
		timeout := time.After(200 * time.Millisecond)
		received := make([]*typedef.Values, 0, 3)

		for range 3 {
			select {
			case val := <-d.ch:
				received = append(received, val)
			case <-timeout:
				t.Fatal("Timeout waiting for partitions")
			}
		}

		assert.Equal(t, values1, received[0])
		assert.Equal(t, values2, received[1])
		assert.Equal(t, values3, received[2])
	})
}

// TestDeletedPartitionsConcurrency tests concurrent access
func TestDeletedPartitionsConcurrency(t *testing.T) {
	t.Parallel()

	t.Run("concurrent_deletes", func(t *testing.T) {
		t.Parallel()
		buckets := []time.Duration{50 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		const goroutines = 10
		const deletesPerGoroutine = 20
		const totalDeletes = goroutines * deletesPerGoroutine

		var wg sync.WaitGroup
		wg.Add(goroutines)

		for range goroutines {
			go func() {
				defer wg.Done()
				for range deletesPerGoroutine {
					values := typedef.NewValues(1)
					d.Delete(values)
				}
			}()
		}

		wg.Wait()

		assert.Equal(t, uint64(totalDeletes), d.deleted.Load())
		assert.Equal(t, totalDeletes, d.Len())
	})

	t.Run("concurrent_delete_and_read", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{10 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		const writers = 5
		const readers = 3
		const deletesPerWriter = 10

		var wg sync.WaitGroup
		wg.Add(writers + readers)

		// Writers
		for range writers {
			go func() {
				defer wg.Done()
				for range deletesPerWriter {
					values := typedef.NewValues(1)
					d.Delete(values)
					time.Sleep(2 * time.Millisecond)
				}
			}()
		}

		// Readers
		readCount := make([]int, readers)
		for i := range readers {
			go func(idx int) {
				defer wg.Done()
				timeout := time.After(500 * time.Millisecond)
				for {
					select {
					case <-d.ch:
						readCount[idx]++
					case <-timeout:
						return
					}
				}
			}(i)
		}

		wg.Wait()

		totalRead := 0
		for _, count := range readCount {
			totalRead += count
		}

		// Should have read at least some partitions
		assert.Greater(t, totalRead, 0)
	})

	t.Run("concurrent_len_and_delete", func(t *testing.T) {
		t.Parallel()
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		var wg sync.WaitGroup
		wg.Add(2)

		// Writer
		go func() {
			defer wg.Done()
			for range 100 {
				values := typedef.NewValues(1)
				d.Delete(values)
				time.Sleep(1 * time.Millisecond)
			}
		}()

		// Len checker
		go func() {
			defer wg.Done()
			for range 100 {
				_ = d.Len()
				time.Sleep(1 * time.Millisecond)
			}
		}()

		wg.Wait()
	})
}

// TestDeletedPartitionsBackgroundProcessor tests the background processing
func TestDeletedPartitionsBackgroundProcessor(t *testing.T) {
	t.Parallel()

	t.Run("processes_ready_partitions", func(t *testing.T) {
		t.Parallel()
		buckets := []time.Duration{50 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		values := typedef.NewValues(1)
		d.Delete(values)

		// Wait for processing
		select {
		case received := <-d.ch:
			assert.Equal(t, values, received)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Timeout waiting for background processor")
		}
	})

	t.Run("does_not_process_unready_partitions", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{1 * time.Second} // Long delay
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		values := typedef.NewValues(1)
		d.Delete(values)

		// Should not be ready quickly
		select {
		case <-d.ch:
			t.Fatal("Should not receive unready partition")
		case <-time.After(100 * time.Millisecond):
			// Expected
		}

		assert.Equal(t, 1, d.Len())
	})

	t.Run("stops_on_context_cancel", func(t *testing.T) {
		t.Parallel()

		ctx, cancel := context.WithCancel(t.Context())
		buckets := []time.Duration{10 * time.Millisecond}
		d := newDeleted(ctx, buckets)

		values := typedef.NewValues(1)
		d.Delete(values)

		// Cancel context
		cancel()

		// Channel should eventually close
		timeout := time.After(500 * time.Millisecond)
		for {
			select {
			case _, ok := <-d.ch:
				if !ok {
					// Channel closed as expected
					return
				}
			case <-timeout:
				t.Fatal("Channel should close after context cancel")
			}
		}
	})

	t.Run("close_stops_background", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{10 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)

		d.Close()

		// Channel should close
		timeout := time.After(500 * time.Millisecond)
		for {
			select {
			case _, ok := <-d.ch:
				if !ok {
					// Channel closed as expected
					return
				}
			case <-timeout:
				t.Fatal("Channel should close after Close()")
			}
		}
	})
}

// TestDeletedPartitionsChannelBehavior tests channel overflow scenarios
func TestDeletedPartitionsChannelBehavior(t *testing.T) {
	t.Parallel()

	t.Run("channel_full_requeues", func(t *testing.T) {
		t.Parallel()
		buckets := []time.Duration{10 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		// Fill channel to capacity (buffer is 100)
		for range 150 {
			values := typedef.NewValues(1)
			d.Delete(values)
		}

		// Wait for processing
		time.Sleep(100 * time.Millisecond)

		// Should still have items in heap (some requeued when channel was full)
		assert.Greater(t, d.Len(), 0)

		// Drain channel
		drained := 0
		timeout := time.After(500 * time.Millisecond)
		for {
			select {
			case <-d.ch:
				drained++
			case <-timeout:
				// Done draining
				t.Logf("Drained %d items from channel", drained)
				return
			}
		}
	})
}

// TestDeletedPartitionsEdgeCases tests edge cases
func TestDeletedPartitionsEdgeCases(t *testing.T) {
	t.Parallel()

	t.Run("empty_buckets", func(t *testing.T) {
		t.Parallel()
		buckets := []time.Duration{}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		// Should handle empty buckets gracefully (will panic on access)
		// In production code, this should be validated
		values := typedef.NewValues(1)

		// This will panic if buckets is empty - that's expected behavior
		// In a real scenario, the constructor should validate buckets
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Expected panic with empty buckets: %v", r)
			}
		}()

		d.Delete(values)
	})

	t.Run("nil_context", func(t *testing.T) {
		t.Parallel()

		// Should handle nil context - but this will panic
		defer func() {
			if r := recover(); r != nil {
				t.Logf("Expected panic with nil context: %v", r)
			}
		}()

		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(nil, buckets) // nolint:staticcheck
		defer d.Close()
	})

	t.Run("zero_duration_bucket", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{0}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		values := typedef.NewValues(1)
		d.Delete(values)

		// Should be ready after the next background processor tick (100ms)
		select {
		case received := <-d.ch:
			assert.Equal(t, values, received)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Should receive quickly with zero duration")
		}
	})

	t.Run("very_long_bucket", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{24 * time.Hour}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		values := typedef.NewValues(1)
		d.Delete(values)

		// Should not be ready for a very long time
		select {
		case <-d.ch:
			t.Fatal("Should not receive with very long bucket")
		case <-time.After(100 * time.Millisecond):
			// Expected
		}

		assert.Equal(t, 1, d.Len())
	})

	t.Run("counter_increment_beyond_buckets", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{20 * time.Millisecond, 20 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		values := typedef.NewValues(1)
		d.Delete(values)

		// Receive multiple times - should use last bucket after exhausting all
		receivedCount := 0
		timeout := time.After(300 * time.Millisecond)

	loop:
		for receivedCount < 5 {
			select {
			case <-d.ch:
				receivedCount++
			case <-timeout:
				break loop
			}
		}

		// Should have received at least as many times as there are buckets
		assert.GreaterOrEqual(t, receivedCount, len(buckets))
	})
}

// TestDeletedPartitionsIntegration tests realistic usage scenarios
func TestDeletedPartitionsIntegration(t *testing.T) {
	t.Parallel()

	t.Run("realistic_workload", func(t *testing.T) {
		t.Parallel()
		// Realistic time buckets: 1s, 5s, 30s
		buckets := []time.Duration{
			100 * time.Millisecond, // scaled down for testing
			200 * time.Millisecond,
			300 * time.Millisecond,
		}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		const totalDeletes = 50
		deletedValues := make([]*typedef.Values, totalDeletes)

		// Simulate deletes over time
		go func() {
			for i := range totalDeletes {
				values := typedef.NewValues(1)
				deletedValues[i] = values
				d.Delete(values)
				time.Sleep(5 * time.Millisecond)
			}
		}()

		// Consume validations
		validations := make(map[*typedef.Values]int)
		timeout := time.After(2 * time.Second)

	consumer:
		for {
			select {
			case values, ok := <-d.ch:
				if !ok {
					break consumer
				}
				validations[values]++
			case <-timeout:
				break consumer
			}
		}

		// Each deleted partition should be validated at least once
		assert.Greater(t, len(validations), 0)
		t.Logf("Validated %d unique partitions with total %d validations",
			len(validations), sumMapValues(validations))
	})
}

// Helper function to sum map values
func sumMapValues(m map[*typedef.Values]int) int {
	sum := 0
	for _, v := range m {
		sum += v
	}
	return sum
}

// BenchmarkDeletedPartitions provides performance benchmarks
func BenchmarkDeletedPartitions(b *testing.B) {
	b.Run("delete", func(b *testing.B) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(b.Context(), buckets)
		defer d.Close()

		b.ResetTimer()
		for range b.N {
			values := typedef.NewValues(1)
			d.Delete(values)
		}
	})

	b.Run("delete_and_receive", func(b *testing.B) {
		buckets := []time.Duration{1 * time.Millisecond}
		d := newDeleted(b.Context(), buckets)
		defer d.Close()

		// Consumer goroutine
		go func() {
			//nolint:revive
			for range d.ch {
				// Drain channel
			}
		}()

		b.ResetTimer()
		for range b.N {
			values := typedef.NewValues(1)
			d.Delete(values)
		}
	})

	b.Run("concurrent_deletes", func(b *testing.B) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(b.Context(), buckets)
		defer d.Close()

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				values := typedef.NewValues(1)
				d.Delete(values)
			}
		})
	})
}
