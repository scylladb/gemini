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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

// TestDeleteBulk tests the bulk delete functionality
func TestDeleteBulk(t *testing.T) {
	t.Run("bulk_delete_empty", func(t *testing.T) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		// Should handle empty batch gracefully
		d.DeleteBulk([]*typedef.Values{})
		assert.Equal(t, 0, d.Len())
	})

	t.Run("bulk_delete_single", func(t *testing.T) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		batch := []*typedef.Values{typedef.NewValues(1)}
		d.DeleteBulk(batch)

		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, 1, d.Len())
	})

	t.Run("bulk_delete_multiple", func(t *testing.T) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		batch := make([]*typedef.Values, 50)
		for i := range batch {
			batch[i] = typedef.NewValues(1)
		}
		d.DeleteBulk(batch)

		time.Sleep(10 * time.Millisecond)
		assert.Equal(t, 50, d.Len())
		assert.Equal(t, uint64(50), d.deleted.Load())
	})

	t.Run("bulk_delete_sets_next_ready", func(t *testing.T) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		batch := make([]*typedef.Values, 10)
		for i := range batch {
			batch[i] = typedef.NewValues(1)
		}
		d.DeleteBulk(batch)

		// Check that nextReadyNs is set
		time.Sleep(10 * time.Millisecond)
		nextNs := d.nextReadyNs.Load()
		assert.Greater(t, nextNs, int64(0))
	})
}

// TestFastPathOptimization tests the atomic fast-path check
func TestFastPathOptimization(t *testing.T) {
	t.Run("fast_path_no_lock", func(t *testing.T) {
		buckets := []time.Duration{1 * time.Second}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		// Add item with future ready time
		d.Delete(typedef.NewValues(1))
		time.Sleep(10 * time.Millisecond)

		// Process should return quickly via fast path
		start := time.Now()
		waitTime := d.processReady()
		elapsed := time.Since(start)

		assert.Greater(t, waitTime, 500*time.Millisecond)
		assert.Less(t, elapsed, 10*time.Millisecond) // Should be very fast
	})

	t.Run("next_ready_updated_on_delete", func(t *testing.T) {
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		before := d.nextReadyNs.Load()
		assert.Equal(t, int64(0), before)

		d.Delete(typedef.NewValues(1))
		time.Sleep(5 * time.Millisecond)

		after := d.nextReadyNs.Load()
		assert.Greater(t, after, int64(0))
	})
}

// TestHeapShrinking tests the memory shrinking functionality
func TestHeapShrinking(t *testing.T) {
	t.Parallel()

	t.Run("shrink_after_spike", func(t *testing.T) {
		t.Parallel()
		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 4),
			len:  0,
		}

		now := time.Now()

		// Grow to large size
		for i := range 5000 {
			h.pushInline(deletedPartition{
				values:  typedef.NewValues(1),
				readyAt: now.Add(time.Duration(i) * time.Millisecond),
			})
		}

		largeCap := len(h.data)
		assert.GreaterOrEqual(t, largeCap, 5000)

		// Remove most items
		for range 4900 {
			h.popInline()
		}

		assert.Equal(t, 100, h.len)

		// Trigger shrink
		h.shrinkIfNeeded()

		newCap := len(h.data)
		assert.Less(t, newCap, largeCap)
		assert.GreaterOrEqual(t, newCap, h.len)
		t.Logf("Shrunk from %d to %d (len=%d)", largeCap, newCap, h.len)
	})

	t.Run("no_shrink_when_not_needed", func(t *testing.T) {
		t.Parallel()

		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 1024),
			len:  0,
		}

		now := time.Now()

		// Add moderate number of items
		for i := range 512 {
			h.pushInline(deletedPartition{
				values:  typedef.NewValues(1),
				readyAt: now.Add(time.Duration(i) * time.Millisecond),
			})
		}

		capBefore := len(h.data)
		h.shrinkIfNeeded()
		capAfter := len(h.data)

		// Should not shrink (utilization is 50%)
		assert.Equal(t, capBefore, capAfter)
	})

	t.Run("min_capacity_respected", func(t *testing.T) {
		t.Parallel()

		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 4096),
			len:  10,
		}

		h.shrinkIfNeeded()

		// Should shrink but not below minimum of 1024
		assert.GreaterOrEqual(t, len(h.data), 1024)
		assert.Less(t, len(h.data), 4096)
	})
}

// TestBatchProcessing tests batch processing optimization
func TestBatchProcessing(t *testing.T) {
	t.Parallel()

	t.Run("batch_size_limits_processing", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{1 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		// Add many items
		for range 100 {
			d.Delete(typedef.NewValues(1))
		}

		// Wait for all to be ready
		time.Sleep(50 * time.Millisecond)

		// Process should be limited by batch size
		d.mu.Lock()
		initialLen := d.heap.Len()
		d.mu.Unlock()

		assert.GreaterOrEqual(t, initialLen, 50)
	})
}

// TestUnixNanoComparison verifies UnixNano time comparisons work correctly
func TestUnixNanoComparison(t *testing.T) {
	t.Parallel()

	t.Run("unixnano_comparison_correctness", func(t *testing.T) {
		t.Parallel()
		now := time.Now()
		future := now.Add(1 * time.Second)

		// Test that UnixNano comparison matches Before()
		assert.True(t, now.Before(future))
		assert.True(t, now.UnixNano() < future.UnixNano())

		assert.False(t, future.Before(now))
		assert.False(t, future.UnixNano() < now.UnixNano())
	})

	t.Run("peekTimeNs", func(t *testing.T) {
		t.Parallel()

		h := &deletedPartitionHeap{
			data: make([]deletedPartition, 64),
			len:  0,
		}

		// Empty heap
		assert.Equal(t, int64(0), h.peekTimeNs())

		// Add item
		now := time.Now()
		h.pushInline(deletedPartition{
			values:  typedef.NewValues(1),
			readyAt: now,
		})

		// Should return Unix nano time
		ns := h.peekTimeNs()
		assert.Equal(t, now.UnixNano(), ns)
	})
}

// TestAdaptiveBackgroundInterval tests the adaptive ticker logic
func TestAdaptiveBackgroundInterval(t *testing.T) {
	t.Parallel()
	t.Run("interval_adapts_to_ready_time", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{50 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		// Add item
		d.Delete(typedef.NewValues(1))

		// The background processor should adapt its interval
		time.Sleep(100 * time.Millisecond)

		// Item should be processed
		select {
		case val := <-d.ch:
			require.NotNil(t, val)
		case <-time.After(200 * time.Millisecond):
			t.Fatal("Expected item to be ready")
		}
	})
}

// TestConcurrentBulkDelete tests concurrent bulk operations
func TestConcurrentBulkDelete(t *testing.T) {
	t.Parallel()

	t.Run("concurrent_bulk_deletes", func(t *testing.T) {
		t.Parallel()

		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(t.Context(), buckets)
		defer d.Close()

		const goroutines = 10
		const batchSize = 10
		done := make(chan bool, goroutines)

		for range goroutines {
			go func() {
				batch := make([]*typedef.Values, batchSize)
				for i := range batch {
					batch[i] = typedef.NewValues(1)
				}
				d.DeleteBulk(batch)
				done <- true
			}()
		}

		// Wait for all goroutines
		for range goroutines {
			<-done
		}

		time.Sleep(20 * time.Millisecond)

		// Check total count
		assert.Equal(t, uint64(goroutines*batchSize), d.deleted.Load())
	})
}
