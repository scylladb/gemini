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
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

// TestDeleteBulk tests the bulk delete functionality
func TestDeleteBulk(t *testing.T) {
	t.Parallel()

	t.Run("bulk_delete_empty", func(t *testing.T) {
		t.Parallel()
		buckets := []time.Duration{100 * time.Millisecond}
		d := newDeleted(t.Context(), buckets, 0)
		defer d.Close()

		// Should handle empty batch gracefully
		d.DeleteBulk([]typedef.PartitionKeys{})
		assert.Equal(t, 0, d.Len())
	})

	t.Run("bulk_delete_single", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			buckets := []time.Duration{100 * time.Millisecond}
			d := newDeleted(t.Context(), buckets, 0)
			defer d.Close()

			batch := []typedef.PartitionKeys{{Values: typedef.NewValues(1)}}
			d.DeleteBulk(batch)

			time.Sleep(10 * time.Millisecond)
			assert.Equal(t, 1, d.Len())
		})
	})

	t.Run("bulk_delete_multiple", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			buckets := []time.Duration{100 * time.Millisecond}
			d := newDeleted(t.Context(), buckets, 0)
			defer d.Close()

			batch := make([]typedef.PartitionKeys, 50)
			for i := range batch {
				batch[i] = typedef.PartitionKeys{Values: typedef.NewValues(1)}
			}
			d.DeleteBulk(batch)

			time.Sleep(10 * time.Millisecond)
			assert.Equal(t, 50, d.Len())
			assert.Equal(t, uint64(50), d.deleted.Load())
		})
	})

	t.Run("bulk_delete_sets_next_ready", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			buckets := []time.Duration{100 * time.Millisecond}
			d := newDeleted(t.Context(), buckets, 0)
			defer d.Close()

			batch := make([]typedef.PartitionKeys, 10)
			for i := range batch {
				batch[i] = typedef.PartitionKeys{Values: typedef.NewValues(1)}
			}
			d.DeleteBulk(batch)

			// Check that nextReadyNs is set
			time.Sleep(10 * time.Millisecond)
			nextNs := d.win.nextReadyNs()
			assert.Greater(t, nextNs, int64(0))
		})
	})

	t.Run("bulk_delete_stamps_DeletedAtNS", func(t *testing.T) {
		t.Parallel()
		before := time.Now().UnixNano()
		buckets := []time.Duration{1 * time.Second}     // long bucket so items stay in heap
		d := newDeleted(t.Context(), buckets, 0, false) // false = don't start background goroutine
		defer d.Close()

		batch := make([]typedef.PartitionKeys, 5)
		for i := range batch {
			batch[i] = typedef.PartitionKeys{Values: typedef.NewValues(1)}
		}
		d.DeleteBulk(batch)
		after := time.Now().UnixNano()

		// Verify the original slice elements are stamped (not just the heap copies).
		for i := range batch {
			ns := int64(batch[i].DeletedAtNS)
			assert.Greater(t, ns, before, "batch[%d].DeletedAtNS should be ≥ before", i)
			assert.LessOrEqual(t, ns, after, "batch[%d].DeletedAtNS should be ≤ after", i)
		}

		require.Equal(t, 5, d.Len())
		for i, e := range d.entriesForTest() {
			ns := int64(e.keys.DeletedAtNS)
			assert.Greater(t, ns, before, "entry[%d].keys.DeletedAtNS should be ≥ before", i)
			assert.LessOrEqual(t, ns, after, "entry[%d].keys.DeletedAtNS should be ≤ after", i)
		}
	})
}

func TestDelete_DeletedAtNS(t *testing.T) {
	t.Parallel()
	before := time.Now().UnixNano()
	buckets := []time.Duration{1 * time.Second}     // long bucket so item stays in heap
	d := newDeleted(t.Context(), buckets, 0, false) // false = don't start background goroutine
	defer d.Close()

	d.Delete(typedef.PartitionKeys{Values: typedef.NewValues(1)})
	after := time.Now().UnixNano()

	require.Equal(t, 1, d.Len())
	e, ok := d.peekSoonestForTest()
	require.True(t, ok)
	ns := int64(e.keys.DeletedAtNS)
	assert.Greater(t, ns, before, "DeletedAtNS should be ≥ before Delete call")
	assert.LessOrEqual(t, ns, after, "DeletedAtNS should be ≤ after Delete call")
}

// TestFastPathOptimization tests the atomic fast-path check
func TestFastPathOptimization(t *testing.T) {
	t.Run("fast_path_no_lock", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			buckets := []time.Duration{1 * time.Second}
			d := newDeleted(t.Context(), buckets, 0)
			defer d.Close()

			// Add item with future ready time
			d.Delete(typedef.PartitionKeys{Values: typedef.NewValues(1)})
			time.Sleep(10 * time.Millisecond)

			// Process should return quickly via fast path
			start := time.Now()
			waitTime := d.processReady()
			elapsed := time.Since(start)

			assert.Greater(t, waitTime, 500*time.Millisecond)
			assert.Less(t, elapsed, 10*time.Millisecond) // Should be very fast
		})
	})

	t.Run("next_ready_updated_on_delete", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			buckets := []time.Duration{100 * time.Millisecond}
			d := newDeleted(t.Context(), buckets, 0)
			defer d.Close()

			before := d.win.nextReadyNs()
			assert.Equal(t, int64(0), before)

			d.Delete(typedef.PartitionKeys{Values: typedef.NewValues(1)})
			time.Sleep(5 * time.Millisecond)

			after := d.win.nextReadyNs()
			assert.Greater(t, after, int64(0))
		})
	})
}

// Ring growth/shrink mechanics are covered by TestRingBuf_GrowAndShrink in
// window_test.go.

// TestBatchProcessing tests batch processing optimization
func TestBatchProcessing(t *testing.T) {
	t.Run("batch_size_limits_processing", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			buckets := []time.Duration{1 * time.Millisecond}
			// start=false: drive processReady() ourselves so the assertion is
			// about the per-cycle batch limit, not about when the background
			// ticker happens to fire.
			d := newDeleted(t.Context(), buckets, 0, false)
			defer d.Close()

			// Queue more items than a single batch can process.
			const items = 100
			require.Greater(t, items, d.batchSize)
			for range items {
				d.Delete(typedef.PartitionKeys{Values: typedef.NewValues(1)})
			}
			require.Equal(t, items, d.Len())

			// Advance past the bucket delay so every queued item is ready.
			time.Sleep(10 * time.Millisecond)

			// One processing cycle must emit at most batchSize items, leaving the
			// remainder queued — this is the batching guarantee.
			d.processReady()
			assert.Equal(t, items-d.batchSize, d.Len(),
				"one processReady cycle should process exactly batchSize (%d) of the %d ready items",
				d.batchSize, items)

			// A second cycle drains what is left (fewer than batchSize remain).
			d.processReady()
			assert.Equal(t, 0, d.Len(), "second cycle should drain the remaining items")
		})
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
}

// TestAdaptiveBackgroundInterval tests the adaptive ticker logic
func TestAdaptiveBackgroundInterval(t *testing.T) {
	t.Run("interval_adapts_to_ready_time", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			buckets := []time.Duration{50 * time.Millisecond}
			d := newDeleted(t.Context(), buckets, 0)
			defer d.Close()

			// Add item
			d.Delete(typedef.PartitionKeys{Values: typedef.NewValues(1)})

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
	})
}

// TestConcurrentBulkDelete tests concurrent bulk operations
func TestConcurrentBulkDelete(t *testing.T) {
	t.Run("concurrent_bulk_deletes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			buckets := []time.Duration{100 * time.Millisecond}
			d := newDeleted(t.Context(), buckets, 0)
			defer d.Close()

			const goroutines = 10
			const batchSize = 10
			done := make(chan bool, goroutines)

			for range goroutines {
				go func() {
					batch := make([]typedef.PartitionKeys, batchSize)
					for i := range batch {
						batch[i] = typedef.PartitionKeys{Values: typedef.NewValues(1)}
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
	})
}
