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

	"github.com/scylladb/gemini/pkg/typedef"
)

// TestHeapMemoryEfficiency validates that the optimized heap uses less memory
func TestHeapMemoryEfficiency(t *testing.T) {
	buckets := []time.Duration{100 * time.Millisecond}
	d := newDeleted(t.Context(), buckets)
	defer d.Close()

	// Add many items
	const count = 10000
	for range count {
		values := typedef.NewValues(1)
		d.Delete(values)
	}

	// Check that we didn't allocate excessively
	// With pre-allocation, we should have grown the backing array only a few times
	// Initial: 1024, then doubles: 2048, 4096, 8192, 16384
	d.mu.Lock()
	capacity := len(d.heap.data)
	length := d.heap.len
	d.mu.Unlock()

	t.Logf("Heap length: %d, capacity: %d, utilization: %.2f%%",
		length, capacity, float64(length)/float64(capacity)*100)

	// Verify capacity is reasonable (should have doubled from initial 1024)
	if capacity < count {
		t.Errorf("Capacity %d is less than count %d", capacity, count)
	}

	// Verify we didn't over-allocate by more than 2x
	if capacity > count*2 {
		t.Errorf("Capacity %d is more than 2x count %d - possible over-allocation", capacity, count)
	}
}

// TestHeapGrowthPattern validates exponential growth
func TestHeapGrowthPattern(t *testing.T) {
	t.Parallel()

	h := &deletedPartitionHeap{
		data: make([]deletedPartition, 4), // Start small
		len:  0,
	}

	capacities := []int{len(h.data)}

	// Add items and track capacity changes
	for i := range 100 {
		values := typedef.NewValues(1)
		h.pushInline(deletedPartition{
			values:  values,
			readyAt: time.Now().Add(time.Duration(i) * time.Millisecond),
		})

		if len(h.data) != capacities[len(capacities)-1] {
			capacities = append(capacities, len(h.data))
			t.Logf("Capacity grew to %d after %d insertions", len(h.data), i+1)
		}
	}

	t.Logf("Capacity growth pattern: %v", capacities)

	// Verify exponential growth (each capacity should be ~2x previous)
	for i := 1; i < len(capacities); i++ {
		ratio := float64(capacities[i]) / float64(capacities[i-1])
		if ratio < 1.5 || ratio > 2.5 {
			t.Errorf("Growth ratio %.2f is not close to 2.0 (from %d to %d)",
				ratio, capacities[i-1], capacities[i])
		}
	}
}
