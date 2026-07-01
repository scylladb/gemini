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

// TestRingMemoryEfficiency validates that the per-bucket rings do not
// over-allocate: total backing capacity is bounded by ~2x the live count.
func TestRingMemoryEfficiency(t *testing.T) {
	buckets := []time.Duration{100 * time.Millisecond}
	d := newDeleted(t.Context(), buckets, 0)
	defer d.Close()

	// Add many items
	const count = 10000
	for range count {
		d.Delete(typedef.PartitionKeys{Values: typedef.NewValues(1)})
	}

	// The ring grows by doubling, so capacity is the next power-of-two ≥ count.
	capacity := d.totalRingCapForTest()
	length := d.Len()

	t.Logf("Ring length: %d, capacity: %d, utilization: %.2f%%",
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

// Ring exponential growth/shrink is covered by TestRingBuf_GrowAndShrink in
// window_test.go.
