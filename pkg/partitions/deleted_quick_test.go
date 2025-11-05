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

// TestDeletedQuickCheck is a minimal test to verify basic functionality
func TestDeletedQuickCheck(t *testing.T) {
	t.Parallel()

	buckets := []time.Duration{50 * time.Millisecond}
	d := newDeleted(t.Context(), buckets)
	defer d.Close()

	t.Log("Created deleted partitions tracker")

	values := typedef.NewValues(1)
	d.Delete(values)

	t.Log("Deleted partition, checking heap state...")

	// Check heap immediately
	d.mu.Lock()
	heapLen := d.heap.Len()
	if heapLen > 0 {
		earliest := d.heap.data[0]
		now := time.Now()
		t.Logf("After delete: heap has %d items", heapLen)
		t.Logf("  earliest.readyAt: %v", earliest.readyAt)
		t.Logf("  now: %v", now)
		t.Logf("  now.Before(readyAt): %v", now.Before(earliest.readyAt))
		t.Logf("  time until ready: %v", earliest.readyAt.Sub(now))
		t.Logf("  counter: %d", earliest.counter)
	} else {
		t.Fatal("Heap is empty after delete!")
	}
	d.mu.Unlock()

	t.Log("Waiting for partition to be ready...")

	select {
	case received := <-d.ch:
		if received != values {
			t.Fatal("Received wrong values")
		}
		t.Log("✓ Successfully received partition")
	case <-time.After(300 * time.Millisecond):
		// Check heap state
		d.mu.Lock()
		heapLen = d.heap.Len()
		if heapLen > 0 {
			earliest := d.heap.data[0]
			now := time.Now()
			t.Logf("After timeout: heap still has %d items", heapLen)
			t.Logf("  earliest.readyAt: %v", earliest.readyAt)
			t.Logf("  now: %v", now)
			t.Logf("  now.Before(readyAt): %v", now.Before(earliest.readyAt))
		}
		d.mu.Unlock()
		t.Fatal("Timeout waiting for partition")
	}
}
