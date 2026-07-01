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
	d := newDeleted(t.Context(), buckets, 0)
	defer d.Close()

	t.Log("Created deleted partitions tracker")

	values := typedef.NewValues(1)
	keys := typedef.PartitionKeys{Values: values}
	d.Delete(keys)

	t.Log("Deleted partition, checking heap state...")

	// Check tracker state immediately
	heapLen := d.Len()
	if heapLen > 0 {
		earliest, _ := d.peekSoonestForTest()
		nowNs := time.Now().UnixNano()
		t.Logf("After delete: tracker has %d items", heapLen)
		t.Logf("  earliest.readyAtNs: %d", earliest.readyAtNs)
		t.Logf("  nowNs: %d", nowNs)
		t.Logf("  now < readyAt: %v", nowNs < earliest.readyAtNs)
		t.Logf("  time until ready: %v", time.Duration(earliest.readyAtNs-nowNs))
		t.Logf("  bucket: %d", earliest.bucket)
	} else {
		t.Fatal("Tracker is empty after delete!")
	}

	t.Log("Waiting for partition to be ready...")

	select {
	case received := <-d.ch:
		if received.Values != values {
			t.Fatal("Received wrong values")
		}
		t.Log("✓ Successfully received partition")
	case <-time.After(300 * time.Millisecond):
		// Check tracker state
		heapLen = d.Len()
		if heapLen > 0 {
			earliest, _ := d.peekSoonestForTest()
			nowNs := time.Now().UnixNano()
			t.Logf("After timeout: tracker still has %d items", heapLen)
			t.Logf("  earliest.readyAtNs: %d", earliest.readyAtNs)
			t.Logf("  nowNs: %d", nowNs)
			t.Logf("  now < readyAt: %v", nowNs < earliest.readyAtNs)
		}
		t.Fatal("Timeout waiting for partition")
	}
}
