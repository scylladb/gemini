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
//
//nolint:forbidigo
package partitions

import (
	"container/heap"
	"fmt"
	"testing"
	"time"

	"github.com/scylladb/gemini/pkg/typedef"
)

// TestOptimizationsWork is a quick smoke test to verify optimizations
func TestOptimizationsWork(t *testing.T) {
	t.Parallel()

	h := &deletedPartitionHeap{
		data: make([]deletedPartition, 64),
		len:  0,
	}

	now := time.Now()

	// Test pushInline
	for i := range 10 {
		h.pushInline(deletedPartition{
			values:  typedef.NewValues(1),
			readyAt: now.Add(time.Duration(10-i) * time.Millisecond),
		})
	}

	if h.len != 10 {
		t.Fatalf("Expected len=10, got %d", h.len)
	}

	// Test popInline
	var lastTime time.Time
	for range 10 {
		item, ok := h.popInline()
		if !ok {
			t.Fatal("popInline failed")
		}
		if !lastTime.IsZero() && item.readyAt.Before(lastTime) {
			t.Fatal("Heap order violated")
		}
		lastTime = item.readyAt
	}

	if h.len != 0 {
		t.Fatalf("Expected len=0 after popping all, got %d", h.len)
	}

	fmt.Println("✓ Optimizations working correctly")
}

// TestInlineVsStdlib compares inline vs stdlib performance
func TestInlineVsStdlib(t *testing.T) {
	t.Parallel()

	const iterations = 10000
	now := time.Now()

	// Test inline version
	startInline := time.Now()
	h1 := &deletedPartitionHeap{
		data: make([]deletedPartition, 1024),
		len:  0,
	}
	for i := range iterations {
		h1.pushInline(deletedPartition{
			values:  typedef.NewValues(1),
			readyAt: now.Add(time.Duration(i) * time.Millisecond),
		})
	}
	for h1.len > 0 {
		h1.popInline()
	}
	inlineDuration := time.Since(startInline)

	// Test stdlib version
	startStdlib := time.Now()
	h2 := &deletedPartitionHeap{
		data: make([]deletedPartition, 1024),
		len:  0,
	}
	heap.Init(h2)
	for i := range iterations {
		heap.Push(h2, deletedPartition{
			values:  typedef.NewValues(1),
			readyAt: now.Add(time.Duration(i) * time.Millisecond),
		})
	}
	for h2.len > 0 {
		heap.Pop(h2)
	}
	stdlibDuration := time.Since(startStdlib)

	improvement := float64(stdlibDuration-inlineDuration) / float64(stdlibDuration) * 100

	fmt.Printf("Inline:  %v\n", inlineDuration)
	fmt.Printf("Stdlib:  %v\n", stdlibDuration)
	fmt.Printf("Improvement: %.1f%%\n", improvement)

	if inlineDuration < stdlibDuration {
		fmt.Println("✓ Inline version is faster")
	} else {
		fmt.Println("⚠ Stdlib version was faster (unexpected)")
	}
}
