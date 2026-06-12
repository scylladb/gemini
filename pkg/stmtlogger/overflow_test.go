// Copyright 2026 ScyllaDB
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

package stmtlogger

import "testing"

func itemWith(host string) Item { return Item{Host: host} }

func TestOverflowRing_FIFOOrder(t *testing.T) {
	t.Parallel()

	var r overflowRing
	for i := range 10 {
		if dropped := r.push(itemWith(string(rune('a' + i)))); dropped {
			t.Fatalf("unexpected drop at %d", i)
		}
	}
	if r.count != 10 {
		t.Fatalf("count = %d, want 10", r.count)
	}

	for i := range 10 {
		got, ok := r.pop()
		if !ok {
			t.Fatalf("pop %d: empty", i)
		}
		if want := string(rune('a' + i)); got.Host != want {
			t.Fatalf("pop %d: got %q want %q", i, got.Host, want)
		}
	}
	if _, ok := r.pop(); ok {
		t.Fatal("pop on empty ring returned ok")
	}
}

func TestOverflowRing_WrapAround(t *testing.T) {
	t.Parallel()

	var r overflowRing
	// Push 64 (initial cap), pop 32, push 32 more to force wrap.
	for i := range 64 {
		r.push(itemWith(string(rune(i))))
	}
	for range 32 {
		r.pop()
	}
	for i := range 32 {
		r.push(itemWith(string(rune(100 + i))))
	}
	if r.count != 64 {
		t.Fatalf("count = %d, want 64", r.count)
	}
	// Drain and verify ordering is preserved across the wrap.
	for i := range 32 {
		got, _ := r.pop()
		if want := string(rune(32 + i)); got.Host != want {
			t.Fatalf("wrap pop %d: got %q want %q", i, got.Host, want)
		}
	}
	for i := range 32 {
		got, _ := r.pop()
		if want := string(rune(100 + i)); got.Host != want {
			t.Fatalf("wrap pop tail %d: got %q want %q", i, got.Host, want)
		}
	}
}

func TestOverflowRing_DropOldestAtCap(t *testing.T) {
	t.Parallel()

	const ringCap = 8
	r := overflowRing{maxItems: ringCap}
	// Fill exactly to the cap.
	for i := range ringCap {
		if dropped := r.push(Item{RecentSuccess: []uint64{uint64(i)}}); dropped {
			t.Fatalf("unexpected drop while filling at %d", i)
		}
	}
	if r.count != ringCap {
		t.Fatalf("count = %d, want %d", r.count, ringCap)
	}

	// One more push must drop the oldest (value 0) and keep count at the cap.
	if dropped := r.push(Item{RecentSuccess: []uint64{uint64(ringCap)}}); !dropped {
		t.Fatal("expected drop at cap")
	}
	if r.count != ringCap {
		t.Fatalf("count after over-cap push = %d, want %d", r.count, ringCap)
	}
	if r.dropped != 1 {
		t.Fatalf("dropped = %d, want 1", r.dropped)
	}

	// Oldest surviving item should now be value 1, newest the just-pushed value.
	first, _ := r.pop()
	if first.RecentSuccess[0] != 1 {
		t.Fatalf("oldest survivor = %d, want 1", first.RecentSuccess[0])
	}
}

func TestOverflowRing_PushFront(t *testing.T) {
	t.Parallel()

	var r overflowRing
	r.push(itemWith("b"))
	r.push(itemWith("c"))
	r.pushFront(itemWith("a"))

	for _, want := range []string{"a", "b", "c"} {
		got, ok := r.pop()
		if !ok || got.Host != want {
			t.Fatalf("got %q (ok=%v) want %q", got.Host, ok, want)
		}
	}
}

func TestOverflowRing_Reset(t *testing.T) {
	t.Parallel()

	var r overflowRing
	for i := range 100 {
		r.push(itemWith(string(rune(i))))
	}
	r.reset()
	if r.count != 0 || r.buf != nil {
		t.Fatalf("after reset count=%d buf!=nil=%v", r.count, r.buf != nil)
	}
	if _, ok := r.pop(); ok {
		t.Fatal("pop after reset returned ok")
	}
}
