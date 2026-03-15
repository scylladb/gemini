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

package utils

import (
	"testing"
)

// resetFinalizers is a test helper that clears the global finalizers slice and
// restores it via t.Cleanup.  Tests that call this must NOT run in parallel
// since they manipulate global state.
func resetFinalizers(tb testing.TB) {
	tb.Helper()
	finalizersMu.Lock()
	old := finalizers
	finalizers = nil
	finalizersMu.Unlock()

	tb.Cleanup(func() {
		finalizersMu.Lock()
		finalizers = old
		finalizersMu.Unlock()
	})
}

func TestAddFinalizer(t *testing.T) { //nolint:tparallel
	// NOT parallel — manipulates global state
	resetFinalizers(t)

	called := false
	AddFinalizer(func() { called = true })

	finalizersMu.Lock()
	n := len(finalizers)
	finalizersMu.Unlock()

	if n != 1 {
		t.Fatalf("expected 1 finalizer, got %d", n)
	}

	ExecuteFinalizers()
	if !called {
		t.Error("expected finalizer to have been called")
	}
}

func TestAddFinalizerMultiple(t *testing.T) { //nolint:tparallel
	// NOT parallel — manipulates global state
	resetFinalizers(t)

	order := make([]int, 0, 3)
	AddFinalizer(func() { order = append(order, 1) })
	AddFinalizer(func() { order = append(order, 2) })
	AddFinalizer(func() { order = append(order, 3) })

	ExecuteFinalizers()

	// ExecuteFinalizers reverses the slice before iterating, so expected order is 3,2,1
	if len(order) != 3 {
		t.Fatalf("expected 3 calls, got %d: %v", len(order), order)
	}
	if order[0] != 3 || order[1] != 2 || order[2] != 1 {
		t.Errorf("expected reverse order [3 2 1], got %v", order)
	}
}

func TestExecuteFinalizersOnce(t *testing.T) { //nolint:tparallel
	// NOT parallel — manipulates global state
	resetFinalizers(t)

	count := 0
	AddFinalizer(func() { count++ })

	// Each finalizer is wrapped in sync.OnceFunc at registration time, so the
	// underlying function body runs exactly once regardless of how many times
	// ExecuteFinalizers is called.
	ExecuteFinalizers()
	ExecuteFinalizers()

	if count != 1 {
		t.Errorf("finalizer called %d times, expected 1 (sync.OnceFunc)", count)
	}
}

func TestExecuteFinalizersEmpty(t *testing.T) { //nolint:tparallel
	// NOT parallel — manipulates global state
	resetFinalizers(t)

	// Should not panic on empty slice
	ExecuteFinalizers()
}
