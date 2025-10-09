// Copyright 2025 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package workpool

import (
	"context"
	"testing"
	"time"
)

// TestWorkpoolContextCancellation tests that workpool respects context cancellation
func TestWorkpoolContextCancellation(t *testing.T) {
	t.Parallel()
	pool := New(2)
	defer pool.Close()

	// Test with a context that is already cancelled
	ctx, cancel := context.WithCancel(t.Context())
	cancel() // Cancel immediately

	// Send a task - this should fail immediately because context is cancelled
	start := time.Now()
	err := pool.SendWithoutResult(ctx, func(_ context.Context) {
		t.Error("Task should not have been executed due to cancelled context")
	})

	elapsed := time.Since(start)

	// The SendWithoutResult should return quickly with an error when context is cancelled
	if err == nil {
		t.Error("Expected error due to context cancellation")
	}

	// Should complete very quickly
	maxExpected := 10 * time.Millisecond
	if elapsed > maxExpected {
		t.Errorf("SendWithoutResult took too long: %v (expected < %v)", elapsed, maxExpected)
	}

	t.Logf("SendWithoutResult completed in %v with error: %v", elapsed, err)
}

// TestWorkpoolDoesNotBlockOnContextCancel tests that workpool doesn't block indefinitely
func TestWorkpoolDoesNotBlockOnContextCancel(t *testing.T) {
	t.Parallel()

	pool := New(1)
	defer pool.Close()

	// Fill up the worker with a long-running task
	longTaskCtx := t.Context()
	err := pool.SendWithoutResult(longTaskCtx, func(_ context.Context) {
		time.Sleep(200 * time.Millisecond)
	})
	if err != nil {
		t.Fatalf("Failed to send long task: %v", err)
	}

	// Create a cancelled context
	shortCtx, cancel := context.WithCancel(t.Context())
	cancel() // Cancel immediately

	start := time.Now()
	err = pool.SendWithoutResult(shortCtx, func(_ context.Context) {
		// This shouldn't run because context is cancelled
		t.Error("Short task should not have executed")
	})
	elapsed := time.Since(start)

	// Should return quickly with context cancellation error
	if err == nil {
		t.Error("Expected error due to context cancellation")
	}

	maxExpected := 10 * time.Millisecond
	if elapsed > maxExpected {
		t.Errorf("SendWithoutResult blocked too long: %v (expected < %v)", elapsed, maxExpected)
	}

	t.Logf("SendWithoutResult with cancelled context completed in %v", elapsed)
}
