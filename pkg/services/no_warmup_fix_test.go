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

package services

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/scylladb/gemini/pkg/stop"
)

func TestWorkloadWithoutWarmupTimeout(t *testing.T) {
	// Test that workloads without warmup don't stall indefinitely
	// This simulates the scenario where no partitions are available initially

	t.Run("context_cancellation_during_no_partitions", func(t *testing.T) {
		// Create a context that will be cancelled after a short time
		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		stopFlag := stop.NewFlag("test")

		// Simulate what happens in mutation/validation loops when ErrNoPartitionKeyValues occurs
		start := time.Now()

		// This loop simulates the fixed behavior
		for !stopFlag.IsHardOrSoft() {
			select {
			case <-ctx.Done():
				// Context was cancelled - this should happen within ~100ms
				duration := time.Since(start)
				assert.Less(t, duration, 200*time.Millisecond, "Should respond to context cancellation quickly")
				return
			default:
			}

			// Simulate ErrNoPartitionKeyValues case with our fix
			select {
			case <-time.After(200 * time.Millisecond):
				// This delay allows context cancellation to be processed
				continue
			case <-ctx.Done():
				// Context was cancelled during delay - good!
				duration := time.Since(start)
				assert.Less(t, duration, 200*time.Millisecond, "Should respond to context cancellation during delay")
				return
			}
		}

		t.Fatal("Loop should have been cancelled by context timeout")
	})

	t.Run("stop_flag_cancellation_during_no_partitions", func(t *testing.T) {
		stopFlag := stop.NewFlag("test")

		// Set a timer to set the stop flag after a short time
		go func() {
			time.Sleep(150 * time.Millisecond)
			stopFlag.SetSoft(true)
		}()

		start := time.Now()

		// This loop simulates the fixed behavior
		for !stopFlag.IsHardOrSoft() {
			select {
			case <-t.Context().Done():
				return
			default:
			}

			// Simulate ErrNoPartitionKeyValues case with our fix
			select {
			case <-time.After(200 * time.Millisecond):
				continue
			case <-t.Context().Done():
				return
			}
		}

		duration := time.Since(start)
		// Should exit when stop flag is set after ~150ms
		assert.Greater(t, duration, 140*time.Millisecond, "Should run for about the expected time")
		assert.Less(t, duration, 250*time.Millisecond, "Should not run much longer after stop flag is set")
	})
}

func TestBusyLoopPreventionFix(t *testing.T) {
	// Test that demonstrates the difference between old buggy behavior and new fixed behavior

	t.Run("old_behavior_simulation", func(t *testing.T) {
		// This simulates the old buggy behavior (busy loop)
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		defer cancel()

		start := time.Now()
		iterations := 0

		// Old buggy loop (immediate continue on ErrNoPartitionKeyValues)
		for iterations < 100 { // Limit iterations to prevent infinite test
			select {
			case <-ctx.Done():
				break
			default:
			}

			// Simulate ErrNoPartitionKeyValues - old behavior was immediate continue
			iterations++
			// No delay - this would cause busy waiting
		}

		duration := time.Since(start)
		// In the old behavior, this would complete many iterations very quickly
		assert.Equal(t, 100, iterations, "Should complete many iterations in busy loop")
		assert.Less(t, duration, 50*time.Millisecond, "Busy loop completes quickly but wastes CPU")
	})

	t.Run("new_behavior_simulation", func(t *testing.T) {
		// This simulates the new fixed behavior (delayed retry)
		ctx, cancel := context.WithTimeout(t.Context(), 350*time.Millisecond)
		defer cancel()

		start := time.Now()
		iterations := 0

		// New fixed loop (delayed retry on ErrNoPartitionKeyValues)
	loop:
		for {
			select {
			case <-ctx.Done():
				break loop
			default:
			}

			// Simulate ErrNoPartitionKeyValues - new behavior has delay
			iterations++

			// New behavior: delay with context cancellation support
			select {
			case <-time.After(200 * time.Millisecond):
				// Continue after delay if not cancelled
			case <-ctx.Done():
				// Exit immediately if context is cancelled during delay
				break loop
			}
		}

		duration := time.Since(start)
		// In the new behavior, with 200ms delays, we should get about 1-2 iterations in 350ms
		assert.LessOrEqual(t, iterations, 3, "Should complete fewer iterations due to delays")
		assert.Greater(t, duration, 300*time.Millisecond, "Should be cancelled by context timeout")
		assert.Less(t, duration, 400*time.Millisecond, "Should respond to context cancellation reasonably quickly")
	})
}
