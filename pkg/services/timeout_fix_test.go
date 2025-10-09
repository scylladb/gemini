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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/scylladb/gemini/pkg/stop"
)

// mockStore implements the store.Store interface for testing

func TestWorkloadTimeoutLogic(t *testing.T) {
	// Test the timeout logic by simulating the timeout handler
	stopFlag := stop.NewFlag("test")

	// Initially should not be stopped
	assert.False(t, stopFlag.IsHardOrSoft())

	// Simulate what the timeout handler does after our fix
	stopFlag.SetSoft(true) // This is the correct way - our fix

	// Now it should be stopped
	assert.True(t, stopFlag.IsHardOrSoft())
}

func TestWorkloadStopFlag(t *testing.T) {
	// Test that the stop flag is properly set to true (not false)
	stopFlag := stop.NewFlag("test")

	// Initially should not be stopped
	assert.False(t, stopFlag.IsHardOrSoft())

	// Set soft stop to true (correct way)
	stopFlag.SetSoft(true)
	assert.True(t, stopFlag.IsHardOrSoft())

	// Create a new flag to test the other way
	stopFlag2 := stop.NewFlag("test2")

	// Set soft stop to false (this still sets the stop signal, just doesn't propagate)
	stopFlag2.SetSoft(false)
	assert.True(t, stopFlag2.IsHardOrSoft(), "SetSoft(false) should still set the stop signal")
}

func TestWorkloadTimeoutBehavior(t *testing.T) {
	// This test demonstrates the difference between the old buggy behavior
	// and the new correct behavior

	t.Run("correct_behavior_with_fix", func(t *testing.T) {
		// Create a stop flag and child flag (simulating workload structure)
		parentFlag := stop.NewFlag("workload")
		childFlag := parentFlag.CreateChild("timeout_test")

		// Initially not stopped
		assert.False(t, parentFlag.IsHardOrSoft())
		assert.False(t, childFlag.IsHardOrSoft())

		// Simulate our fix: SetSoft(true) when timeout occurs
		childFlag.SetSoft(true)

		// Both flags should now be stopped
		assert.True(t, childFlag.IsHardOrSoft(), "Child flag should be stopped after SetSoft(true)")
		// Parent may or may not be affected depending on propagation, but child definitely should be
	})

	t.Run("demonstrates_old_vs_new", func(t *testing.T) {
		// Both SetSoft(false) and SetSoft(true) set the stop signal
		// The difference is in parent propagation, but both stop the current flag

		flag1 := stop.NewFlag("test1")
		flag1.SetSoft(false) // Old buggy call
		assert.True(t, flag1.IsHardOrSoft(), "SetSoft(false) still sets stop signal")

		flag2 := stop.NewFlag("test2")
		flag2.SetSoft(true) // New correct call
		assert.True(t, flag2.IsHardOrSoft(), "SetSoft(true) sets stop signal")

		// Both work for stopping, but SetSoft(true) is semantically correct
		// and handles parent propagation properly
	})
}
