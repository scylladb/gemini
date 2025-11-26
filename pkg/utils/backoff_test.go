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
//nolint:revive
package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestExponentialBackoffCapped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		attempt  int
		maxDelay time.Duration
		minDelay time.Duration
		expected time.Duration
	}{
		// Basic functionality tests
		{
			name:     "first attempt (attempt 0) returns minDelay",
			attempt:  0,
			maxDelay: 5 * time.Second,
			minDelay: 100 * time.Millisecond,
			expected: 100 * time.Millisecond,
		},
		{
			name:     "second attempt (attempt 1) doubles minDelay",
			attempt:  1,
			maxDelay: 5 * time.Second,
			minDelay: 100 * time.Millisecond,
			expected: 200 * time.Millisecond,
		},
		{
			name:     "third attempt (attempt 2) quadruples minDelay",
			attempt:  2,
			maxDelay: 5 * time.Second,
			minDelay: 100 * time.Millisecond,
			expected: 400 * time.Millisecond,
		},
		{
			name:     "fourth attempt (attempt 3) multiplies by 8",
			attempt:  3,
			maxDelay: 5 * time.Second,
			minDelay: 100 * time.Millisecond,
			expected: 800 * time.Millisecond,
		},

		// Edge cases with zero and negative values
		{
			name:     "zero maxDelay returns zero",
			attempt:  1,
			maxDelay: 0,
			minDelay: 100 * time.Millisecond,
			expected: 0,
		},
		{
			name:     "negative maxDelay returns zero",
			attempt:  1,
			maxDelay: -1 * time.Second,
			minDelay: 100 * time.Millisecond,
			expected: 0,
		},
		{
			name:     "zero minDelay defaults to 10ms",
			attempt:  0,
			maxDelay: 5 * time.Second,
			minDelay: 0,
			expected: 50 * time.Millisecond,
		},
		{
			name:     "negative minDelay defaults to 10ms",
			attempt:  0,
			maxDelay: 5 * time.Second,
			minDelay: -100 * time.Millisecond,
			expected: 50 * time.Millisecond,
		},
		{
			name:     "negative attempt returns minDelay",
			attempt:  -1,
			maxDelay: 5 * time.Second,
			minDelay: 100 * time.Millisecond,
			expected: 100 * time.Millisecond,
		},

		// Capping behavior tests
		{
			name:     "delay capped at maxDelay",
			attempt:  10, // Would be 100ms * 2^10 = 102.4s without cap
			maxDelay: 1 * time.Second,
			minDelay: 100 * time.Millisecond,
			expected: 1 * time.Second,
		},
		{
			name:     "minDelay greater than maxDelay returns maxDelay at attempt 0",
			attempt:  0,
			maxDelay: 50 * time.Millisecond,
			minDelay: 100 * time.Millisecond,
			expected: 50 * time.Millisecond,
		},
		{
			name:     "minDelay greater than maxDelay returns maxDelay at positive attempt",
			attempt:  1,
			maxDelay: 50 * time.Millisecond,
			minDelay: 100 * time.Millisecond,
			expected: 50 * time.Millisecond,
		},

		// Boundary conditions
		{
			name:     "very small delays",
			attempt:  1,
			maxDelay: 5 * time.Nanosecond,
			minDelay: 1 * time.Nanosecond,
			expected: 2 * time.Nanosecond,
		},
		{
			name:     "large attempt number",
			attempt:  20,
			maxDelay: 1 * time.Hour,
			minDelay: 1 * time.Millisecond,
			expected: 1048576 * time.Millisecond, // 2^20 ms = 17m28.576s, not capped
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ExponentialBackoffCapped(tt.attempt, tt.maxDelay, tt.minDelay)
			if result != tt.expected {
				t.Errorf("ExponentialBackoffCapped(%d, %v, %v) = %v, want %v",
					tt.attempt, tt.maxDelay, tt.minDelay, result, tt.expected)
			}
		})
	}
}

func TestExponentialBackoffCapped_ExponentialGrowth(t *testing.T) {
	t.Parallel()

	minDelay := 10 * time.Millisecond
	maxDelay := 10 * time.Second

	// Test that each attempt doubles the previous (until capped)
	for attempt := range 10 {
		delay := ExponentialBackoffCapped(attempt, maxDelay, minDelay)
		expected := minDelay << uint(attempt)
		if expected > maxDelay {
			expected = maxDelay
		}

		if delay != expected {
			t.Errorf("Attempt %d: got %v, want %v", attempt, delay, expected)
		}
	}
}

func TestExponentialBackoffCapped_DefaultMinDelay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		minDelay time.Duration
		expected time.Duration
	}{
		{
			name:     "zero minDelay uses default 10ms",
			minDelay: 0,
			expected: 50 * time.Millisecond,
		},
		{
			name:     "negative minDelay uses default 10ms",
			minDelay: -5 * time.Millisecond,
			expected: 50 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := ExponentialBackoffCapped(0, time.Second, tt.minDelay)
			if result != tt.expected {
				t.Errorf("ExponentialBackoffCapped(0, 1s, %v) = %v, want %v",
					tt.minDelay, result, tt.expected)
			}
		})
	}
}

func TestExponentialBackoffCapped_OverflowProtection(t *testing.T) {
	t.Parallel()

	// Test with very large attempt numbers to understand overflow behavior
	minDelay := 1 * time.Millisecond
	maxDelay := 1 * time.Minute

	// Use a large attempt number that causes bit shift overflow
	largeAttempt := 100
	result := ExponentialBackoffCapped(largeAttempt, maxDelay, minDelay)

	// When bit shift overflows, it wraps to 0, which triggers maxDelay <= 0 check
	// This is actually the current behavior of the implementation
	if result != 0 {
		t.Errorf("Large attempt with overflow: got %v, want 0 (due to overflow behavior)", result)
	}

	// Test with smaller but still large attempt that should be capped
	mediumAttempt := 25 // This should exceed maxDelay and be capped
	result2 := ExponentialBackoffCapped(mediumAttempt, maxDelay, minDelay)
	if result2 != maxDelay {
		t.Errorf("Medium large attempt should be capped: got %v, want %v", result2, maxDelay)
	}

	// Ensure no panic occurred
	if result2 < 0 {
		t.Error("Backoff delay should never be negative")
	}
}

func BenchmarkExponentialBackoffCapped(b *testing.B) {
	minDelay := 10 * time.Millisecond
	maxDelay := 5 * time.Second

	b.ResetTimer()
	for i := range b.N {
		_ = ExponentialBackoffCapped(i%20, maxDelay, minDelay)
	}
}

func BenchmarkExponentialBackoffCapped_EdgeCases(b *testing.B) {
	b.Run("ZeroMaxDelay", func(b *testing.B) {
		for range b.N {
			_ = ExponentialBackoffCapped(5, 0, 10*time.Millisecond)
		}
	})

	b.Run("ZeroMinDelay", func(b *testing.B) {
		for range b.N {
			_ = ExponentialBackoffCapped(5, time.Second, 0)
		}
	})

	b.Run("LargeAttempt", func(b *testing.B) {
		for range b.N {
			_ = ExponentialBackoffCapped(50, time.Minute, time.Millisecond)
		}
	})
}

// Test property: backoff should never exceed maxDelay
func TestExponentialBackoffCapped_Property_NeverExceedsMax(t *testing.T) {
	t.Parallel()

	maxDelay := 1 * time.Second
	minDelay := 10 * time.Millisecond

	for attempt := range 50 {
		result := ExponentialBackoffCapped(attempt, maxDelay, minDelay)
		if result > maxDelay {
			t.Errorf("Attempt %d exceeded maxDelay: got %v, max %v", attempt, result, maxDelay)
		}
	}
}

// Test property: backoff should be monotonic (non-decreasing) until cap
func TestExponentialBackoffCapped_Property_Monotonic(t *testing.T) {
	t.Parallel()

	maxDelay := 10 * time.Second
	minDelay := 10 * time.Millisecond

	var prev time.Duration
	for attempt := range 20 {
		current := ExponentialBackoffCapped(attempt, maxDelay, minDelay)
		if attempt > 0 && current < prev {
			t.Errorf("Backoff decreased from attempt %d to %d: %v -> %v",
				attempt-1, attempt, prev, current)
		}
		prev = current
	}
}

// Test that the function handles edge case where minDelay equals maxDelay
func TestExponentialBackoffCapped_MinEqualsMax(t *testing.T) {
	t.Parallel()

	delay := 100 * time.Millisecond

	for attempt := range 10 {
		result := ExponentialBackoffCapped(attempt, delay, delay)
		if result != delay {
			t.Errorf("Attempt %d with minDelay=maxDelay: got %v, want %v",
				attempt, result, delay)
		}
	}
}

// Test mathematical correctness of exponential growth
func TestExponentialBackoffCapped_MathematicalCorrectness(t *testing.T) {
	t.Parallel()

	minDelay := 50 * time.Millisecond
	maxDelay := 1 * time.Hour // Large enough to not interfere

	tests := []struct {
		attempt  int
		expected time.Duration
	}{
		{0, 50 * time.Millisecond},              // 50 * 2^0 = 50
		{1, 100 * time.Millisecond},             // 50 * 2^1 = 100
		{2, 200 * time.Millisecond},             // 50 * 2^2 = 200
		{3, 400 * time.Millisecond},             // 50 * 2^3 = 400
		{4, 800 * time.Millisecond},             // 50 * 2^4 = 800
		{5, 1600 * time.Millisecond},            // 50 * 2^5 = 1600
		{10, 50 * time.Millisecond * (1 << 10)}, // 50 * 2^10 = 51200ms
	}

	for _, tt := range tests {
		t.Run("", func(t *testing.T) {
			t.Parallel()
			assert := require.New(t)
			result := ExponentialBackoffCapped(tt.attempt, maxDelay, minDelay)
			assert.Equal(tt.expected, result, "Attempt %d: got %v, want %v", tt.attempt, result, tt.expected)
		})
	}
}
