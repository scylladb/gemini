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

package utils_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/utils"
)

func TestTimeDurationToScyllaDuration(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	duration := 10*365*24*time.Hour + // 10 years (approx)
		2*30*24*time.Hour + // 2 months (approx)
		1*7*24*time.Hour + // 1 week
		2*24*time.Hour + // 2 days
		1*time.Hour + // 1 hour
		30*time.Minute + // 30 minutes
		45*time.Second + // 45 seconds
		500*time.Millisecond + // 500 milliseconds
		200*time.Microsecond + // 200 microseconds
		100*time.Nanosecond // 100 nanoseconds

	expected := "10y2mo1w2d1h30m45s500ms200us100ns"

	result := utils.TimeDurationToScyllaDuration(duration)

	assert.Equal(expected, result, "The formatted duration should match the expected string")
}

func BenchmarkTimeDurationToScyllaDuration(b *testing.B) {
	b.ReportAllocs()
	// Test cases with different complexity levels
	testCases := []struct {
		name     string
		duration time.Duration
	}{
		{
			name:     "Simple",
			duration: 5*time.Second + 100*time.Millisecond,
		},
		{
			name: "Complex", //nolint:usestdlibvars
			//nolint:lll
			duration: 10*365*24*time.Hour + 2*30*24*time.Hour + 1*7*24*time.Hour + 2*24*time.Hour + 1*time.Hour + 30*time.Minute + 45*time.Second + 500*time.Millisecond + 200*time.Microsecond + 100*time.Nanosecond,
		},
		{
			name:     "Zero",
			duration: 0,
		},
		{
			name:     "Nanoseconds",
			duration: 123 * time.Nanosecond,
		},
		{
			name:     "Mixed",
			duration: 1*time.Hour + 30*time.Minute + 500*time.Millisecond,
		},
	}

	b.ResetTimer()
	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				_ = utils.TimeDurationToScyllaDuration(tc.duration)
			}
		})
	}
}

func BenchmarkTimeDurationToScyllaDuration_AllUnits(b *testing.B) {
	// Benchmark with a duration that exercises all units
	duration := 10*365*24*time.Hour + // 10 years
		2*30*24*time.Hour + // 2 months
		1*7*24*time.Hour + // 1 week
		2*24*time.Hour + // 2 days
		1*time.Hour + // 1 hour
		30*time.Minute + // 30 minutes
		45*time.Second + // 45 seconds
		500*time.Millisecond + // 500 milliseconds
		200*time.Microsecond + // 200 microseconds
		100*time.Nanosecond // 100 nanoseconds

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_ = utils.TimeDurationToScyllaDuration(duration)
	}
}
