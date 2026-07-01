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

package jobs

import (
	"strconv"
	"testing"
)

// BenchmarkSampleRowsForTracker measures the per-validation sampling hot path.
// TrackRow is a no-op here so we isolate the allocations sampleRowsForTracker
// itself makes (notably the random index selection) from the tracker's own work.
func BenchmarkSampleRowsForTracker(b *testing.B) {
	for _, n := range []int{10, 100, 1000} {
		b.Run("rows="+strconv.Itoa(n), func(b *testing.B) {
			gen := &trackingGenerator{fillRatio: 0.10, noTrack: true}
			// maxSamplesPerRun high so forceAll exercises up to n pushes.
			v := newSampleValidation(gen, n+1)
			stmt := makeStmt(1)
			rows := makeRows(n)

			b.ReportAllocs()
			for b.Loop() {
				v.sampleRowsForTracker(stmt, rows, true)
			}
		})
	}
}
