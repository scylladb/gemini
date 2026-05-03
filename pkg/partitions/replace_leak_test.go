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

package partitions

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/typedef"
)

// TestReplaceDoesNotLeakUUIDToIdx is a regression test for the partitions
// memory leak observed in the 2026-04-30 SCT runs.
//
// Before the fix, every call to Replace() (which is invoked by every
// production DELETE statement) inserted a new entry into uuidToIdx and never
// removed the old one, so the map grew without bound. With the seed-70 SCT
// configuration (200 mutators, 20% delete ratio, 60k partitions) this
// dominated the resident memory growth observed during the first 2-3 hours
// of the run.
//
// After the fix, deleteValidation cleans up uuidToIdx as well as
// validationMap, so the map size remains bounded by the
// deleted-partitions heap occupancy (which itself drains as buckets
// expire).
func TestReplaceDoesNotLeakUUIDToIdx(t *testing.T) {
	t.Parallel()

	const (
		count       = uint64(64)
		replacePass = 5_000
	)

	src, fn := distributions.New(distributions.Uniform, count, 1, 0, 0)
	table := createTestTable()
	config := typedef.PartitionRangeConfig{
		MaxBlobLength:   100,
		MinBlobLength:   10,
		MaxStringLength: 50,
		MinStringLength: 5,
		// Use a very short bucket so deleted-partition entries drain
		// quickly during the test, exercising the cleanup path.
		DeleteBuckets: []time.Duration{50 * time.Millisecond},
	}
	parts := New(t.Context(), rand.New(src), fn, table, config, count, 0)
	t.Cleanup(parts.Close)

	// Drain the deleted channel so processReady can pop entries and fire
	// the onDone closures (which call deleteValidation under the hood).
	drainerDone := make(chan struct{})
	go func() {
		defer close(drainerDone)
		for {
			select {
			case keys, ok := <-parts.Deleted():
				if !ok {
					return
				}
				if keys.Release != nil {
					keys.Release()
				}
			case <-t.Context().Done():
				return
			}
		}
	}()

	// Hammer Replace() to mimic the production delete workload.
	for i := uint64(0); i < replacePass; i++ {
		parts.Replace(i % count)
	}

	// Give the deleted-partitions background processor time to flush
	// every bucket and call onDone for each entry.
	require.Eventually(t, func() bool {
		parts.validationMu.RLock()
		defer parts.validationMu.RUnlock()
		// Once the heap has fully drained, both maps must shrink back to
		// roughly the live partition count (one entry per live slot).
		// Allow a little slack for in-flight items.
		return len(parts.uuidToIdx) <= int(count)*2 &&
			len(parts.validationMap) <= int(count)*2
	}, 10*time.Second, 50*time.Millisecond,
		"uuidToIdx/validationMap should shrink after deleted-partition heap drains; "+
			"this is the regression guard for the 2026-04-30 leak")

	parts.validationMu.RLock()
	uuidToIdxLen := len(parts.uuidToIdx)
	validationMapLen := len(parts.validationMap)
	parts.validationMu.RUnlock()

	// Hard upper bound: under no circumstance should the maps hold one
	// entry per Replace call. Before the fix this was exactly the
	// failure mode (uuidToIdxLen would be ~replacePass).
	assert.Less(t, uuidToIdxLen, int(count)*4,
		"uuidToIdx leaks one entry per Replace; got %d entries after %d replaces",
		uuidToIdxLen, replacePass)
	assert.Less(t, validationMapLen, int(count)*4,
		"validationMap leaks one entry per Replace; got %d entries after %d replaces",
		validationMapLen, replacePass)

	parts.Close()
	<-drainerDone
}
