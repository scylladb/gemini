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
	"sync"
	"sync/atomic"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/typedef"
)

// createTestPartitionsWithMaxInvalid creates a Partitions instance with
// the given count and maxInvalid limit.
func createTestPartitionsWithMaxInvalid(t *testing.T, count, maxInvalid uint64) *Partitions {
	t.Helper()
	src, fn := distributions.New(distributions.Uniform, count, 1, 0, 0)
	table := createTestTable()
	config := typedef.PartitionRangeConfig{
		MaxBlobLength:   100,
		MinBlobLength:   10,
		MaxStringLength: 50,
		MinStringLength: 5,
	}
	parts := New(t.Context(), rand.New(src), fn, table, config, count, maxInvalid)
	t.Cleanup(parts.Close)
	return parts
}

// TestMarkInvalid_FirstCallerWins verifies that MarkInvalid is idempotent:
// only the very first call for a given partition returns true.
func TestMarkInvalid_FirstCallerWins(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 10)
	key := parts.Get(0)
	require.NotNil(t, key)
	defer key.Release()

	// First call must succeed.
	assert.True(t, parts.MarkInvalid(&key))
	// Second call must be a no-op.
	assert.False(t, parts.MarkInvalid(&key))
	// Count must be exactly 1.
	assert.Equal(t, uint64(1), parts.InvalidCount())
}

// TestMarkInvalid_NilKey verifies that MarkInvalid is safe with nil input.
func TestMarkInvalid_NilKey(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 5)
	assert.False(t, parts.MarkInvalid(nil))
	assert.Equal(t, uint64(0), parts.InvalidCount())
}

// TestMarkInvalid_UnknownUUID verifies that MarkInvalid returns false for a UUID
// that is not tracked (e.g. already released or never registered).
func TestMarkInvalid_UnknownUUID(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 5)
	ghost := typedef.PartitionKeys{ID: uuid.Must(uuid.NewV7())}
	assert.False(t, parts.MarkInvalid(&ghost))
	assert.Equal(t, uint64(0), parts.InvalidCount())
}

// TestMarkInvalid_LimitEnforced verifies that the maxInvalid cap is respected.
// After maxInvalid partitions are marked, further calls must return false.
func TestMarkInvalid_LimitEnforced(t *testing.T) {
	t.Parallel()

	const total uint64 = 10
	const limit uint64 = 3
	parts := createTestPartitionsWithMaxInvalid(t, total, limit)

	var successCount int
	for i := range total {
		key := parts.Get(i)
		require.NotNil(t, key)
		if parts.MarkInvalid(&key) {
			successCount++
		}
		key.Release()
	}

	assert.Equal(t, int(limit), successCount)
	assert.Equal(t, limit, parts.InvalidCount())
}

// TestMarkInvalid_NoLimit verifies that when maxInvalid == 0, all partitions
// can be marked (no cap).
func TestMarkInvalid_NoLimit(t *testing.T) {
	t.Parallel()

	const total uint64 = 5
	parts := createTestPartitionsWithMaxInvalid(t, total, 0)

	for i := range total {
		key := parts.Get(i)
		require.NotNil(t, key)
		assert.True(t, parts.MarkInvalid(&key))
		key.Release()
	}

	assert.Equal(t, total, parts.InvalidCount())
}

// TestMarkInvalid_ReplacedSlotReportsButDoesNotMark is a regression test for two
// interacting review findings:
//
//   - Finding 1: a divergence discovered on a partition whose slot has since been
//     replaced (the deleted-partition re-validation path) must still be
//     reportable — MarkInvalid must return true.
//   - Finding 3: that reused slot now holds a fresh, valid partition, so it must
//     NOT be added to the invalid set or counted toward the maxInvalid budget;
//     doing so would skip a valid partition and could trigger a premature stop.
func TestMarkInvalid_ReplacedSlotReportsButDoesNotMark(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 4)

	// Borrow the partition currently at slot 0, then replace it. The borrowed
	// keys keep the old UUID mapped (uuidToIdx) alive, mirroring a deleted
	// partition still awaiting re-validation.
	oldKey := parts.Get(0)
	oldKeys := parts.Replace(0) // installs a fresh, valid partition at slot 0
	require.NotEqual(t, oldKey.ID, parts.Get(0).ID, "slot 0 must hold a new partition after Replace")

	// The divergence is real and must be reportable even though the slot moved on.
	assert.True(t, parts.MarkInvalid(&oldKey), "divergence on a replaced partition must still be reported")

	// ...but the fresh, valid partition now occupying slot 0 must not be marked
	// or counted.
	assert.False(t, parts.IsInvalid(0), "replaced slot must not be flagged invalid")
	assert.Equal(t, uint64(0), parts.InvalidCount(), "replaced slot must not inflate the invalid count")

	oldKey.Release()
	if oldKeys.Release != nil {
		oldKeys.Release()
	}
}

// TestMarkInvalid_ConcurrentReplaceNoInflation hammers MarkInvalid against a
// concurrent Replace of the same slot (the finding-3 race) and asserts the
// invalid count is never inflated by a freshly-replaced valid slot. Run under
// -race, it also guards the invalidByIdx/invalidCount bookkeeping.
func TestMarkInvalid_ConcurrentReplaceNoInflation(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 8)

	const rounds = 2000
	var wg sync.WaitGroup
	wg.Add(2)

	// Marker: repeatedly borrows slot 0 and tries to mark it invalid.
	go func() {
		defer wg.Done()
		for range rounds {
			key := parts.Get(0)
			parts.MarkInvalid(&key)
			key.Release()
		}
	}()

	// Replacer: repeatedly replaces slot 0 (clears any invalid mark).
	go func() {
		defer wg.Done()
		for range rounds {
			old := parts.Replace(0)
			if old.Release != nil {
				old.Release()
			}
		}
	}()

	wg.Wait()

	// InvalidCount must never exceed the number of slots and must equal the
	// number of slots actually flagged — a stale mark left on a replaced valid
	// slot would break this invariant.
	count := parts.InvalidCount()
	assert.LessOrEqual(t, count, parts.Len())

	var flagged uint64
	for i := range parts.Len() {
		if parts.IsInvalid(i) {
			flagged++
		}
	}
	assert.Equal(t, flagged, count, "invalidCount must match the number of slots actually flagged (no inflation)")
}

// TestIsInvalid verifies that IsInvalid correctly reflects the marked state.
func TestIsInvalid(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 5)

	for i := range uint64(5) {
		assert.False(t, parts.IsInvalid(i), "index %d should not be invalid yet", i)
	}

	key := parts.Get(2)
	require.NotNil(t, key)
	defer key.Release()

	require.True(t, parts.MarkInvalid(&key))
	assert.True(t, parts.IsInvalid(2))

	// Other indices untouched.
	for i := range uint64(5) {
		if i == 2 {
			continue
		}
		assert.False(t, parts.IsInvalid(i))
	}
}

// TestInvalidCount verifies the counter increments correctly.
func TestInvalidCount(t *testing.T) {
	t.Parallel()

	const total uint64 = 8
	parts := createTestPartitions(t, total)

	assert.Equal(t, uint64(0), parts.InvalidCount())

	for i := range total {
		key := parts.Get(i)
		require.NotNil(t, key)
		parts.MarkInvalid(&key)
		key.Release()
		assert.Equal(t, i+1, parts.InvalidCount())
	}
}

// TestMarkInvalid_SkippedInNext verifies that Next() never returns a partition
// that has been marked invalid (within the available valid slots).
func TestMarkInvalid_SkippedInNext(t *testing.T) {
	t.Parallel()

	const total uint64 = 20
	parts := createTestPartitions(t, total)

	// Mark all but index 0 as invalid to give the loop the hardest possible job.
	for i := uint64(1); i < total; i++ {
		key := parts.Get(i)
		require.NotNil(t, key)
		parts.MarkInvalid(&key)
		key.Release()
	}

	// Next() should always return a valid partition.
	for range 50 {
		key := parts.Next()
		require.NotNil(t, key)

		// Resolve the index from the UUID.
		parts.validationMu.RLock()
		idx, ok := parts.uuidToIdx[key.ID]
		parts.validationMu.RUnlock()

		if ok {
			assert.False(t, parts.IsInvalid(idx),
				"Next() returned an invalid partition at index %d", idx)
		}
		key.Release()
	}
}

// TestMarkInvalid_SkippedInReplaceNext verifies that ReplaceNext() skips
// partitions that have been marked invalid.
func TestMarkInvalid_SkippedInReplaceNext(t *testing.T) {
	t.Parallel()

	const total uint64 = 20
	parts := createTestPartitions(t, total)

	// Mark all but the last index as invalid.
	for i := range total - 1 {
		key := parts.Get(i)
		require.NotNil(t, key)
		parts.MarkInvalid(&key)
		key.Release()
	}

	// ReplaceNext should still succeed and return a valid old key.
	for range 10 {
		oldKey := parts.ReplaceNext()
		require.NotNil(t, oldKey)
		oldKey.Release()
	}
}

// TestMarkInvalid_SkippedInReplaceNextWithoutOld verifies that
// ReplaceNextWithoutOld() skips invalid partitions.
func TestMarkInvalid_SkippedInReplaceNextWithoutOld(t *testing.T) {
	t.Parallel()

	const total uint64 = 20
	parts := createTestPartitions(t, total)

	// Mark all but two indices as invalid.
	for i := range total - 2 {
		key := parts.Get(i)
		require.NotNil(t, key)
		parts.MarkInvalid(&key)
		key.Release()
	}

	// Must not panic or loop forever.
	for range 10 {
		parts.ReplaceNextWithoutOld()
	}
}

// TestMarkInvalid_StatsReflectsInvalidCount verifies that Stats() includes
// the correct invalid count.
func TestMarkInvalid_StatsReflectsInvalidCount(t *testing.T) {
	t.Parallel()

	const total uint64 = 10
	parts := createTestPartitions(t, total)

	assert.Equal(t, uint64(0), parts.Stats().InvalidPartitionsCount)

	for i := range uint64(3) {
		key := parts.Get(i)
		require.NotNil(t, key)
		parts.MarkInvalid(&key)
		key.Release()
	}

	assert.Equal(t, uint64(3), parts.Stats().InvalidPartitionsCount)
}

// TestMarkInvalid_Concurrent verifies the concurrency contract:
// exactly one goroutine wins per partition, and the total count is consistent.
func TestMarkInvalid_Concurrent(t *testing.T) {
	t.Parallel()

	const total uint64 = 50
	const goroutines = 20
	parts := createTestPartitions(t, total)

	// Snapshot all keys before the concurrent phase to avoid races on Get().
	keys := make([]typedef.PartitionKeys, total)
	for i := range total {
		keys[i] = parts.Get(i)
	}
	t.Cleanup(func() {
		for i := range total {
			keys[i].Release()
		}
	})

	var wins atomic.Int64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for i := range total {
				if parts.MarkInvalid(&keys[i]) {
					wins.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	// Each of the `total` partitions should have been claimed exactly once.
	assert.Equal(t, int64(total), wins.Load())
	assert.Equal(t, total, parts.InvalidCount())
}

// TestMarkInvalid_ConcurrentDedup verifies that when many goroutines race on
// the same partition, exactly one returns true.
func TestMarkInvalid_ConcurrentDedup(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 5)
	key := parts.Get(0)
	require.NotNil(t, key)
	defer key.Release()

	const goroutines = 100
	var wins atomic.Int64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			if parts.MarkInvalid(&key) {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(1), wins.Load(), "exactly one goroutine must win the race")
	assert.Equal(t, uint64(1), parts.InvalidCount())
}

// TestFill_ClearsInvalidFlag verifies that fill() (used by ReplaceWithoutOld /
// ReplaceNextWithoutOld) removes the invalid flag for a slot so that Next()
// can return the freshly filled partition again.
func TestFill_ClearsInvalidFlag(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 5)

	// Mark slot 0 as invalid.
	key := parts.Get(0)
	require.NotNil(t, key)
	require.True(t, parts.MarkInvalid(&key))
	key.Release()

	assert.True(t, parts.IsInvalid(0))
	assert.Equal(t, uint64(1), parts.InvalidCount())

	// ReplaceWithoutOld replaces via fill(). The invalid flag must be cleared.
	parts.ReplaceWithoutOld(0)

	assert.False(t, parts.IsInvalid(0), "fill() must clear the invalid flag for the replaced slot")
	assert.Equal(t, uint64(0), parts.InvalidCount(), "invalid count must decrement after fill()")
}

// TestReplace_ClearsInvalidFlag verifies that Replace() clears the invalid flag
// so the slot becomes accessible to Next() after replacement.
func TestReplace_ClearsInvalidFlag(t *testing.T) {
	t.Parallel()

	const total uint64 = 10
	parts := createTestPartitions(t, total)

	// Mark all slots except the last one as invalid.
	for i := range total - 1 {
		key := parts.Get(i)
		require.NotNil(t, key)
		require.True(t, parts.MarkInvalid(&key))
		key.Release()
	}

	assert.Equal(t, total-1, parts.InvalidCount())

	// Replace slot 0. The invalid flag must be cleared.
	old := parts.Replace(0)
	old.Release()

	assert.False(t, parts.IsInvalid(0), "Replace() must clear the invalid flag for the new partition")
	assert.Equal(t, total-2, parts.InvalidCount(), "invalid count must decrement after Replace()")
}

// TestMarkInvalid_ConcurrentCapEnforcement verifies that the CAS-based cap
// enforcement never allows more than maxInvalid entries to be stored, even
// under high concurrent load on distinct partition indices.
func TestMarkInvalid_ConcurrentCapEnforcement(t *testing.T) {
	t.Parallel()

	const total uint64 = 200
	const limit uint64 = 50
	const goroutines = 20

	parts := createTestPartitionsWithMaxInvalid(t, total, limit)

	keys := make([]typedef.PartitionKeys, total)
	for i := range total {
		keys[i] = parts.Get(i)
	}
	t.Cleanup(func() {
		for i := range total {
			keys[i].Release()
		}
	})

	var wins atomic.Int64
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for range goroutines {
		go func() {
			defer wg.Done()
			for i := range total {
				if parts.MarkInvalid(&keys[i]) {
					wins.Add(1)
				}
			}
		}()
	}
	wg.Wait()

	assert.Equal(t, int64(limit), wins.Load(), "exactly maxInvalid partitions should be marked")
	assert.Equal(t, limit, parts.InvalidCount(), "InvalidCount must equal the cap")
}
