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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

// rowGetter returns the values of keys as a scanned row holds them: through
// a pointer, as the typed scan slabs return them.
func rowGetter(t *testing.T, keys typedef.PartitionKeys) func(string) any {
	t.Helper()

	return func(name string) any {
		values := keys.Values.Get(name)
		require.Len(t, values, 1)
		switch v := values[0].(type) {
		case int32:
			return &v
		case string:
			return &v
		default:
			return v
		}
	}
}

func TestKeyFingerprintMatchesScannedValues(t *testing.T) {
	t.Parallel()

	parts := createTestPartitions(t, 10)
	keys := parts.Get(3)
	t.Cleanup(keys.Release)

	fromKeys, ok := parts.keysFingerprint(&keys)
	require.True(t, ok)

	fromRow, ok := KeyFingerprint(parts.table.PartitionKeys, rowGetter(t, keys))
	require.True(t, ok)
	assert.Equal(t, fromKeys, fromRow, "a scanned row and the keys of its partition give one fingerprint")

	other := parts.Get(4)
	t.Cleanup(other.Release)
	fromOther, ok := parts.keysFingerprint(&other)
	require.True(t, ok)
	assert.NotEqual(t, fromKeys, fromOther)
}

func TestKeyFingerprintNeedsEveryColumn(t *testing.T) {
	t.Parallel()

	_, ok := KeyFingerprint(createTestTable().PartitionKeys, func(name string) any {
		if name == "pk1" {
			return int32(1)
		}
		return nil
	})
	assert.False(t, ok)
}

// TestClaimDivergedKey: a partition is reported once, by MarkInvalid or by an
// index query, whichever sees it first.
func TestClaimDivergedKey(t *testing.T) {
	t.Parallel()

	t.Run("index query after MarkInvalid", func(t *testing.T) {
		t.Parallel()

		parts := createTestPartitions(t, 10)
		keys := parts.Get(1)
		t.Cleanup(keys.Release)
		fp, ok := KeyFingerprint(parts.table.PartitionKeys, rowGetter(t, keys))
		require.True(t, ok)

		require.True(t, parts.MarkInvalid(&keys))
		assert.False(t, parts.ClaimDivergedKey(fp), "gemini already knows that the partition differs")
	})

	t.Run("MarkInvalid after index query", func(t *testing.T) {
		t.Parallel()

		parts := createTestPartitions(t, 10)
		keys := parts.Get(2)
		t.Cleanup(keys.Release)
		fp, ok := KeyFingerprint(parts.table.PartitionKeys, rowGetter(t, keys))
		require.True(t, ok)

		require.True(t, parts.ClaimDivergedKey(fp))
		assert.False(t, parts.ClaimDivergedKey(fp), "a second index query does not report it again")
		assert.False(t, parts.MarkInvalid(&keys), "the index query already reported the partition")
		assert.True(t, parts.IsInvalid(2), "selection must still skip the partition")
		assert.Equal(t, uint64(1), parts.InvalidCount())
	})
}
