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

package partitions

import (
	"math/rand/v2"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/typedef"
)

// createTestPartitionsWithTracker builds a *Partitions with a live row tracker
// of the given capacity so TrackRow / PopTrackedRow / FillRatio paths are reachable.
func createTestPartitionsWithTracker(t *testing.T, count uint64, trackerCap int) *Partitions {
	t.Helper()
	src, fn := distributions.New(distributions.Uniform, count, 1, 0, 0)
	table := &typedef.Table{
		Name: "test_table",
		PartitionKeys: typedef.Columns{
			{Name: "pk1", Type: typedef.TypeInt},
		},
		ClusteringKeys: typedef.Columns{
			{Name: "ck1", Type: typedef.TypeBigint},
		},
	}
	config := typedef.PartitionRangeConfig{
		MaxBlobLength:      100,
		MinBlobLength:      10,
		MaxStringLength:    50,
		MinStringLength:    5,
		RowTrackerCapacity: trackerCap,
	}
	parts := New(t.Context(), rand.New(src), fn, table, config, count, 0)
	t.Cleanup(parts.Close)
	return parts
}

// ---------------------------------------------------------------------------
// TrackRow / PopTrackedRow / TrackedRowCount
// ---------------------------------------------------------------------------

func TestPartitions_TrackRow_PopTrackedRow(t *testing.T) {
	t.Parallel()

	p := createTestPartitionsWithTracker(t, 10, 50)

	id := uuid.New()
	row := TrackedRow{
		PartitionID:      id,
		PartitionValues:  []any{int32(7)},
		ClusteringValues: []any{int64(99)},
	}

	p.TrackRow(row)
	assert.Equal(t, uint64(1), p.TrackedRowCount())

	got, ok := p.PopTrackedRow()
	require.True(t, ok)
	assert.Equal(t, id, got.PartitionID)
	assert.Equal(t, []any{int64(99)}, got.ClusteringValues)
	assert.Equal(t, uint64(0), p.TrackedRowCount())
}

func TestPartitions_PopTrackedRow_Empty(t *testing.T) {
	t.Parallel()

	p := createTestPartitionsWithTracker(t, 10, 50)

	_, ok := p.PopTrackedRow()
	assert.False(t, ok, "PopTrackedRow must return false when tracker is empty")
}

func TestPartitions_TrackRow_NilTracker(t *testing.T) {
	t.Parallel()

	// Capacity 0 disables the tracker; calls must be no-ops.
	p := createTestPartitionsWithTracker(t, 10, 0)

	p.TrackRow(TrackedRow{PartitionID: uuid.New(), PartitionValues: []any{int32(1)}})
	assert.Equal(t, uint64(0), p.TrackedRowCount())

	_, ok := p.PopTrackedRow()
	assert.False(t, ok)
}

// ---------------------------------------------------------------------------
// RowTrackerFillRatio
// ---------------------------------------------------------------------------

func TestPartitions_RowTrackerFillRatio_Disabled(t *testing.T) {
	t.Parallel()

	p := createTestPartitionsWithTracker(t, 10, 0)
	assert.Equal(t, 0.0, p.RowTrackerFillRatio())
}

func TestPartitions_RowTrackerFillRatio_Empty(t *testing.T) {
	t.Parallel()

	p := createTestPartitionsWithTracker(t, 10, 10)
	assert.Equal(t, 0.0, p.RowTrackerFillRatio())
}

func TestPartitions_RowTrackerFillRatio_Half(t *testing.T) {
	t.Parallel()

	const capacity = 10
	p := createTestPartitionsWithTracker(t, 10, capacity)

	for range capacity / 2 {
		p.TrackRow(TrackedRow{PartitionID: uuid.New(), PartitionValues: []any{int32(1)}})
	}

	assert.InDelta(t, 0.5, p.RowTrackerFillRatio(), 0.001)
}

func TestPartitions_RowTrackerFillRatio_Full(t *testing.T) {
	t.Parallel()

	const capacity = 8
	p := createTestPartitionsWithTracker(t, 10, capacity)

	for range capacity {
		p.TrackRow(TrackedRow{PartitionID: uuid.New(), PartitionValues: []any{int32(1)}})
	}

	assert.Equal(t, 1.0, p.RowTrackerFillRatio())
}

// ---------------------------------------------------------------------------
// Replace invalidates tracked rows for the replaced partition
// ---------------------------------------------------------------------------

func TestPartitions_Replace_InvalidatesTrackedRows(t *testing.T) {
	t.Parallel()

	p := createTestPartitionsWithTracker(t, 10, 100)

	// Fetch the old partition at slot 0 so we know its UUID.
	oldKeys := p.Get(0)
	oldID := oldKeys.ID

	// Track a row belonging to that partition.
	p.TrackRow(TrackedRow{
		PartitionID:      oldID,
		PartitionValues:  []any{int32(1)},
		ClusteringValues: []any{int64(42)},
	})
	// Also track a row for a different (fake) partition so it survives.
	otherID := uuid.New()
	p.TrackRow(TrackedRow{
		PartitionID:      otherID,
		PartitionValues:  []any{int32(2)},
		ClusteringValues: []any{int64(99)},
	})
	require.Equal(t, uint64(2), p.TrackedRowCount())

	// Replace slot 0 — must invalidate the oldID tracked row.
	p.Replace(0)

	// Drain the tracker; only the otherID row must come out.
	var poppedIDs []uuid.UUID
	for {
		row, ok := p.PopTrackedRow()
		if !ok {
			break
		}
		poppedIDs = append(poppedIDs, row.PartitionID)
	}

	require.Len(t, poppedIDs, 1)
	assert.Equal(t, otherID, poppedIDs[0], "only the unrelated row must survive Replace")
}
