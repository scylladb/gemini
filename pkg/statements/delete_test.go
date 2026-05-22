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

package statements

import (
	"math/rand/v2"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/typedef"
)

// mockPartitionsWithTracker extends mockPartitions with row tracking support.
type mockPartitionsWithTracker struct {
	tracker *partitions.RowTracker
	mockPartitions
}

func newMockPartitionsWithTracker(count, trackerCapacity uint64) *mockPartitionsWithTracker {
	return &mockPartitionsWithTracker{
		mockPartitions: *newMockPartitions(count),
		tracker:        partitions.NewRowTracker(trackerCapacity),
	}
}

func (m *mockPartitionsWithTracker) Next() typedef.PartitionKeys {
	return typedef.PartitionKeys{
		ID: uuid.New(),
		Values: typedef.NewValuesFromMap(map[string][]any{
			"pk1": {int32(42)},
		}),
	}
}

func (m *mockPartitionsWithTracker) ReplaceNext() typedef.PartitionKeys {
	return m.Next()
}

func (m *mockPartitionsWithTracker) TrackRow(row partitions.TrackedRow) {
	m.tracker.Push(row)
}

func (m *mockPartitionsWithTracker) PopTrackedRow() (partitions.TrackedRow, bool) {
	return m.tracker.Pop()
}

func (m *mockPartitionsWithTracker) TrackedRowCount() uint64 {
	return m.tracker.Len()
}

func (m *mockPartitionsWithTracker) RowTrackerFillRatio() float64 {
	return m.tracker.FillRatio()
}

func TestDeleteSingleRow_WithTrackedRow(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name: "test_table",
		PartitionKeys: typedef.Columns{
			{Name: "pk1", Type: typedef.TypeInt},
		},
		ClusteringKeys: typedef.Columns{
			{Name: "ck1", Type: typedef.TypeBigint},
			{Name: "ck2", Type: typedef.TypeText},
		},
	}

	mp := newMockPartitionsWithTracker(10, 100)
	t.Cleanup(mp.Close)

	// Push a tracked row
	trackedID := uuid.New()
	mp.TrackRow(partitions.TrackedRow{
		PartitionID:      trackedID,
		PartitionValues:  []any{int32(99)},
		ClusteringValues: []any{int64(123), "hello"},
	})

	ratios := DefaultStatementRatios()
	rng := rand.New(rand.NewChaCha8([32]byte{}))
	rc, err := NewRatioController(ratios, rng)
	require.NoError(t, err)

	vc := &typedef.ValueRangeConfig{
		MaxBlobLength:   32,
		MinBlobLength:   1,
		MaxStringLength: 32,
		MinStringLength: 1,
	}

	gen := New("ks", mp, table, rng, vc, rc, false)

	stmt, err := gen.deleteSingleRow(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)

	// Should contain partition key and ALL clustering keys
	assert.Equal(t, typedef.DeleteSingleRowType, stmt.QueryType)
	assert.Contains(t, stmt.Query, "pk1=?")
	assert.Contains(t, stmt.Query, "ck1=?")
	assert.Contains(t, stmt.Query, "ck2=?")

	// Values should be: pk1=99, ck1=123, ck2="hello"
	assert.Equal(t, []any{int32(99), int64(123), "hello"}, stmt.Values)
}

func TestDeleteSingleRow_FallsBackWhenNoTrackedRows(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name: "test_table",
		PartitionKeys: typedef.Columns{
			{Name: "pk1", Type: typedef.TypeInt},
		},
		ClusteringKeys: typedef.Columns{
			{Name: "ck1", Type: typedef.TypeBigint},
		},
	}

	mp := newMockPartitionsWithTracker(10, 100)
	t.Cleanup(mp.Close)
	ratios := DefaultStatementRatios()
	rng := rand.New(rand.NewChaCha8([32]byte{}))
	rc, err := NewRatioController(ratios, rng)
	require.NoError(t, err)

	vc := &typedef.ValueRangeConfig{
		MaxBlobLength:   32,
		MinBlobLength:   1,
		MaxStringLength: 32,
		MinStringLength: 1,
	}

	gen := New("ks", mp, table, rng, vc, rc, false)

	stmt, err := gen.deleteSingleRow(t.Context())
	require.NoError(t, err, "empty tracker must fall back, not error")
	require.NotNil(t, stmt)
	assert.Equal(t, typedef.DeleteWholePartitionType, stmt.QueryType, "fallback must be a whole-partition delete")
	assert.Contains(t, stmt.Query, "pk1=?")
	assert.NotContains(t, stmt.Query, "ck1=?", "whole-partition fallback must not bind clustering keys")
}

func TestDeleteClusteringSubset_FallsBackWhenNoTrackedRows(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name: "test_table",
		PartitionKeys: typedef.Columns{
			{Name: "pk1", Type: typedef.TypeInt},
		},
		ClusteringKeys: typedef.Columns{
			{Name: "ck1", Type: typedef.TypeBigint},
			{Name: "ck2", Type: typedef.TypeInt},
		},
	}

	mp := newMockPartitionsWithTracker(10, 100)
	t.Cleanup(mp.Close)

	ratios := DefaultStatementRatios()
	rng := rand.New(rand.NewChaCha8([32]byte{}))
	rc, err := NewRatioController(ratios, rng)
	require.NoError(t, err)

	vc := &typedef.ValueRangeConfig{
		MaxBlobLength:   32,
		MinBlobLength:   1,
		MaxStringLength: 32,
		MinStringLength: 1,
	}

	gen := New("ks", mp, table, rng, vc, rc, false)

	stmt, err := gen.deleteClusteringSubset(t.Context())
	require.NoError(t, err, "empty tracker must fall back, not error")
	require.NotNil(t, stmt)
	assert.Equal(t, typedef.DeleteWholePartitionType, stmt.QueryType, "fallback must be a whole-partition delete")
	assert.Contains(t, stmt.Query, "pk1=?")
}

func TestDeleteSingleRow_NoClustering(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name: "test_table",
		PartitionKeys: typedef.Columns{
			{Name: "pk1", Type: typedef.TypeInt},
		},
		ClusteringKeys: nil, // No clustering keys
	}

	mp := newMockPartitionsWithTracker(10, 100)
	t.Cleanup(mp.Close)

	ratios := DefaultStatementRatios()
	rng := rand.New(rand.NewChaCha8([32]byte{}))
	rc, err := NewRatioController(ratios, rng)
	require.NoError(t, err)

	vc := &typedef.ValueRangeConfig{
		MaxBlobLength:   32,
		MinBlobLength:   1,
		MaxStringLength: 32,
		MinStringLength: 1,
	}

	gen := New("ks", mp, table, rng, vc, rc, false)

	stmt, err := gen.deleteSingleRow(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)

	// With no CKs, single row delete is the same as whole partition delete
	assert.Equal(t, typedef.DeleteWholePartitionType, stmt.QueryType)
	assert.Contains(t, stmt.Query, "pk1=?")
}

func TestDeleteClusteringSubset_WithTrackedRow(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name: "test_table",
		PartitionKeys: typedef.Columns{
			{Name: "pk1", Type: typedef.TypeInt},
		},
		ClusteringKeys: typedef.Columns{
			{Name: "ck1", Type: typedef.TypeBigint},
			{Name: "ck2", Type: typedef.TypeText},
			{Name: "ck3", Type: typedef.TypeInt},
		},
	}

	mp := newMockPartitionsWithTracker(10, 100)
	t.Cleanup(mp.Close)

	// Push a tracked row with full CK values
	trackedID := uuid.New()
	mp.TrackRow(partitions.TrackedRow{
		PartitionID:      trackedID,
		PartitionValues:  []any{int32(77)},
		ClusteringValues: []any{int64(10), "world", int32(5)},
	})

	ratios := DefaultStatementRatios()
	rng := rand.New(rand.NewChaCha8([32]byte{}))
	rc, err := NewRatioController(ratios, rng)
	require.NoError(t, err)

	vc := &typedef.ValueRangeConfig{
		MaxBlobLength:   32,
		MinBlobLength:   1,
		MaxStringLength: 32,
		MinStringLength: 1,
	}

	gen := New("ks", mp, table, rng, vc, rc, false)

	stmt, err := gen.deleteClusteringSubset(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)

	// Must be a cluster (prefix) delete: pk equality + at least one CK equality,
	// but fewer than all CK columns (so it deletes a cluster, not a single row).
	assert.Equal(t, typedef.DeleteClusteringSubsetType, stmt.QueryType)
	assert.Contains(t, stmt.Query, "pk1=?")
	// Must have at least one CK equality condition.
	assert.Contains(t, stmt.Query, "ck1=?", "expected at least ck1 equality in cluster delete query")
	// Must NOT have range operators (this is a prefix delete, not a range tombstone).
	assert.NotContains(t, stmt.Query, ">?", "cluster delete must not use range operators")
	assert.NotContains(t, stmt.Query, "<=?", "cluster delete must not use range operators")
}

// ---------------------------------------------------------------------------
// Helper
// ---------------------------------------------------------------------------

func newGen(t *testing.T, table *typedef.Table, trackerCap uint64) (*Generator, *mockPartitionsWithTracker) {
	t.Helper()
	mp := newMockPartitionsWithTracker(10, trackerCap)
	t.Cleanup(mp.Close)
	ratios := DefaultStatementRatios()
	rng := rand.New(rand.NewChaCha8([32]byte{42}))
	rc, err := NewRatioController(ratios, rng)
	require.NoError(t, err)
	vc := &typedef.ValueRangeConfig{MaxBlobLength: 32, MinBlobLength: 1, MaxStringLength: 32, MinStringLength: 1}
	return New("ks", mp, table, rng, vc, rc, false), mp
}

func TestDeleteSingleRow_FallsBackOnShortPartitionValues(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:           "test_table",
		PartitionKeys:  typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck1", Type: typedef.TypeBigint}},
	}

	gen, mp := newGen(t, table, 100)

	// Push a row with no PartitionValues — too short for the PK extraction loop.
	mp.TrackRow(partitions.TrackedRow{
		PartitionID:      uuid.New(),
		PartitionValues:  []any{}, // empty — triggers bounds check
		ClusteringValues: []any{int64(1)},
	})

	stmt, err := gen.deleteSingleRow(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)
	assert.Equal(t, typedef.DeleteWholePartitionType, stmt.QueryType)
}

func TestDeleteSingleRow_FallsBackOnShortClusteringValues(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:           "test_table",
		PartitionKeys:  typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck1", Type: typedef.TypeBigint}, {Name: "ck2", Type: typedef.TypeText}},
	}

	gen, mp := newGen(t, table, 100)

	// ClusteringValues has only 1 entry but table has 2 CKs.
	mp.TrackRow(partitions.TrackedRow{
		PartitionID:      uuid.New(),
		PartitionValues:  []any{int32(5)},
		ClusteringValues: []any{int64(1)}, // missing ck2 — too short
	})

	stmt, err := gen.deleteSingleRow(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)
	assert.Equal(t, typedef.DeleteWholePartitionType, stmt.QueryType)
}

func TestDeleteClusteringSubset_FallsBackOnShortPartitionValues(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:           "test_table",
		PartitionKeys:  typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck1", Type: typedef.TypeBigint}, {Name: "ck2", Type: typedef.TypeInt}},
	}

	gen, mp := newGen(t, table, 100)

	mp.TrackRow(partitions.TrackedRow{
		PartitionID:      uuid.New(),
		PartitionValues:  []any{}, // empty
		ClusteringValues: []any{int64(99)},
	})

	stmt, err := gen.deleteClusteringSubset(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)
	assert.Equal(t, typedef.DeleteWholePartitionType, stmt.QueryType)
}

func TestDeleteClusteringSubset_FallsBackOnShortClusteringValues(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:           "test_table",
		PartitionKeys:  typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck1", Type: typedef.TypeBigint}, {Name: "ck2", Type: typedef.TypeInt}},
	}

	gen, mp := newGen(t, table, 100)

	mp.TrackRow(partitions.TrackedRow{
		PartitionID:      uuid.New(),
		PartitionValues:  []any{int32(3)},
		ClusteringValues: []any{}, // empty — triggers bounds check
	})

	stmt, err := gen.deleteClusteringSubset(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)
	assert.Equal(t, typedef.DeleteWholePartitionType, stmt.QueryType)
}

func TestDeleteSingleRow_StmtPKCarriesID(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:           "test_table",
		PartitionKeys:  typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck1", Type: typedef.TypeBigint}},
	}

	gen, mp := newGen(t, table, 100)

	trackedID := uuid.New()
	mp.TrackRow(partitions.TrackedRow{
		PartitionID:      trackedID,
		PartitionValues:  []any{int32(7)},
		ClusteringValues: []any{int64(77)},
	})

	stmt, err := gen.deleteSingleRow(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)
	require.Equal(t, typedef.DeleteSingleRowType, stmt.QueryType)

	require.Len(t, stmt.PartitionKeys, 1)
	assert.Equal(t, trackedID, stmt.PartitionKeys[0].ID, "Stmt must carry the tracked row's partition ID")
}

func TestDeleteClusteringSubset_StmtPKCarriesID(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:           "test_table",
		PartitionKeys:  typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck1", Type: typedef.TypeBigint}, {Name: "ck2", Type: typedef.TypeInt}},
	}

	gen, mp := newGen(t, table, 100)

	trackedID := uuid.New()
	mp.TrackRow(partitions.TrackedRow{
		PartitionID:      trackedID,
		PartitionValues:  []any{int32(3)},
		ClusteringValues: []any{int64(33), int32(7)},
	})

	stmt, err := gen.deleteClusteringSubset(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)
	require.Equal(t, typedef.DeleteClusteringSubsetType, stmt.QueryType)

	require.Len(t, stmt.PartitionKeys, 1)
	assert.Equal(t, trackedID, stmt.PartitionKeys[0].ID)
}
