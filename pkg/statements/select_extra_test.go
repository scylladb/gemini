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

func newSelectRangeTestGenerator(t *testing.T, table *typedef.Table, mp *mockPartitionsWithTracker) *Generator {
	t.Helper()

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

	return New("ks", mp, table, rng, vc, rc, false)
}

func singleClusteringKeyTable() *typedef.Table {
	return &typedef.Table{
		Name:          "test_table",
		PartitionKeys: typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{
			{Name: "ck1", Type: typedef.TypeBigint},
		},
		Columns: typedef.Columns{{Name: "v", Type: typedef.TypeInt}},
	}
}

func TestGenClusteringRangeQuery_WithTrackedRow(t *testing.T) {
	t.Parallel()

	table := singleClusteringKeyTable()
	mp := newMockPartitionsWithTracker(10, 100)
	t.Cleanup(mp.Close)

	trackedID := uuid.New()
	mp.TrackRow(partitions.TrackedRow{
		PartitionID:      trackedID,
		PartitionValues:  []any{int32(99)},
		ClusteringValues: []any{int64(123)},
	})

	gen := newSelectRangeTestGenerator(t, table, mp)

	stmt, err := gen.genClusteringRangeQuery(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)

	assert.Equal(t, typedef.SelectRangeStatementType, stmt.QueryType)
	assert.Contains(t, stmt.Query, "pk1=?")
	assert.Contains(t, stmt.Query, "ck1>=?")
	assert.Contains(t, stmt.Query, "ck1<=?")

	// Both range bounds must bracket the tracked row's actual value so the
	// query is guaranteed to match it, instead of two independent random
	// values that (almost) never overlap real data.
	assert.Equal(t, []any{int32(99), int64(123), int64(123)}, stmt.Values)
	require.Len(t, stmt.PartitionKeys, 1)
	assert.Equal(t, trackedID, stmt.PartitionKeys[0].ID)
}

func TestGenClusteringRangeQuery_FallsBackWhenNoTrackedRows(t *testing.T) {
	t.Parallel()

	table := singleClusteringKeyTable()
	mp := newMockPartitionsWithTracker(10, 100)
	t.Cleanup(mp.Close)

	gen := newSelectRangeTestGenerator(t, table, mp)

	stmt, err := gen.genClusteringRangeQuery(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)
	assert.Equal(t, typedef.SelectStatementType, stmt.QueryType)
}

func TestGenClusteringRangeQuery_NoClusteringKeysFallsBack(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:          "test_table",
		PartitionKeys: typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		Columns:       typedef.Columns{{Name: "v", Type: typedef.TypeInt}},
	}
	mp := newMockPartitionsWithTracker(10, 100)
	t.Cleanup(mp.Close)

	gen := newSelectRangeTestGenerator(t, table, mp)

	stmt, err := gen.genClusteringRangeQuery(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)
	assert.Equal(t, typedef.SelectStatementType, stmt.QueryType)
}

func TestGenMultiplePartitionClusteringRangeQuery_WithTrackedRow(t *testing.T) {
	t.Parallel()

	table := singleClusteringKeyTable()
	mp := newMockPartitionsWithTracker(10, 100)
	t.Cleanup(mp.Close)

	trackedID := uuid.New()
	mp.TrackRow(partitions.TrackedRow{
		PartitionID:      trackedID,
		PartitionValues:  []any{int32(99)},
		ClusteringValues: []any{int64(123)},
	})

	gen := newSelectRangeTestGenerator(t, table, mp)

	stmt, err := gen.genMultiplePartitionClusteringRangeQuery(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)

	assert.Equal(t, typedef.SelectMultiPartitionRangeStatementType, stmt.QueryType)
	assert.Contains(t, stmt.Query, "pk1 IN")
	assert.Contains(t, stmt.Query, "ck1>=?")
	assert.Contains(t, stmt.Query, "ck1<=?")

	// The tracked row's own partition must always be one of the IN-tupled
	// partitions and its clustering value must be present as both range
	// bounds, so the query is guaranteed to match at least that row.
	found := false
	for _, pk := range stmt.PartitionKeys {
		if pk.ID == trackedID {
			found = true
		}
	}
	assert.True(t, found, "tracked row's partition must be included in the IN-tuple")
	assert.Contains(t, stmt.Values, int32(99))
	assert.Contains(t, stmt.Values, int64(123))
}

func TestGenMultiplePartitionClusteringRangeQuery_FallsBackWhenNoTrackedRows(t *testing.T) {
	t.Parallel()

	table := singleClusteringKeyTable()
	mp := newMockPartitionsWithTracker(10, 100)
	t.Cleanup(mp.Close)

	gen := newSelectRangeTestGenerator(t, table, mp)

	stmt, err := gen.genMultiplePartitionClusteringRangeQuery(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)
	assert.Equal(t, typedef.SelectMultiPartitionType, stmt.QueryType)
}
