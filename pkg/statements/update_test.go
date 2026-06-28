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
	"fmt"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/typedef"
)

func TestUpdate_WithTrackedRow(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:           "test_table",
		PartitionKeys:  typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck1", Type: typedef.TypeBigint}, {Name: "ck2", Type: typedef.TypeText}},
		Columns:        typedef.Columns{{Name: "col1", Type: typedef.TypeInt}},
	}

	gen, mp := newGen(t, table, 100)

	trackedID := uuid.New()
	mp.TrackRow(partitions.TrackedRow{
		PartitionID:      trackedID,
		PartitionValues:  []any{int32(99)},
		ClusteringValues: []any{int64(123), "hello"},
	})

	stmt, err := gen.Update(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)

	assert.Equal(t, typedef.UpdateStatementType, stmt.QueryType)
	assert.Contains(t, stmt.Query, "UPDATE")
	assert.Contains(t, stmt.Query, "col1=?")
	assert.Contains(t, stmt.Query, "pk1=?")
	assert.Contains(t, stmt.Query, "ck1=?")
	assert.Contains(t, stmt.Query, "ck2=?")

	// Values are [SET values..., PK values..., CK values...]. The single SET
	// column contributes one value; the tracked PK+CK must be the suffix.
	require.Len(t, stmt.Values, 4)
	assert.Equal(t, []any{int32(99), int64(123), "hello"}, stmt.Values[1:])

	require.Len(t, stmt.PartitionKeys, 1)
	assert.Equal(t, trackedID, stmt.PartitionKeys[0].ID, "Stmt must carry the tracked row's partition ID")
}

func TestUpdate_FallsBackToRandomWhenNoTrackedRows(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:           "test_table",
		PartitionKeys:  typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck1", Type: typedef.TypeBigint}},
		Columns:        typedef.Columns{{Name: "col1", Type: typedef.TypeInt}},
	}

	gen, _ := newGen(t, table, 100)

	stmt, err := gen.Update(t.Context())
	require.NoError(t, err, "empty tracker must fall back to a random-row update, not error")
	require.NotNil(t, stmt)

	assert.Equal(t, typedef.UpdateStatementType, stmt.QueryType)
	assert.Contains(t, stmt.Query, "pk1=?")
	assert.Contains(t, stmt.Query, "ck1=?")
	// mockPartitionsWithTracker.Next() returns pk1=42.
	require.Len(t, stmt.Values, 3) // col1, pk1, ck1
	assert.Equal(t, int32(42), stmt.Values[1])
}

func TestUpdate_NoClusteringKeys(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:          "test_table",
		PartitionKeys: typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		Columns:       typedef.Columns{{Name: "col1", Type: typedef.TypeInt}},
	}

	gen, mp := newGen(t, table, 100)

	trackedID := uuid.New()
	mp.TrackRow(partitions.TrackedRow{
		PartitionID:     trackedID,
		PartitionValues: []any{int32(7)},
	})

	stmt, err := gen.Update(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)

	assert.Equal(t, typedef.UpdateStatementType, stmt.QueryType)
	assert.Contains(t, stmt.Query, "pk1=?")
	assert.NotContains(t, stmt.Query, "ck1=?")
	require.Len(t, stmt.Values, 2) // col1, pk1
	assert.Equal(t, int32(7), stmt.Values[1])
	assert.Equal(t, trackedID, stmt.PartitionKeys[0].ID)
}

func TestUpdate_CounterTable(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:           "counter_table",
		PartitionKeys:  typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck1", Type: typedef.TypeBigint}},
		Columns:        typedef.Columns{{Name: "col1", Type: &typedef.CounterType{}}},
	}
	require.True(t, table.IsCounterTable())

	gen, mp := newGen(t, table, 100)

	trackedID := uuid.New()
	mp.TrackRow(partitions.TrackedRow{
		PartitionID:      trackedID,
		PartitionValues:  []any{int32(5)},
		ClusteringValues: []any{int64(50)},
	})

	stmt, err := gen.Update(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)

	assert.Equal(t, typedef.UpdateStatementType, stmt.QueryType)
	// Counter SET uses the increment form and binds no value.
	assert.Contains(t, stmt.Query, "col1=col1+1")
	// Only the PK + CK values are bound (no SET value for the counter).
	assert.Equal(t, []any{int32(5), int64(50)}, stmt.Values)
}

func makeWideTable(name string, nCols int) *typedef.Table {
	cols := make(typedef.Columns, nCols)
	for i := range nCols {
		cols[i] = typedef.ColumnDef{Name: fmt.Sprintf("col%d", i), Type: typedef.TypeInt}
	}
	return &typedef.Table{
		Name:           name,
		PartitionKeys:  typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck1", Type: typedef.TypeBigint}},
		Columns:        cols,
	}
}

func TestUpdate_VariantPoolIsBoundedAndCoversAllColumns(t *testing.T) {
	t.Parallel()

	for _, nCols := range []int{1, 2, 7, 8, 20, 64} {
		t.Run(fmt.Sprintf("cols=%d", nCols), func(t *testing.T) {
			t.Parallel()
			gen, _ := newGen(t, makeWideTable("wide", nCols), 0)

			// Bounded: never more cached UPDATE queries than the cap, regardless
			// of table width — this is what keeps gocql's prepared-statement
			// cache from thrashing.
			require.LessOrEqual(t, len(gen.updateVariants), maxUpdateVariants)
			require.NotEmpty(t, gen.updateVariants)

			// Every column is covered by at least one variant, and every variant
			// is non-empty (an empty SET is invalid CQL).
			covered := make(map[int]bool, nCols)
			for _, v := range gen.updateVariants {
				require.NotEmpty(t, v.columns, "variant must SET at least one column")
				for _, ci := range v.columns {
					covered[ci] = true
				}
			}
			for i := range nCols {
				assert.True(t, covered[i], "column %d not covered by any update variant", i)
			}
		})
	}
}

func TestUpdate_VariantsWriteDifferentFields(t *testing.T) {
	t.Parallel()

	table := makeWideTable("fields", 6)
	gen, _ := newGen(t, table, 0) // trackerCap 0 => always the random path

	queries := make(map[string]int)
	for range 400 {
		stmt, err := gen.Update(t.Context())
		require.NoError(t, err)

		// Bind values must be exactly one per SET column + 1 PK + 1 CK, proving
		// the variant's columns / setValuesLen stay aligned with the query.
		setCount := 0
		for i := range 6 {
			if strings.Contains(stmt.Query, fmt.Sprintf("col%d=?", i)) {
				setCount++
			}
		}
		require.GreaterOrEqual(t, setCount, 1)
		assert.Len(t, stmt.Values, setCount+2)
		queries[stmt.Query]++
	}

	// More than one distinct SET shape is actually exercised at runtime.
	assert.Greater(t, len(queries), 1, "expected updates to write different field subsets")
}

func TestUpdate_TrackedMissCountsFallback(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:           "uniq_update_miss_table",
		PartitionKeys:  typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck1", Type: typedef.TypeBigint}},
		Columns:        typedef.Columns{{Name: "col1", Type: typedef.TypeInt}},
	}

	gen, mp := newGen(t, table, 100)

	// A tracked row with no PartitionValues is too short for the schema, so the
	// targeted update must fall back AND record the mismatch on the generator.
	mp.TrackRow(partitions.TrackedRow{
		PartitionID:      uuid.New(),
		PartitionValues:  []any{},
		ClusteringValues: []any{int64(1)},
	})

	stmt, err := gen.Update(t.Context())
	require.NoError(t, err, "schema-short tracked row must fall back, not error")
	require.NotNil(t, stmt)
	assert.Equal(t, typedef.UpdateStatementType, stmt.QueryType)

	// The fallback is accounted on the generator; jobs drains this into metrics.
	got := gen.DrainTrackedMisses()
	assert.Equal(t, TrackedMissCounts{Update: 1}, got)

	// Drain is destructive — a second call sees zero.
	assert.Equal(t, TrackedMissCounts{}, gen.DrainTrackedMisses())
}

func TestUpdate_NoColumnsFallsBackToInsert(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:           "keys_only_table",
		PartitionKeys:  typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck1", Type: typedef.TypeBigint}},
	}

	gen, _ := newGen(t, table, 100)
	assert.Empty(t, gen.updateVariants, "no SET columns means no cached update variants")

	stmt, err := gen.Update(t.Context())
	require.NoError(t, err)
	require.NotNil(t, stmt)
	assert.Equal(t, typedef.InsertStatementType, stmt.QueryType, "update with no SET columns must fall back to insert")
}
