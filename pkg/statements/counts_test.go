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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

func countsTable(pks, cks, indexes int) *typedef.Table {
	table := &typedef.Table{Name: "t"}
	for i := range pks {
		table.PartitionKeys = append(table.PartitionKeys, typedef.ColumnDef{Name: "pk" + string(rune('0'+i)), Type: typedef.TypeInt})
	}
	for i := range cks {
		table.ClusteringKeys = append(table.ClusteringKeys, typedef.ColumnDef{Name: "ck" + string(rune('0'+i)), Type: typedef.TypeInt})
	}
	for i := range indexes {
		col := typedef.ColumnDef{Name: "c" + string(rune('0'+i)), Type: typedef.TypeInt}
		table.Columns = append(table.Columns, col)
		table.Indexes = append(table.Indexes, typedef.IndexDef{IndexName: "i" + col.Name, ColumnName: col.Name, Column: col})
	}

	return table
}

// TestMultiplePartitionCountCoversItsRange: a multi-partition statement must
// name more than one partition on tables with one or two partition-key
// columns, and must keep count^columns below MaxCartesianProductCount.
func TestMultiplePartitionCountCoversItsRange(t *testing.T) {
	t.Parallel()

	for pks := 1; pks <= 6; pks++ {
		g, _ := newGen(t, countsTable(pks, 1, 0), 10)
		maxCount := g.maxMultiplePartitionCount()

		require.Less(t, math.Pow(float64(maxCount), float64(pks)), MaxCartesianProductCount)
		require.LessOrEqual(t, maxCount, maxMultiplePartitionKeys)
		require.Len(t, g.selectMultiplePartitionQueries, maxCount+1, "every count must have a cached query")

		seen := map[int]bool{}
		for range 2000 {
			n := g.getMultiplePartitionKeys()
			require.GreaterOrEqual(t, n, 1)
			require.LessOrEqual(t, n, maxCount)
			seen[n] = true
		}

		assert.Len(t, seen, maxCount, "%d partition-key columns: every count from 1 to %d must occur", pks, maxCount)
		if maxCount > 1 {
			assert.True(t, seen[maxCount])
		}
	}
}

// TestClusteringRangeCanUseEveryColumn: the ranged clustering column must be
// able to be the last one.
func TestClusteringRangeCanUseEveryColumn(t *testing.T) {
	t.Parallel()

	g, _ := newGen(t, countsTable(1, 4, 0), 10)

	seen := map[int]bool{}
	for range 2000 {
		seen[g.getMultipleClusteringKeys()] = true
	}

	assert.Equal(t, map[int]bool{1: true, 2: true, 3: true, 4: true}, seen)
}

// TestIndexCountIsNeverZero: zero indexed columns gives a SELECT without a
// WHERE clause, which scans the whole table.
func TestIndexCountIsNeverZero(t *testing.T) {
	t.Parallel()

	g, _ := newGen(t, countsTable(1, 1, 3), 10)

	seen := map[int]bool{}
	for range 2000 {
		seen[g.getIndexCount()] = true
	}

	assert.Equal(t, map[int]bool{1: true, 2: true, 3: true}, seen)

	stmt := g.genSingleIndexQuery()
	assert.Contains(t, stmt.Query, "WHERE")
}
