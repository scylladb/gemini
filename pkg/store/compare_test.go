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

package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

// Helper to quickly construct a row with arbitrary column order
func makeRow(cols []string, vals []any) Row { return NewRow(cols, vals) }

func TestCompareCollectedRows_MultiPKMultiCK_SortingAndDiff(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		PartitionKeys: []typedef.ColumnDef{
			{Name: "pk1", Type: typedef.TypeInt},
			{Name: "pk2", Type: typedef.TypeText},
		},
		ClusteringKeys: []typedef.ColumnDef{
			{Name: "ck1", Type: typedef.TypeInt},
			{Name: "ck2", Type: typedef.TypeInt},
		},
	}

	// Three distinct keys across two partitions, columns shuffled per row
	oracle := Rows{
		makeRow([]string{"val", "ck2", "pk2", "pk1", "ck1"}, []any{100, 2, "A", 1, 1}), // (1,"A",1,2)
		makeRow([]string{"ck1", "pk1", "val", "pk2", "ck2"}, []any{1, 1, 200, "B", 1}), // (1,"B",1,1)
		makeRow([]string{"pk1", "pk2", "ck1", "ck2", "val"}, []any{2, "A", 2, 3, 300}), // (2,"A",2,3)
	}

	// Same keys and column sets, but one row differs by value only
	test := Rows{
		makeRow([]string{"ck2", "pk2", "ck1", "val", "pk1"}, []any{2, "A", 1, 101, 1}), // differs only in val vs oracle (100)
		makeRow([]string{"pk1", "ck1", "ck2", "val", "pk2"}, []any{1, 1, 1, 200, "B"}),
		makeRow([]string{"val", "ck1", "pk2", "ck2", "pk1"}, []any{300, 2, "A", 3, 2}),
	}

	res := CompareCollectedRows(table, test, oracle)

	// Two should match, one should differ (same PK/CK)
	assert.Equal(t, 2, res.MatchCount)
	require.Len(t, res.DifferentRows, 1)

	diff := res.DifferentRows[0]
	// Ensure the differing row is aligned by keys (1, "A", 1, 2)
	assert.Equal(t, 1, diff.TestRow.Get("pk1"))
	assert.Equal(t, "A", diff.TestRow.Get("pk2"))
	assert.Equal(t, 1, diff.TestRow.Get("ck1"))
	assert.Equal(t, 2, diff.TestRow.Get("ck2"))
}

func TestCompareCollectedRows_Deduplicate_LastWins(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		PartitionKeys:  []typedef.ColumnDef{{Name: "pk", Type: typedef.TypeText}},
		ClusteringKeys: []typedef.ColumnDef{{Name: "ck", Type: typedef.TypeInt}},
	}

	// Duplicate key on test side: last occurrence should win and match oracle
	t1 := makeRow([]string{"pk", "ck", "v"}, []any{"X", 1, "old"})
	t2 := makeRow([]string{"v", "ck", "pk"}, []any{"new", 1, "X"}) // same key, updated value
	t3 := makeRow([]string{"pk", "ck", "v"}, []any{"Y", 2, "same"})

	o1 := makeRow([]string{"pk", "ck", "v"}, []any{"X", 1, "new"})
	o2 := makeRow([]string{"pk", "ck", "v"}, []any{"Y", 2, "same"})

	res := CompareCollectedRows(table, Rows{t1, t2, t3}, Rows{o1, o2})

	assert.Empty(t, res.DifferentRows)
	assert.Equal(t, 2, res.MatchCount)
	assert.NoError(t, res.ToError())
}

func TestRowKeyString_CompositeFormatting(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		PartitionKeys:  []typedef.ColumnDef{{Name: "pk1", Type: typedef.TypeInt}, {Name: "pk2", Type: typedef.TypeText}},
		ClusteringKeys: []typedef.ColumnDef{{Name: "ck", Type: typedef.TypeInt}},
	}
	row := makeRow([]string{"ck", "val", "pk2", "pk1"}, []any{7, true, "B", 3})

	got := rowKeyString(table, row)
	// Order: pk1, pk2, ck and includes column names via formatRows
	assert.Equal(t, "pk1=3,pk2=B,ck=7", got)
}

func TestDiffRows_OnlyChangedColumnsShown(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		PartitionKeys:  []typedef.ColumnDef{{Name: "pk", Type: typedef.TypeInt}},
		ClusteringKeys: []typedef.ColumnDef{{Name: "ck", Type: typedef.TypeInt}},
	}

	oracle := makeRow([]string{"pk", "ck", "same", "diffA", "onlyOracle"}, []any{1, 2, "x", 10, "left"})
	test := makeRow([]string{"pk", "ck", "same", "diffA", "onlyTest"}, []any{1, 2, "x", 20, "right"})

	d := diffRows(table, oracle, test)

	// Header present and does not include identical columns
	require.NotEmpty(t, d)
	assert.Contains(t, d, "pk:")
	assert.NotContains(t, d, "same:")

	// Shows one minus for onlyOracle and one plus for onlyTest
	assert.Contains(t, d, "- onlyOracle: left")
	assert.Contains(t, d, "+ onlyTest: right")

	// Shows old and new values for diffA
	assert.Contains(t, d, "- diffA: 10")
	assert.Contains(t, d, "+ diffA: 20")
}

func TestRowsToKeyStrings_PreservesOrder(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		PartitionKeys:  []typedef.ColumnDef{{Name: "pk", Type: typedef.TypeInt}},
		ClusteringKeys: []typedef.ColumnDef{{Name: "ck", Type: typedef.TypeInt}},
	}

	r1 := makeRow([]string{"ck", "pk"}, []any{2, 1})
	r2 := makeRow([]string{"pk", "ck"}, []any{3, 4})

	got := rowsToKeyStrings(table, []Row{r1, r2})
	assert.Equal(t, []string{"pk=1,ck=2", "pk=3,ck=4"}, got)
}

func TestZipAndCompare_UnorderedIterators(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{PartitionKeys: []typedef.ColumnDef{{Name: "pk", Type: typedef.TypeInt}}}

	a := makeRow([]string{"pk", "v"}, []any{1, "x"})
	b := makeRow([]string{"pk", "v"}, []any{2, "y"})

	// Iterators yield in different orders
	testIter := func(yield func(Row, error) bool) {
		_ = yield(b, nil)
		_ = yield(a, nil)
	}
	oracleIter := func(yield func(Row, error) bool) {
		_ = yield(a, nil)
		_ = yield(b, nil)
	}

	res := ZipAndCompare(t.Context(), table, testIter, oracleIter)
	assert.NoError(t, res.ToError())
	assert.Equal(t, 2, res.MatchCount)
	assert.Empty(t, res.DifferentRows)
}
