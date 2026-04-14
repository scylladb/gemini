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
	"math/big"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/inf.v0"

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

func TestDeduplicateSlice_AllTypes(t *testing.T) {
	t.Parallel()

	t.Run("[]any with int64 duplicates", func(t *testing.T) {
		t.Parallel()
		s := []any{int64(1), int64(2), int64(1), int64(3), int64(2)}
		newVal, before, after := deduplicateSlice(s)
		assert.Equal(t, 5, before)
		assert.Equal(t, 3, after)
		assert.Equal(t, []any{int64(1), int64(2), int64(3)}, newVal)
	})

	t.Run("[]any already unique", func(t *testing.T) {
		t.Parallel()
		s := []any{int64(1), int64(2), int64(3)}
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, before, after)
	})

	t.Run("nil value", func(t *testing.T) {
		t.Parallel()
		_, before, after := deduplicateSlice(nil)
		assert.Equal(t, 0, before)
		assert.Equal(t, 0, after)
	})

	// ── bool ─────────────────────────────────────────────────────────────────

	t.Run("[]bool", func(t *testing.T) {
		t.Parallel()
		s := []bool{true, false, true, true}
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, 4, before)
		assert.Equal(t, 2, after)
		assert.Equal(t, []bool{true, false}, s[:after])
	})

	t.Run("*[]bool", func(t *testing.T) {
		t.Parallel()
		s := []bool{true, false, true}
		_, before, after := deduplicateSlice(&s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
		assert.Equal(t, []bool{true, false}, s[:after])
	})

	t.Run("*[]bool nil pointer", func(t *testing.T) {
		t.Parallel()
		var p *[]bool
		_, before, after := deduplicateSlice(p)
		assert.Equal(t, 0, before)
		assert.Equal(t, 0, after)
	})

	// ── int (CQL int) ─────────────────────────────────────────────────────────

	t.Run("[]int", func(t *testing.T) {
		t.Parallel()
		s := []int{1, 2, 1, 3}
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, 4, before)
		assert.Equal(t, 3, after)
		assert.Equal(t, []int{1, 2, 3}, s[:after])
	})

	t.Run("*[]int", func(t *testing.T) {
		t.Parallel()
		s := []int{1, 1, 2}
		_, before, after := deduplicateSlice(&s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
		assert.Equal(t, []int{1, 2}, s[:after])
	})

	// ── int8 (tinyint) ───────────────────────────────────────────────────────

	t.Run("[]int8", func(t *testing.T) {
		t.Parallel()
		s := []int8{1, 2, 1, 3}
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, 4, before)
		assert.Equal(t, 3, after)
		assert.Equal(t, []int8{1, 2, 3}, s[:after])
	})

	t.Run("*[]int8", func(t *testing.T) {
		t.Parallel()
		s := []int8{5, 5, 6}
		_, before, after := deduplicateSlice(&s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
		assert.Equal(t, []int8{5, 6}, s[:after])
	})

	// ── int16 (smallint) ─────────────────────────────────────────────────────

	t.Run("[]int16", func(t *testing.T) {
		t.Parallel()
		s := []int16{10, 20, 10, 30}
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, 4, before)
		assert.Equal(t, 3, after)
		assert.Equal(t, []int16{10, 20, 30}, s[:after])
	})

	t.Run("*[]int16", func(t *testing.T) {
		t.Parallel()
		s := []int16{100, 100, 200}
		_, before, after := deduplicateSlice(&s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
	})

	// ── int32 ────────────────────────────────────────────────────────────────

	t.Run("[]int32", func(t *testing.T) {
		t.Parallel()
		s := []int32{1, 2, 1}
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
		assert.Equal(t, []int32{1, 2}, s[:after])
	})

	t.Run("*[]int32", func(t *testing.T) {
		t.Parallel()
		s := []int32{7, 7, 8}
		_, before, after := deduplicateSlice(&s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
	})

	// ── int64 (bigint) ───────────────────────────────────────────────────────

	t.Run("[]int64", func(t *testing.T) {
		t.Parallel()
		s := []int64{100, 200, 100, 300}
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, 4, before)
		assert.Equal(t, 3, after)
		assert.Equal(t, []int64{100, 200, 300}, s[:after])
	})

	t.Run("*[]int64", func(t *testing.T) {
		t.Parallel()
		// This is the real-world gocql scan form for list<bigint>
		s := []int64{
			1290817393298494093,
			1290817393298494093,
			5837704151482537345,
			5837704151482537345,
		}
		_, before, after := deduplicateSlice(&s)
		assert.Equal(t, 4, before)
		assert.Equal(t, 2, after)
		assert.Equal(t, []int64{1290817393298494093, 5837704151482537345}, s[:after])
	})

	// ── float32 (float) ──────────────────────────────────────────────────────

	t.Run("[]float32", func(t *testing.T) {
		t.Parallel()
		s := []float32{1.1, 2.2, 1.1}
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
	})

	t.Run("*[]float32", func(t *testing.T) {
		t.Parallel()
		s := []float32{3.14, 3.14, 2.72}
		_, before, after := deduplicateSlice(&s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
	})

	// ── float64 (double) ─────────────────────────────────────────────────────

	t.Run("[]float64", func(t *testing.T) {
		t.Parallel()
		s := []float64{1.1, 2.2, 1.1}
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
	})

	t.Run("*[]float64", func(t *testing.T) {
		t.Parallel()
		s := []float64{0.1, 0.2, 0.1, 0.3}
		_, before, after := deduplicateSlice(&s)
		assert.Equal(t, 4, before)
		assert.Equal(t, 3, after)
	})

	// ── string (text, ascii, varchar, inet) ──────────────────────────────────

	t.Run("[]string", func(t *testing.T) {
		t.Parallel()
		s := []string{"a", "b", "a", "c"}
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, 4, before)
		assert.Equal(t, 3, after)
		assert.Equal(t, []string{"a", "b", "c"}, s[:after])
	})

	t.Run("*[]string", func(t *testing.T) {
		t.Parallel()
		s := []string{"x", "x", "y"}
		_, before, after := deduplicateSlice(&s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
	})

	// ── gocql.UUID (uuid, timeuuid) ───────────────────────────────────────────

	t.Run("[]gocql.UUID", func(t *testing.T) {
		t.Parallel()
		u1 := gocql.MustRandomUUID()
		u2 := gocql.MustRandomUUID()
		s := []gocql.UUID{u1, u2, u1}
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
		assert.Equal(t, []gocql.UUID{u1, u2}, s[:after])
	})

	t.Run("*[]gocql.UUID", func(t *testing.T) {
		t.Parallel()
		u1 := gocql.MustRandomUUID()
		s := []gocql.UUID{u1, u1, u1}
		_, before, after := deduplicateSlice(&s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 1, after)
	})

	// ── time.Time (timestamp, date) ───────────────────────────────────────────

	t.Run("[]time.Time", func(t *testing.T) {
		t.Parallel()
		t1 := time.Now().Round(time.Second)
		t2 := t1.Add(time.Hour)
		s := []time.Time{t1, t2, t1}
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
		assert.True(t, s[0].Equal(t1))
		assert.True(t, s[1].Equal(t2))
	})

	t.Run("*[]time.Time", func(t *testing.T) {
		t.Parallel()
		t1 := time.Now().Round(time.Second)
		s := []time.Time{t1, t1, t1}
		_, before, after := deduplicateSlice(&s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 1, after)
	})

	// ── time.Duration (CQL time) ──────────────────────────────────────────────

	t.Run("[]time.Duration", func(t *testing.T) {
		t.Parallel()
		s := []time.Duration{time.Second, time.Minute, time.Second}
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
	})

	t.Run("*[]time.Duration", func(t *testing.T) {
		t.Parallel()
		s := []time.Duration{time.Hour, time.Hour}
		_, before, after := deduplicateSlice(&s)
		assert.Equal(t, 2, before)
		assert.Equal(t, 1, after)
	})

	// ── gocql.Duration (CQL duration) ─────────────────────────────────────────

	t.Run("[]gocql.Duration", func(t *testing.T) {
		t.Parallel()
		d1 := gocql.Duration{Months: 1, Days: 2, Nanoseconds: 3}
		d2 := gocql.Duration{Months: 4, Days: 5, Nanoseconds: 6}
		s := []gocql.Duration{d1, d2, d1}
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
		assert.Equal(t, d1, s[0])
		assert.Equal(t, d2, s[1])
	})

	t.Run("*[]gocql.Duration", func(t *testing.T) {
		t.Parallel()
		d := gocql.Duration{Months: 1}
		s := []gocql.Duration{d, d, d}
		_, before, after := deduplicateSlice(&s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 1, after)
	})

	// ── []byte (blob) ─────────────────────────────────────────────────────────

	t.Run("[][]byte", func(t *testing.T) {
		t.Parallel()
		b1 := []byte{0x01, 0x02}
		b2 := []byte{0x03, 0x04}
		s := [][]byte{b1, b2, {0x01, 0x02}} // third is equal to b1 by value
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
	})

	t.Run("*[][]byte", func(t *testing.T) {
		t.Parallel()
		b := []byte{0xAA, 0xBB}
		s := [][]byte{b, b, {0xCC}}
		_, before, after := deduplicateSlice(&s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
	})

	// ── *big.Int (varint) ────────────────────────────────────────────────────

	t.Run("[]*big.Int", func(t *testing.T) {
		t.Parallel()
		v1 := big.NewInt(123)
		v2 := big.NewInt(456)
		v3 := big.NewInt(123) // equal to v1 by value
		s := []*big.Int{v1, v2, v3}
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
		assert.Equal(t, 0, s[0].Cmp(big.NewInt(123)))
		assert.Equal(t, 0, s[1].Cmp(big.NewInt(456)))
	})

	t.Run("*[]*big.Int", func(t *testing.T) {
		t.Parallel()
		v := big.NewInt(999)
		s := []*big.Int{v, v, big.NewInt(999)}
		_, before, after := deduplicateSlice(&s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 1, after)
	})

	// ── *inf.Dec (decimal) ───────────────────────────────────────────────────

	t.Run("[]*inf.Dec", func(t *testing.T) {
		t.Parallel()
		d1 := inf.NewDec(12345, 2) // 123.45
		d2 := inf.NewDec(67890, 3) // 67.890
		d3 := inf.NewDec(12345, 2) // equal to d1
		s := []*inf.Dec{d1, d2, d3}
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 2, after)
		assert.Equal(t, 0, s[0].Cmp(d1))
		assert.Equal(t, 0, s[1].Cmp(d2))
	})

	t.Run("*[]*inf.Dec", func(t *testing.T) {
		t.Parallel()
		d := inf.NewDec(1, 0)
		s := []*inf.Dec{d, d, inf.NewDec(1, 0)}
		_, before, after := deduplicateSlice(&s)
		assert.Equal(t, 3, before)
		assert.Equal(t, 1, after)
	})

	// ── single-element slice (no-op) ──────────────────────────────────────────

	t.Run("single element is never modified", func(t *testing.T) {
		t.Parallel()
		s := []int64{42}
		_, before, after := deduplicateSlice(s)
		assert.Equal(t, 1, before)
		assert.Equal(t, 1, after)
	})
}

// TestDeduplicateListValues covers the full deduplicateListValues path,
// verifying that the row value is mutated in-place for all stored forms.
func TestDeduplicateListValues(t *testing.T) {
	t.Parallel()

	listType := &typedef.Collection{ComplexType: typedef.TypeList, ValueType: typedef.TypeInt}
	setType := &typedef.Collection{ComplexType: typedef.TypeSet, ValueType: typedef.TypeInt}

	t.Run("[]any list column deduped", func(t *testing.T) {
		t.Parallel()
		table := &typedef.Table{
			PartitionKeys: []typedef.ColumnDef{{Name: "pk", Type: typedef.TypeInt}},
			Columns:       []typedef.ColumnDef{{Name: "vals", Type: listType}},
		}
		rows := Rows{
			makeRow([]string{"pk", "vals"}, []any{int32(1), []any{int32(10), int32(20), int32(10), int32(30)}}),
		}
		deduplicateListValues(table, rows)
		// The []any is compacted in-place; we read back the full slice but only
		// the first `after` elements are valid — here the row stores the original
		// slice reference and the caller sees the compacted view via Set.
		got := rows[0].Get("vals").([]any)
		assert.Equal(t, []any{int32(10), int32(20), int32(30)}, got[:3])
	})

	t.Run("*[]int64 list column deduped (real gocql form)", func(t *testing.T) {
		t.Parallel()
		bigintListType := &typedef.Collection{ComplexType: typedef.TypeList, ValueType: typedef.TypeBigint}
		table := &typedef.Table{
			PartitionKeys: []typedef.ColumnDef{{Name: "pk", Type: typedef.TypeInt}},
			Columns:       []typedef.ColumnDef{{Name: "col4", Type: bigintListType}},
		}
		s := []int64{100, 100, 200, 200, 300}
		rows := Rows{
			makeRow([]string{"pk", "col4"}, []any{int32(1), &s}),
		}
		deduplicateListValues(table, rows)
		// The underlying slice is mutated; s[:3] holds the deduped values.
		assert.Equal(t, []int64{100, 200, 300}, s[:3])
	})

	t.Run("non-list columns untouched", func(t *testing.T) {
		t.Parallel()
		table := &typedef.Table{
			PartitionKeys: []typedef.ColumnDef{{Name: "pk", Type: typedef.TypeInt}},
			Columns:       []typedef.ColumnDef{{Name: "v", Type: typedef.TypeInt}},
		}
		rows := Rows{
			makeRow([]string{"pk", "v"}, []any{int32(1), int32(42)}),
		}
		deduplicateListValues(table, rows)
		assert.Equal(t, int32(42), rows[0].Get("v"))
	})

	t.Run("set columns skipped", func(t *testing.T) {
		t.Parallel()
		table := &typedef.Table{
			PartitionKeys: []typedef.ColumnDef{{Name: "pk", Type: typedef.TypeInt}},
			Columns:       []typedef.ColumnDef{{Name: "s", Type: setType}},
		}
		original := []any{int32(1), int32(2), int32(1)}
		rows := Rows{
			makeRow([]string{"pk", "s"}, []any{int32(1), original}),
		}
		deduplicateListValues(table, rows)
		assert.Equal(t, original, rows[0].Get("s"))
	})
}

// TestCompareCollectedRows_ListDuplicatesMatch is an end-to-end test: either
// cluster side may have duplicate entries in a list column; after dedup both
// sides should match.
func TestCompareCollectedRows_ListDuplicatesMatch(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name      string
		testVal   any
		oracleVal any
	}{
		{
			name:      "test-side []any duplicates",
			testVal:   []any{int32(10), int32(20), int32(10)},
			oracleVal: []any{int32(10), int32(20)},
		},
		{
			// Real gocql scan form: list<bigint> → *[]int64
			name: "test-side *[]int64 duplicates",
			testVal: func() *[]int64 {
				s := []int64{100, 200, 100, 300, 200}
				return &s
			}(),
			oracleVal: func() *[]int64 {
				s := []int64{100, 200, 300}
				return &s
			}(),
		},
		{
			name:      "test-side []int64 duplicates",
			testVal:   []int64{1, 2, 1, 3},
			oracleVal: []int64{1, 2, 3},
		},
		{
			name:      "oracle-side []any duplicates",
			testVal:   []any{int32(10), int32(20)},
			oracleVal: []any{int32(10), int32(20), int32(10)},
		},
		{
			name: "oracle-side *[]int64 duplicates",
			testVal: func() *[]int64 {
				s := []int64{100, 200, 300}
				return &s
			}(),
			oracleVal: func() *[]int64 {
				s := []int64{100, 200, 100, 300, 200}
				return &s
			}(),
		},
		{
			name:      "both sides []string duplicates",
			testVal:   []string{"a", "b", "a", "c"},
			oracleVal: []string{"a", "b", "b", "c"},
		},
	} {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			listType := &typedef.Collection{ComplexType: typedef.TypeList, ValueType: typedef.TypeInt}
			table := &typedef.Table{
				PartitionKeys: []typedef.ColumnDef{{Name: "pk", Type: typedef.TypeInt}},
				Columns:       []typedef.ColumnDef{{Name: "vals", Type: listType}},
			}

			testRows := Rows{
				makeRow([]string{"pk", "vals"}, []any{int32(1), tc.testVal}),
			}
			oracleRows := Rows{
				makeRow([]string{"pk", "vals"}, []any{int32(1), tc.oracleVal}),
			}

			res := CompareCollectedRows(table, testRows, oracleRows)
			assert.NoError(t, res.ToError())
			assert.Equal(t, 1, res.MatchCount)
			assert.Empty(t, res.DifferentRows)
		})
	}
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
