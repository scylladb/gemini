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

// Benchmarks for the full validation pipeline.
//
// Call graph (top-down):
//
//	CompareCollectedRows / ZipAndCompare
//	  ├─ deduplicateListValues (covered in dedup_bench_test.go)
//	  ├─ slices.SortStableFunc  ←  compareValues / rowsCmp
//	  ├─ reflect.DeepEqual  (fast-path, all-match)
//	  ├─ pks + buildRowMap  (count-mismatch path)
//	  └─ diffRows × N  (same-count, some-differ path)
//	       └─ canonicalizeRow × 2  →  canonicalValueString × col
//
// Each layer is benchmarked individually and end-to-end so that a single
// profile run can attribute cost to the right function.

import (
	"context"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/typedef"
)

// ── table / row fixture factories ────────────────────────────────────────────

// makeScalarTable returns a table with one int PK, one int CK, and `nExtra`
// additional int columns — a typical gemini "simple" table.
func makeScalarTable(nExtra int) *typedef.Table {
	cols := make(typedef.Columns, nExtra)
	for i := range nExtra {
		cols[i] = typedef.ColumnDef{Name: fmt.Sprintf("v%d", i), Type: typedef.TypeInt}
	}
	return &typedef.Table{
		PartitionKeys:  []typedef.ColumnDef{{Name: "pk", Type: typedef.TypeInt}},
		ClusteringKeys: []typedef.ColumnDef{{Name: "ck", Type: typedef.TypeInt}},
		Columns:        cols,
	}
}

// makeMultiPKTable returns a table with nPK partition keys, nCK clustering
// keys and nExtra value columns — exercises the key-sorting hot path.
func makeMultiPKTable(nPK, nCK, nExtra int) *typedef.Table {
	pks := make([]typedef.ColumnDef, nPK)
	for i := range nPK {
		pks[i] = typedef.ColumnDef{Name: fmt.Sprintf("pk%d", i), Type: typedef.TypeInt}
	}
	cks := make([]typedef.ColumnDef, nCK)
	for i := range nCK {
		cks[i] = typedef.ColumnDef{Name: fmt.Sprintf("ck%d", i), Type: typedef.TypeInt}
	}
	cols := make(typedef.Columns, nExtra)
	for i := range nExtra {
		cols[i] = typedef.ColumnDef{Name: fmt.Sprintf("v%d", i), Type: typedef.TypeInt}
	}
	return &typedef.Table{
		PartitionKeys:  pks,
		ClusteringKeys: cks,
		Columns:        cols,
	}
}

// makeScalarRow builds one Row for a scalar table produced by makeScalarTable.
// pk, ck are the key values; vals are the extra value columns.
func makeScalarRow(table *typedef.Table, pk, ck int) Row {
	names := make([]string, 0, 2+len(table.Columns))
	vals := make([]any, 0, 2+len(table.Columns))

	names = append(names, "pk")
	vals = append(vals, int32(pk))
	names = append(names, "ck")
	vals = append(vals, int32(ck))
	for i := range len(table.Columns) {
		names = append(names, fmt.Sprintf("v%d", i))
		vals = append(vals, int32(pk*100+i))
	}
	return NewRow(names, vals)
}

// makeScalarRows builds nRows identical test and oracle Rows for a scalar table.
func makeScalarRows(table *typedef.Table, nRows int) (test, oracle Rows) {
	test = make(Rows, nRows)
	oracle = make(Rows, nRows)
	for i := range nRows {
		test[i] = makeScalarRow(table, i, i)
		oracle[i] = makeScalarRow(table, i, i)
	}
	return test, oracle
}

// makeScalarRowsWithDiffs builds nRows pairs where `nDiff` rows differ in
// their first extra value column — exercises the diffRows path.
func makeScalarRowsWithDiffs(table *typedef.Table, nRows, nDiff int) (test, oracle Rows) {
	test = make(Rows, nRows)
	oracle = make(Rows, nRows)
	for i := range nRows {
		test[i] = makeScalarRow(table, i, i)
		oracle[i] = makeScalarRow(table, i, i)
		if i < nDiff && len(table.Columns) > 0 {
			// Mutate first value column in oracle to create a diff
			oracle[i].Set("v0", int32(9999))
		}
	}
	return test, oracle
}

// makeScalarRowsCountMismatch returns test with nRows rows and oracle with
// nRows-nMissing rows — exercises the pks + buildRowMap path.
func makeScalarRowsCountMismatch(table *typedef.Table, nRows, nMissing int) (test, oracle Rows) {
	test = make(Rows, nRows)
	oracle = make(Rows, nRows-nMissing)
	for i := range nRows {
		test[i] = makeScalarRow(table, i, i)
	}
	for i := range nRows - nMissing {
		oracle[i] = makeScalarRow(table, i, i)
	}
	return test, oracle
}

// makeRowIterator wraps a pre-built Rows slice into a RowIterator.
func makeRowIterator(rows Rows) RowIterator {
	return func(yield func(Row, error) bool) {
		for _, r := range rows {
			if !yield(r, nil) {
				return
			}
		}
	}
}

// ── BenchmarkNewRow ──────────────────────────────────────────────────────────
// NewRow is called once per DB row loaded; it allocates a map[string]int.
// This benchmark isolates that allocation cost at realistic column counts.

func BenchmarkNewRow(b *testing.B) {
	for _, nCols := range []int{3, 8, 16, 32} {
		nCols := nCols
		names := make([]string, nCols)
		vals := make([]any, nCols)
		for i := range nCols {
			names[i] = fmt.Sprintf("col%d", i)
			vals[i] = int32(i)
		}

		b.Run(fmt.Sprintf("cols=%d", nCols), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				r := NewRow(names, vals)
				_ = r
			}
		})
	}
}

// ── BenchmarkCompareValues ───────────────────────────────────────────────────
// compareValues is the innermost comparator called by both the sort and
// diffRows.  Benchmark each type branch so regressions are visible.

func BenchmarkCompareValues(b *testing.B) {
	b.Run("int32/equal", func(b *testing.B) {
		b.ReportAllocs()
		var a, bv any = int32(42), int32(42)
		for range b.N {
			_ = compareValues(a, bv)
		}
	})
	b.Run("int32/different", func(b *testing.B) {
		b.ReportAllocs()
		var a, bv any = int32(1), int32(2)
		for range b.N {
			_ = compareValues(a, bv)
		}
	})
	b.Run("int64/equal", func(b *testing.B) {
		b.ReportAllocs()
		var a, bv any = int64(100), int64(100)
		for range b.N {
			_ = compareValues(a, bv)
		}
	})
	b.Run("string/equal", func(b *testing.B) {
		b.ReportAllocs()
		var a, bv any = "hello-world", "hello-world"
		for range b.N {
			_ = compareValues(a, bv)
		}
	})
	b.Run("string/different", func(b *testing.B) {
		b.ReportAllocs()
		var a, bv any = "aaa", "zzz"
		for range b.N {
			_ = compareValues(a, bv)
		}
	})
	b.Run("uuid/equal", func(b *testing.B) {
		b.ReportAllocs()
		u := gocql.MustRandomUUID()
		var a, bv any = u, u
		for range b.N {
			_ = compareValues(a, bv)
		}
	})
	b.Run("time.Time/equal", func(b *testing.B) {
		b.ReportAllocs()
		t0 := time.Now()
		var a, bv any = t0, t0
		for range b.N {
			_ = compareValues(a, bv)
		}
	})
	b.Run("float64/equal", func(b *testing.B) {
		b.ReportAllocs()
		var a, bv any = float64(3.14), float64(3.14)
		for range b.N {
			_ = compareValues(a, bv)
		}
	})
	b.Run("blob/equal", func(b *testing.B) {
		b.ReportAllocs()
		blob := []byte("some-binary-data")
		var a, bv any = blob, blob
		for range b.N {
			_ = compareValues(a, bv)
		}
	})
	b.Run("bigInt/equal", func(b *testing.B) {
		b.ReportAllocs()
		n := big.NewInt(123456789)
		var a, bv any = n, n
		for range b.N {
			_ = compareValues(a, bv)
		}
	})
	b.Run("decimal/equal", func(b *testing.B) {
		b.ReportAllocs()
		d := inf.NewDec(12345, 2)
		var a, bv any = d, d
		for range b.N {
			_ = compareValues(a, bv)
		}
	})
	b.Run("fallback/fmt.Sprint", func(b *testing.B) {
		// Exercises the fmt.Sprint fallback branch (mismatched/unknown types).
		b.ReportAllocs()
		var a, bv any = int32(1), int64(1) // same number, different concrete types
		for range b.N {
			_ = compareValues(a, bv)
		}
	})
	b.Run("nil/nil", func(b *testing.B) {
		b.ReportAllocs()
		var a, bv any
		for range b.N {
			_ = compareValues(a, bv)
		}
	})
}

// ── BenchmarkRowsCmp ─────────────────────────────────────────────────────────
// rowsCmp is the tiebreaker used inside the sort closure.

func BenchmarkRowsCmp(b *testing.B) {
	for _, nCols := range []int{3, 8, 16} {
		nCols := nCols
		table := makeScalarTable(nCols - 2)
		r1 := makeScalarRow(table, 1, 1)
		r2 := makeScalarRow(table, 1, 1) // equal — worst case (all cols compared)
		r3 := makeScalarRow(table, 2, 2) // different first value — fast exit

		b.Run(fmt.Sprintf("cols=%d/equal", nCols), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_ = rowsCmp(r1, r2)
			}
		})
		b.Run(fmt.Sprintf("cols=%d/different", nCols), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_ = rowsCmp(r1, r3)
			}
		})
	}
}

// ── BenchmarkSortRows ────────────────────────────────────────────────────────
// The sort step is O(n log n) * compareValues cost.  Benchmark with realistic
// row counts and key widths to understand sort contribution.

func BenchmarkSortRows(b *testing.B) {
	type sortCase struct {
		nRows, nPK, nCK, nExtra int
	}
	cases := []sortCase{
		{nRows: 10, nPK: 1, nCK: 1, nExtra: 2},
		{nRows: 100, nPK: 1, nCK: 1, nExtra: 4},
		{nRows: 1000, nPK: 1, nCK: 1, nExtra: 4},
		{nRows: 100, nPK: 2, nCK: 2, nExtra: 4},
		{nRows: 1000, nPK: 2, nCK: 2, nExtra: 4},
	}

	for _, tc := range cases {
		tc := tc
		table := makeMultiPKTable(tc.nPK, tc.nCK, tc.nExtra)

		// Build a pre-shuffled slice; copy it each iteration so each sort
		// starts from the same unsorted input.
		src := make(Rows, tc.nRows)
		for i := range tc.nRows {
			row := make([]any, 0, tc.nPK+tc.nCK+tc.nExtra)
			names := make([]string, 0, tc.nPK+tc.nCK+tc.nExtra)
			for j := range tc.nPK {
				names = append(names, fmt.Sprintf("pk%d", j))
				// Reverse order to maximise sort work
				row = append(row, int32(tc.nRows-i))
			}
			for j := range tc.nCK {
				names = append(names, fmt.Sprintf("ck%d", j))
				row = append(row, int32(i))
			}
			for j := range tc.nExtra {
				names = append(names, fmt.Sprintf("v%d", j))
				row = append(row, int32(i*j))
			}
			src[i] = NewRow(names, row)
		}

		name := fmt.Sprintf("rows=%d/pk=%d/ck=%d/extra=%d",
			tc.nRows, tc.nPK, tc.nCK, tc.nExtra)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			cmpFn := func(a, bRow Row) int {
				for _, pk := range table.PartitionKeys {
					if c := compareValues(a.Get(pk.Name), bRow.Get(pk.Name)); c != 0 {
						return c
					}
				}
				for _, ck := range table.ClusteringKeys {
					if c := compareValues(a.Get(ck.Name), bRow.Get(ck.Name)); c != 0 {
						return c
					}
				}
				return rowsCmp(a, bRow)
			}

			dst := make(Rows, tc.nRows)
			b.ResetTimer()
			for range b.N {
				copy(dst, src)
				slices.SortStableFunc(dst, cmpFn)
			}
		})
	}
}

// ── BenchmarkCanonicalValueString ────────────────────────────────────────────
// canonicalValueString is called once per column per differing row inside
// diffRows.  Each type branch has a different cost.

func BenchmarkCanonicalValueString(b *testing.B) {
	b.Run("int32", func(b *testing.B) {
		b.ReportAllocs()
		var v any = int32(42)
		for range b.N {
			_ = canonicalValueString(v)
		}
	})
	b.Run("int64", func(b *testing.B) {
		b.ReportAllocs()
		var v any = int64(9876543210)
		for range b.N {
			_ = canonicalValueString(v)
		}
	})
	b.Run("string", func(b *testing.B) {
		b.ReportAllocs()
		var v any = "some-text-value"
		for range b.N {
			_ = canonicalValueString(v)
		}
	})
	b.Run("bool", func(b *testing.B) {
		b.ReportAllocs()
		var v any = true
		for range b.N {
			_ = canonicalValueString(v)
		}
	})
	b.Run("uuid", func(b *testing.B) {
		b.ReportAllocs()
		u := gocql.MustRandomUUID()
		var v any = u
		for range b.N {
			_ = canonicalValueString(v)
		}
	})
	b.Run("time.Time", func(b *testing.B) {
		b.ReportAllocs()
		var v any = time.Now()
		for range b.N {
			_ = canonicalValueString(v)
		}
	})
	b.Run("blob", func(b *testing.B) {
		b.ReportAllocs()
		var v any = []byte("binary-data-here")
		for range b.N {
			_ = canonicalValueString(v)
		}
	})
	b.Run("float64", func(b *testing.B) {
		b.ReportAllocs()
		var v any = float64(3.141592653589793)
		for range b.N {
			_ = canonicalValueString(v)
		}
	})
	b.Run("ptr/int64", func(b *testing.B) {
		b.ReportAllocs()
		n := int64(42)
		var v any = &n
		for range b.N {
			_ = canonicalValueString(v)
		}
	})
	b.Run("nil", func(b *testing.B) {
		b.ReportAllocs()
		var v any
		for range b.N {
			_ = canonicalValueString(v)
		}
	})
}

// ── BenchmarkCanonicalizeRow ─────────────────────────────────────────────────
// canonicalizeRow builds a map[string]string per row — one allocation per
// differing row in diffRows.

func BenchmarkCanonicalizeRow(b *testing.B) {
	for _, nCols := range []int{4, 8, 16, 32} {
		nCols := nCols
		table := makeScalarTable(nCols - 2)
		row := makeScalarRow(table, 1, 1)

		b.Run(fmt.Sprintf("cols=%d", nCols), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_ = canonicalizeRow(row)
			}
		})
	}
}

// ── BenchmarkDiffRows ────────────────────────────────────────────────────────
// diffRows allocates two canonicalizeRow maps plus a key union slice.
// Benchmark with varying column widths and diff fractions.

func BenchmarkDiffRows(b *testing.B) {
	type diffCase struct {
		nCols   int
		nDiffed int // how many columns differ between oracle and test
		desc    string
	}
	cases := []diffCase{
		{nCols: 4, nDiffed: 1, desc: "cols=4/diff=1"},
		{nCols: 4, nDiffed: 4, desc: "cols=4/diff=4"},
		{nCols: 16, nDiffed: 1, desc: "cols=16/diff=1"},
		{nCols: 16, nDiffed: 8, desc: "cols=16/diff=8"},
		{nCols: 32, nDiffed: 4, desc: "cols=32/diff=4"},
		{nCols: 4, nDiffed: 0, desc: "cols=4/diff=0/nodiff"}, // returns "" immediately
	}

	for _, tc := range cases {
		tc := tc
		table := makeScalarTable(tc.nCols - 2)

		// Build a row, then a mutated oracle row with nDiffed cols changed.
		testRow := makeScalarRow(table, 1, 1)
		oracleRow := makeScalarRow(table, 1, 1)
		for i := range tc.nDiffed {
			name := fmt.Sprintf("v%d", i)
			oracleRow.Set(name, int32(9999+i))
		}

		b.Run(tc.desc, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_ = diffRows(table, oracleRow, testRow)
			}
		})
	}
}

// ── BenchmarkBuildRowMap ─────────────────────────────────────────────────────
// buildRowMap allocates a map[string]Row; used only on count-mismatch path.

func BenchmarkBuildRowMap(b *testing.B) {
	for _, nRows := range []int{10, 100, 1000} {
		nRows := nRows
		table := makeScalarTable(2)
		rows := make(Rows, nRows)
		for i := range nRows {
			rows[i] = makeScalarRow(table, i, i)
		}

		b.Run(fmt.Sprintf("rows=%d", nRows), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_ = buildRowMap(table, rows)
			}
		})
	}
}

// ── BenchmarkPks ─────────────────────────────────────────────────────────────
// pks builds a *strset.Set of composite key strings; also count-mismatch only.

func BenchmarkPks(b *testing.B) {
	for _, nRows := range []int{10, 100, 1000} {
		nRows := nRows
		table := makeScalarTable(2)
		rows := make(Rows, nRows)
		for i := range nRows {
			rows[i] = makeScalarRow(table, i, i)
		}

		b.Run(fmt.Sprintf("rows=%d", nRows), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_ = pks(table, rows)
			}
		})
	}
}

// ── BenchmarkRowKeyString ────────────────────────────────────────────────────
// rowKeyString / rowsToKeyStrings are on the error reporting path (ToError).

func BenchmarkRowKeyString(b *testing.B) {
	for _, nKeys := range []int{1, 3, 6} {
		nKeys := nKeys
		table := makeMultiPKTable(nKeys, nKeys, 2)

		// Build column names / values matching table schema
		names := make([]string, 0, nKeys*2+2)
		vals := make([]any, 0, nKeys*2+2)
		for i := range nKeys {
			names = append(names, fmt.Sprintf("pk%d", i))
			vals = append(vals, int32(i*10))
		}
		for i := range nKeys {
			names = append(names, fmt.Sprintf("ck%d", i))
			vals = append(vals, int32(i*10+1))
		}
		names = append(names, "v0", "v1")
		vals = append(vals, int32(99), int32(100))
		row := NewRow(names, vals)

		b.Run(fmt.Sprintf("keyWidth=%d", nKeys), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_ = rowKeyString(table, row)
			}
		})
	}
}

func BenchmarkRowsToKeyStrings(b *testing.B) {
	table := makeScalarTable(2)
	for _, nRows := range []int{10, 100, 1000} {
		nRows := nRows
		rows := make(Rows, nRows)
		for i := range nRows {
			rows[i] = makeScalarRow(table, i, i)
		}

		b.Run(fmt.Sprintf("rows=%d", nRows), func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				_ = rowsToKeyStrings(table, rows)
			}
		})
	}
}

// ── BenchmarkCompareCollectedRows (full end-to-end) ──────────────────────────
//
// Three sub-scenarios that exercise different code paths inside
// CompareCollectedRows:
//
//  1. allMatch   — reflect.DeepEqual fast-path after sort
//  2. someDiff   — same row count but diffRows called for nDiff rows
//  3. countMismatch — pks + buildRowMap set-difference path

func BenchmarkCompareCollectedRows_AllMatch(b *testing.B) {
	type matchCase struct {
		nRows, nPK, nCK, nExtra int
	}
	cases := []matchCase{
		{nRows: 10, nPK: 1, nCK: 1, nExtra: 2},
		{nRows: 100, nPK: 1, nCK: 1, nExtra: 4},
		{nRows: 1000, nPK: 1, nCK: 1, nExtra: 4},
		{nRows: 100, nPK: 2, nCK: 2, nExtra: 4},
	}

	for _, tc := range cases {
		tc := tc
		table := makeMultiPKTable(tc.nPK, tc.nCK, tc.nExtra)

		name := fmt.Sprintf("rows=%d/pk=%d/ck=%d/extra=%d",
			tc.nRows, tc.nPK, tc.nCK, tc.nExtra)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			// Pre-build the rows; regenerate each iteration to give the sort
			// a realistic unsorted input.
			b.ResetTimer()
			for range b.N {
				test, oracle := makeScalarRows(table, tc.nRows)
				result := CompareCollectedRows(table, test, oracle)
				if result.MatchCount != tc.nRows {
					b.Fatalf("expected %d matches, got %d", tc.nRows, result.MatchCount)
				}
			}
		})
	}
}

func BenchmarkCompareCollectedRows_SomeDiff(b *testing.B) {
	type diffCase struct {
		nRows, nExtra, nDiff int
	}
	cases := []diffCase{
		{nRows: 10, nExtra: 4, nDiff: 1},
		{nRows: 100, nExtra: 4, nDiff: 10},
		{nRows: 100, nExtra: 4, nDiff: 50},
		{nRows: 100, nExtra: 16, nDiff: 10},
		{nRows: 1000, nExtra: 4, nDiff: 100},
	}

	for _, tc := range cases {
		tc := tc
		table := makeScalarTable(tc.nExtra)

		name := fmt.Sprintf("rows=%d/extra=%d/diffs=%d", tc.nRows, tc.nExtra, tc.nDiff)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				test, oracle := makeScalarRowsWithDiffs(table, tc.nRows, tc.nDiff)
				result := CompareCollectedRows(table, test, oracle)
				if len(result.DifferentRows) != tc.nDiff {
					b.Fatalf("expected %d diffs, got %d", tc.nDiff, len(result.DifferentRows))
				}
			}
		})
	}
}

func BenchmarkCompareCollectedRows_CountMismatch(b *testing.B) {
	type mismatchCase struct {
		nRows, nMissing int
	}
	cases := []mismatchCase{
		{nRows: 10, nMissing: 1},
		{nRows: 100, nMissing: 10},
		{nRows: 100, nMissing: 50},
		{nRows: 1000, nMissing: 100},
	}

	for _, tc := range cases {
		tc := tc
		table := makeScalarTable(2)

		name := fmt.Sprintf("rows=%d/missing=%d", tc.nRows, tc.nMissing)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				test, oracle := makeScalarRowsCountMismatch(table, tc.nRows, tc.nMissing)
				result := CompareCollectedRows(table, test, oracle)
				if len(result.TestOnlyRows) != tc.nMissing {
					b.Fatalf("expected %d test-only rows, got %d", tc.nMissing, len(result.TestOnlyRows))
				}
			}
		})
	}
}

// ── BenchmarkZipAndCompare ───────────────────────────────────────────────────
// ZipAndCompare wraps Collect() × 2 + CompareCollectedRows; benchmarks the
// overhead of the iterator-based entry point.

func BenchmarkZipAndCompare(b *testing.B) {
	type zipCase struct {
		nRows, nExtra int
		scenario      string
	}
	cases := []zipCase{
		{nRows: 10, nExtra: 2, scenario: "allMatch"},
		{nRows: 100, nExtra: 4, scenario: "allMatch"},
		{nRows: 1000, nExtra: 4, scenario: "allMatch"},
	}

	table := makeScalarTable(4)
	ctx := context.Background()

	for _, tc := range cases {
		tc := tc
		name := fmt.Sprintf("rows=%d/extra=%d/%s", tc.nRows, tc.nExtra, tc.scenario)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				test, oracle := makeScalarRows(table, tc.nRows)
				result := ZipAndCompare(ctx, table, makeRowIterator(test), makeRowIterator(oracle))
				if result.MatchCount != tc.nRows {
					b.Fatalf("expected %d matches, got %d", tc.nRows, result.MatchCount)
				}
			}
		})
	}
}

// ── BenchmarkComparisonResult_ToError ────────────────────────────────────────
// ToError builds multierr error values; benchmarks the error-rendering path.

func BenchmarkComparisonResult_ToError(b *testing.B) {
	table := makeScalarTable(2)

	b.Run("noError", func(b *testing.B) {
		b.ReportAllocs()
		result := ComparisonResult{Table: table, MatchCount: 10}
		for range b.N {
			_ = result.ToError()
		}
	})

	b.Run("testOnlyRows/n=10", func(b *testing.B) {
		b.ReportAllocs()
		rows := make(Rows, 10)
		for i := range 10 {
			rows[i] = makeScalarRow(table, i, i)
		}
		result := ComparisonResult{
			Table:        table,
			TestOnlyRows: rows,
		}
		for range b.N {
			_ = result.ToError()
		}
	})

	b.Run("differentRows/n=10", func(b *testing.B) {
		b.ReportAllocs()
		diffs := make([]RowDifference, 10)
		for i := range 10 {
			var sb strings.Builder
			fmt.Fprintf(&sb, "pk: pk=%d\n- v0: %d\n+ v0: %d\n", i, i, i+1)
			diffs[i] = RowDifference{
				TestRow:   makeScalarRow(table, i, i),
				OracleRow: makeScalarRow(table, i, i),
				Diff:      sb.String(),
			}
		}
		result := ComparisonResult{
			Table:         table,
			DifferentRows: diffs,
		}
		for range b.N {
			_ = result.ToError()
		}
	})
}
