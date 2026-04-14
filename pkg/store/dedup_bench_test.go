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
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"

	"github.com/scylladb/gemini/pkg/typedef"
)

// ── helpers ──────────────────────────────────────────────────────────────────

// makeDupInt64Slice returns a *[]int64 that gocql would produce for
// list<bigint>.  The first half of the slice is a duplicate of the second half
// so that every element is deduplicated exactly once.
func makeDupInt64Slice(n int) *[]int64 {
	s := make([]int64, n)
	half := n / 2
	for i := range half {
		s[i] = int64(i)
		s[i+half] = int64(i) // duplicate
	}
	return &s
}

// makeUniqueInt64Slice returns a *[]int64 with no duplicates (worst case for
// dedup: every element must be tested against the growing seen-set).
func makeUniqueInt64Slice(n int) *[]int64 {
	s := make([]int64, n)
	for i := range n {
		s[i] = int64(i)
	}
	return &s
}

// makeStringSlice produces a []string with `dups` duplicate items appended to
// `unique` unique items so we always get a realistic mixture.
func makeStringSlice(unique, dups int) []string {
	s := make([]string, 0, unique+dups)
	for i := range unique {
		s = append(s, fmt.Sprintf("value-%d", i))
	}
	for i := range dups {
		s = append(s, fmt.Sprintf("value-%d", i%unique))
	}
	return s
}

// makeAnyInt64Slice mimics how some callers hand already-expanded []any to
// deduplicateSlice.
func makeAnyInt64Slice(unique, dups int) []any {
	s := make([]any, 0, unique+dups)
	for i := range unique {
		s = append(s, int64(i))
	}
	for i := range dups {
		s = append(s, int64(i%unique))
	}
	return s
}

// makeListTable builds a *typedef.Table with `nListCols` list<bigint> columns
// plus one int partition key.
func makeListTable(nListCols int) *typedef.Table {
	cols := make(typedef.Columns, nListCols)
	for i := range nListCols {
		cols[i] = typedef.ColumnDef{
			Name: fmt.Sprintf("col%d", i),
			Type: &typedef.Collection{
				ComplexType: typedef.TypeList,
				ValueType:   typedef.TypeBigint,
			},
		}
	}
	return &typedef.Table{
		PartitionKeys: []typedef.ColumnDef{{Name: "pk", Type: typedef.TypeInt}},
		Columns:       cols,
	}
}

// makeListRows builds `nRows` Rows, each containing `nListCols` *[]int64 list
// columns.  The lists have `listLen` elements with `dups` duplicates each.
func makeListRows(nRows, nListCols, listLen, dups int) Rows {
	colNames := make([]string, 0, 1+nListCols)
	colNames = append(colNames, "pk")
	for i := range nListCols {
		colNames = append(colNames, fmt.Sprintf("col%d", i))
	}

	rows := make(Rows, nRows)
	for r := range nRows {
		vals := make([]any, 0, 1+nListCols)
		vals = append(vals, int32(r))
		for range nListCols {
			s := make([]int64, listLen)
			unique := listLen - dups
			for i := range unique {
				s[i] = int64(i)
			}
			for i := range dups {
				s[unique+i] = int64(i % unique)
			}
			vals = append(vals, &s)
		}
		rows[r] = NewRow(colNames, vals)
	}
	return rows
}

// makeCompareRows builds matching test+oracle row sets with nListCols list
// columns each.  testRows has `dups` duplicates per list; oracleRows is clean.
func makeCompareRows(nRows, nListCols, listLen, dups int) (test, oracle Rows) {
	test = makeListRows(nRows, nListCols, listLen, dups)
	oracle = makeListRows(nRows, nListCols, listLen-dups, 0)
	return test, oracle
}

// ── BenchmarkDeduplicateSlice ─────────────────────────────────────────────────
// Covers the hot gocql types at various list lengths, in both the duplicates-
// present and all-unique (no-dedup work) shapes.

func BenchmarkDeduplicateSlice(b *testing.B) {
	sizes := []int{4, 16, 64, 256}

	// *[]int64  — most common real-world form (list<bigint>)
	b.Run("ptrInt64/halfDups", func(b *testing.B) {
		for _, n := range sizes {
			n := n
			b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
				b.ReportAllocs()
				orig := makeDupInt64Slice(n)
				b.ResetTimer()
				for range b.N {
					// Restore the slice length each iteration so we always
					// feed the full duplicated input.
					s := make([]int64, n)
					copy(s, (*orig)[:n/2])
					copy(s[n/2:], (*orig)[:n/2])
					ptr := &s
					_, _, _ = deduplicateSlice(ptr)
				}
			})
		}
	})

	// *[]int64 — no duplicates (early-exit / steady-state path)
	b.Run("ptrInt64/noDups", func(b *testing.B) {
		for _, n := range sizes {
			n := n
			b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
				b.ReportAllocs()
				orig := makeUniqueInt64Slice(n)
				b.ResetTimer()
				for range b.N {
					s := make([]int64, n)
					copy(s, *orig)
					ptr := &s
					_, _, _ = deduplicateSlice(ptr)
				}
			})
		}
	})

	// []string — second most common (list<text>)
	b.Run("sliceString/halfDups", func(b *testing.B) {
		for _, n := range sizes {
			n := n
			b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
				b.ReportAllocs()
				orig := makeStringSlice(n/2, n/2)
				b.ResetTimer()
				for range b.N {
					s := make([]string, len(orig))
					copy(s, orig)
					_, _, _ = deduplicateSlice(s)
				}
			})
		}
	})

	// []any — legacy / already-expanded path
	b.Run("sliceAny/halfDups", func(b *testing.B) {
		for _, n := range sizes {
			n := n
			b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
				b.ReportAllocs()
				orig := makeAnyInt64Slice(n/2, n/2)
				b.ResetTimer()
				for range b.N {
					s := make([]any, len(orig))
					copy(s, orig)
					_, _, _ = deduplicateSlice(s)
				}
			})
		}
	})

	// []gocql.UUID
	b.Run("sliceUUID/halfDups", func(b *testing.B) {
		for _, n := range sizes {
			n := n
			b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
				b.ReportAllocs()
				half := n / 2
				uuids := make([]gocql.UUID, n)
				for i := range half {
					uuids[i] = gocql.MustRandomUUID()
					uuids[i+half] = uuids[i]
				}
				b.ResetTimer()
				for range b.N {
					s := make([]gocql.UUID, n)
					copy(s, uuids)
					_, _, _ = deduplicateSlice(s)
				}
			})
		}
	})

	// []time.Time — uses special Equal-based path
	b.Run("sliceTime/halfDups", func(b *testing.B) {
		for _, n := range sizes {
			n := n
			b.Run(fmt.Sprintf("n=%d", n), func(b *testing.B) {
				b.ReportAllocs()
				half := n / 2
				times := make([]time.Time, n)
				base := time.Now().Round(time.Second)
				for i := range half {
					times[i] = base.Add(time.Duration(i) * time.Second)
					times[i+half] = times[i]
				}
				b.ResetTimer()
				for range b.N {
					s := make([]time.Time, n)
					copy(s, times)
					_, _, _ = deduplicateSlice(s)
				}
			})
		}
	})
}

// ── BenchmarkDeduplicateListValues ───────────────────────────────────────────
// Covers the table-level loop that calls deduplicateSlice for every list
// column in every row.

func BenchmarkDeduplicateListValues(b *testing.B) {
	// Rows x ListCols x ListLen/Dups combinations that represent realistic
	// Gemini workloads.
	type bench struct {
		rows, cols, listLen, dups int
	}
	cases := []bench{
		{rows: 1, cols: 1, listLen: 8, dups: 4},
		{rows: 10, cols: 1, listLen: 8, dups: 4},
		{rows: 100, cols: 1, listLen: 8, dups: 4},
		{rows: 100, cols: 3, listLen: 8, dups: 4},
		{rows: 100, cols: 1, listLen: 64, dups: 32},
		{rows: 100, cols: 1, listLen: 8, dups: 0}, // no-op path
	}

	for _, bc := range cases {
		bc := bc
		name := fmt.Sprintf("rows=%d/cols=%d/len=%d/dups=%d",
			bc.rows, bc.cols, bc.listLen, bc.dups)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			table := makeListTable(bc.cols)
			b.ResetTimer()
			for range b.N {
				rows := makeListRows(bc.rows, bc.cols, bc.listLen, bc.dups)
				deduplicateListValues(table, rows)
			}
		})
	}
}

// ── BenchmarkCompareCollectedRows_WithListCols ────────────────────────────────
// End-to-end benchmark for the full comparison path when list columns are
// present and require deduplication on both sides.

func BenchmarkCompareCollectedRows_WithListCols(b *testing.B) {
	type bench struct {
		rows, cols, listLen, dups int
	}
	cases := []bench{
		{rows: 1, cols: 1, listLen: 8, dups: 4},
		{rows: 10, cols: 1, listLen: 8, dups: 4},
		{rows: 100, cols: 1, listLen: 8, dups: 4},
		{rows: 100, cols: 3, listLen: 8, dups: 4},
		{rows: 100, cols: 1, listLen: 64, dups: 32},
		{rows: 100, cols: 1, listLen: 8, dups: 0}, // all-match, no dedup needed
	}

	for _, bc := range cases {
		bc := bc
		name := fmt.Sprintf("rows=%d/cols=%d/len=%d/dups=%d",
			bc.rows, bc.cols, bc.listLen, bc.dups)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			table := makeListTable(bc.cols)
			b.ResetTimer()
			for range b.N {
				test, oracle := makeCompareRows(bc.rows, bc.cols, bc.listLen, bc.dups)
				result := CompareCollectedRows(table, test, oracle)
				if result.ToError() != nil {
					b.Fatalf("unexpected diff: %v", result.ToError())
				}
			}
		})
	}
}
