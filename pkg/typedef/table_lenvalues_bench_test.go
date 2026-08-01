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

package typedef

import "testing"

// wideBenchTable builds a table with n non-key columns to exercise the
// metadata-cache hot path the way a wide generated schema would.
func wideBenchTable(n int) *Table {
	cols := make(Columns, n)
	for i := range n {
		cols[i] = ColumnDef{Name: "col" + itoa(i), Type: TypeInt}
	}
	return &Table{
		Name:           "t",
		PartitionKeys:  Columns{{Name: "pk1", Type: TypeInt}, {Name: "pk2", Type: TypeText}},
		ClusteringKeys: Columns{{Name: "ck1", Type: TypeBigint}, {Name: "ck2", Type: TypeText}},
		Columns:        cols,
	}
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var b [12]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(b[pos:])
}

// BenchmarkTableLenValues_Cached measures the per-statement cost of summing the
// bind-value counts for all three key/column groups via the cached accessors
// (one sync.Once fast-path load each), as the INSERT path does.
func BenchmarkTableLenValues_Cached(b *testing.B) {
	t := wideBenchTable(30)
	t.cacheOnce.Do(t.buildCache) // pre-warm, as Init would
	b.ReportAllocs()
	b.ResetTimer()
	sink := 0
	for b.Loop() {
		sink += t.PartitionKeysLenValues() + t.ClusteringKeysLenValues() + t.ColumnsLenValues()
	}
	_ = sink
}

// BenchmarkTableLenValues_Recompute measures the previous behaviour: re-summing
// Type.LenValue() across every column on every call.
func BenchmarkTableLenValues_Recompute(b *testing.B) {
	t := wideBenchTable(30)
	b.ReportAllocs()
	b.ResetTimer()
	sink := 0
	for b.Loop() {
		sink += t.PartitionKeys.LenValues() + t.ClusteringKeys.LenValues() + t.Columns.LenValues()
	}
	_ = sink
}
