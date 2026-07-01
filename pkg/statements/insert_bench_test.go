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

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/typedef"
)

// insertBenchTable builds a wide table (n non-key columns) so the per-column
// value-generation cost in Insert is visible.
func insertBenchTable(n int) *typedef.Table {
	cols := make(typedef.Columns, n)
	for i := range n {
		cols[i] = typedef.ColumnDef{Name: "col" + itoaStmt(i), Type: typedef.TypeInt}
	}
	return &typedef.Table{
		Name:           "t",
		PartitionKeys:  typedef.Columns{{Name: "pk1", Type: typedef.TypeInt}},
		ClusteringKeys: typedef.Columns{{Name: "ck1", Type: typedef.TypeBigint}},
		Columns:        cols,
	}
}

func itoaStmt(i int) string {
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

func BenchmarkInsertWide(b *testing.B) {
	const n = 1024
	table := insertBenchTable(20)
	src, fn := distributions.New(distributions.Uniform, n, 1, 0, 0)
	parts := partitions.New(b.Context(), rand.New(src), fn, table, typedef.PartitionRangeConfig{}, n, 0)

	ratios := DefaultStatementRatios()
	rng := rand.New(rand.NewChaCha8([32]byte{9}))
	rc, err := NewRatioController(ratios, rng)
	if err != nil {
		b.Fatal(err)
	}
	vc := &typedef.ValueRangeConfig{MaxBlobLength: 32, MinBlobLength: 1, MaxStringLength: 32, MinStringLength: 1}
	g := New("ks", parts, table, rng, vc, rc, false)
	ctx := b.Context()

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		stmt, err := g.Insert(ctx)
		if err != nil {
			b.Fatal(err)
		}
		for i := range stmt.PartitionKeys {
			if stmt.PartitionKeys[i].Release != nil {
				stmt.PartitionKeys[i].Release()
			}
		}
	}
}
