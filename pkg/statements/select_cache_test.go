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
	"uuid"

	"github.com/scylladb/gocqlx/v3/qb"
	"github.com/stretchr/testify/assert"

	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/typedef"
)

func cachedSelectTable() *typedef.Table {
	table := &typedef.Table{
		Name: "t",
		PartitionKeys: typedef.Columns{
			{Name: "pk1", Type: typedef.TypeInt},
			{Name: "pk2", Type: typedef.TypeText},
		},
		ClusteringKeys: typedef.Columns{
			{Name: "ck1", Type: typedef.TypeBigint},
			{Name: "ck2", Type: typedef.TypeInt},
			{Name: "ck3", Type: typedef.TypeText},
		},
		Columns: typedef.Columns{
			{Name: "c1", Type: typedef.TypeInt},
			{Name: "c2", Type: typedef.TypeFloat},
			{Name: "c3", Type: typedef.TypeDouble},
		},
	}
	for i, col := range table.Columns {
		table.Indexes = append(table.Indexes, typedef.IndexDef{
			IndexName:  GenIndexName("t_col", i),
			ColumnName: col.Name,
			Column:     col,
		})
	}

	return table
}

// TestCachedSelectShapesMatchBuilder: every cached SELECT shape must be the
// query that the qb builder makes for the same parameters.
func TestCachedSelectShapesMatchBuilder(t *testing.T) {
	t.Parallel()

	table := cachedSelectTable()
	mp := newMockPartitionsWithTracker(10)
	t.Cleanup(mp.Close)
	gen := newSelectRangeTestGenerator(t, table, mp)

	rangeQuery := func(builder *qb.SelectBuilder, m int) string {
		for _, ck := range table.ClusteringKeys[:m-1] {
			builder = builder.Where(qb.Eq(ck.Name))
		}
		ranged := table.ClusteringKeys[m-1].Name
		query, _ := builder.Where(qb.GtOrEq(ranged)).Where(qb.LtOrEq(ranged)).ToCql()
		return query
	}

	for m := 1; m <= len(table.ClusteringKeys); m++ {
		builder := qb.Select("ks.t").Columns(table.SelectColumnNames()...)
		for _, pk := range table.PartitionKeys {
			builder = builder.Where(qb.Eq(pk.Name))
		}
		assert.Equal(t, rangeQuery(builder, m), gen.selectClusteringRangeQueries[m], "m=%d", m)

		for n := 1; n <= len(table.PartitionKeys); n++ {
			mb := qb.Select("ks.t").Columns(table.SelectColumnNames()...)
			for _, pk := range table.PartitionKeys {
				mb = mb.Where(qb.InTuple(pk.Name, n))
			}
			assert.Equal(t, rangeQuery(mb, m), gen.selectMultiPartitionClusteringRangeQueries[n][m], "n=%d m=%d", n, m)
		}
	}

	for k := range len(table.Indexes) + 1 {
		builder := qb.Select("ks.t").Columns(table.SelectColumnNames()...).AllowFiltering()
		for _, idx := range table.Indexes[:k] {
			builder = builder.Where(qb.Eq(idx.ColumnName))
		}
		want, _ := builder.ToCql()
		assert.Equal(t, want, gen.selectIndexQueries[k], "k=%d", k)
	}
}

// TestSingleIndexQueryBindsOneValuePerIndex: the cached query and the values
// must agree on the number of bind markers.
func TestSingleIndexQueryBindsOneValuePerIndex(t *testing.T) {
	t.Parallel()

	table := cachedSelectTable()
	mp := newMockPartitionsWithTracker(10)
	t.Cleanup(mp.Close)
	gen := newSelectRangeTestGenerator(t, table, mp)

	for range 50 {
		stmt := gen.genSingleIndexQuery()
		assert.Len(t, stmt.Values, countBindMarkers(stmt.Query), "query %q", stmt.Query)
	}
}

func countBindMarkers(query string) int {
	n := 0
	for _, c := range query {
		if c == '?' {
			n++
		}
	}
	return n
}

func newSelectBenchGenerator(b *testing.B, table *typedef.Table) *Generator {
	b.Helper()

	mp := newMockPartitionsWithTracker(10)
	b.Cleanup(mp.Close)
	mp.TrackRow(partitions.TrackedRow{
		PartitionID:      uuid.New(),
		PartitionValues:  []any{int32(1)},
		ClusteringValues: []any{int64(2), int32(3), "x"},
	})

	rng := rand.New(rand.NewChaCha8([32]byte{5}))
	rc, err := NewRatioController(DefaultStatementRatios(), rng)
	if err != nil {
		b.Fatal(err)
	}
	vc := &typedef.ValueRangeConfig{MaxBlobLength: 32, MinBlobLength: 1, MaxStringLength: 32, MinStringLength: 1}

	return New("ks", mp, table, rng, vc, rc, false)
}

// benchSelectTable has one partition key, because the tracker mock returns
// partitions with pk1 only.
func benchSelectTable() *typedef.Table {
	table := cachedSelectTable()
	table.PartitionKeys = table.PartitionKeys[:1]
	return table
}

func BenchmarkGenClusteringRangeQuery(b *testing.B) {
	gen := newSelectBenchGenerator(b, benchSelectTable())
	ctx := b.Context()

	b.ReportAllocs()
	for b.Loop() {
		stmt, err := gen.genClusteringRangeQuery(ctx)
		if err != nil || stmt.QueryType != typedef.SelectRangeStatementType {
			b.Fatalf("got %v, %v", stmt, err)
		}
	}
}

func BenchmarkGenMultiplePartitionClusteringRangeQuery(b *testing.B) {
	gen := newSelectBenchGenerator(b, benchSelectTable())
	ctx := b.Context()

	b.ReportAllocs()
	for b.Loop() {
		stmt, err := gen.genMultiplePartitionClusteringRangeQuery(ctx)
		if err != nil || stmt.QueryType != typedef.SelectMultiPartitionRangeStatementType {
			b.Fatalf("got %v, %v", stmt, err)
		}
	}
}

func BenchmarkGenSingleIndexQuery(b *testing.B) {
	gen := newSelectBenchGenerator(b, benchSelectTable())

	b.ReportAllocs()
	for b.Loop() {
		if stmt := gen.genSingleIndexQuery(); stmt.QueryType != typedef.SelectByIndexStatementType {
			b.Fatalf("got %v", stmt)
		}
	}
}
