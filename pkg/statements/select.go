// Copyright 2025 ScyllaDB
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
	"context"
	"fmt"
	"slices"

	"github.com/scylladb/gocqlx/v3/qb"

	"github.com/scylladb/gemini/pkg/typedef"
)

func (g *Generator) Select(ctx context.Context) (*typedef.Stmt, error) {
	switch n := g.ratioController.GetSelectSubtype(); n {
	case SelectSinglePartitionQuery:
		return g.genSelectSinglePartitionQuery(ctx)
	case SelectMultiplePartitionQuery:
		return g.genSelectMultiplePartitionQuery(ctx)
	case SelectClusteringRangeQuery:
		return g.genSelectSinglePartitionQuery(ctx)
		// TODO(CodeLieutenant): single partition clustering range queries
		//    are doing **always** 0 returns, basically generated values for clustering keys
		//    are always in the not found range.
		// return g.genClusteringRangeQuery(ctx)
	case SelectMultiplePartitionClusteringRangeQuery:
		return g.genSelectMultiplePartitionQuery(ctx)
		// TODO(CodeLieutenant): multiple partition clustering range queries
		//    are doing **always** 0 returns, basically generated values for clustering keys
		//    are always in the not found range.
		// return g.genMultiplePartitionClusteringRangeQuery(ctx)
	case SelectSingleIndexQuery:
		if len(g.table.Indexes) == 0 {
			return g.genSelectSinglePartitionQuery(ctx)
		}

		return g.genSingleIndexQuery(), nil
	default:
		panic(fmt.Sprintf("unexpected case in GenCheckStmt, random value: %d", n))
	}
}

// buildCachedSelectQueries pre-builds the static SELECT query strings. The
// single-partition SELECT (equality on every partition key) is the most common
// validation query and is identical for the immutable schema, so it is built
// once here instead of via the qb builder on every validation.
//
// The multi-partition SELECT varies only in how many partition keys are
// IN-tupled together (see getMultiplePartitionKeys), and that count is bounded
// by the number of partition-key columns, so every possible shape is built
// once here too and indexed by count instead of re-run through the qb builder
// on every validation.
func (g *Generator) buildCachedSelectQueries() {
	builder := qb.Select(g.keyspaceAndTable).Columns(g.selectColumns...)
	for _, pk := range g.table.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
	}
	g.selectSinglePartitionQuery, _ = builder.ToCql()

	maxN := g.table.PartitionKeys.Len()
	g.selectMultiplePartitionQueries = make([]string, maxN+1)
	for n := 1; n <= maxN; n++ {
		mb := qb.Select(g.keyspaceAndTable).Columns(g.selectColumns...)
		for _, pk := range g.table.PartitionKeys {
			mb = mb.Where(qb.InTuple(pk.Name, n))
		}
		g.selectMultiplePartitionQueries[n], _ = mb.ToCql()
	}
}

func (g *Generator) genSelectSinglePartitionQuery(_ context.Context) (*typedef.Stmt, error) {
	pk := g.generator.Next()

	return &typedef.Stmt{
		QueryType:     typedef.SelectStatementType,
		Query:         g.selectSinglePartitionQuery,
		PartitionKeys: []typedef.PartitionKeys{pk},
		Values:        pk.Values.ToCQLValues(g.table.PartitionKeys),
	}, nil
}

// selectMultiPartitionKeys picks a random number of partition keys (bounded by
// TotalCartesianProductCount, see getMultiplePartitionKeys) and merges their
// values for a multi-partition SELECT.
func (g *Generator) selectMultiPartitionKeys() (int, []typedef.PartitionKeys, *typedef.Values) {
	numQueryPKs := g.getMultiplePartitionKeys()
	pks := make([]typedef.PartitionKeys, 0, numQueryPKs)
	combined := typedef.NewValues(g.table.PartitionKeys.Len())

	for range numQueryPKs {
		pk := g.generator.Next()
		pks = append(pks, pk)
		combined.Merge(pk.Values)
	}

	return numQueryPKs, pks, combined
}

//nolint:unused
func (g *Generator) buildSelectClusteringRange(builder *qb.SelectBuilder, values []any) []any {
	if len(g.table.ClusteringKeys) == 0 {
		return values
	}

	values = slices.Grow(values, g.table.ClusteringKeysLenValues())
	maxClusteringKeys := g.getMultipleClusteringKeys()

	for _, ck := range g.table.ClusteringKeys[:maxClusteringKeys-1] {
		builder.Where(qb.Eq(ck.Name))
		values = ck.Type.GenValueOut(values, g.random, g.valueRangeConfig)
	}

	ck := g.table.ClusteringKeys[maxClusteringKeys-1]
	builder.Where(qb.Gt(ck.Name)).Where(qb.Lt(ck.Name))
	values = ck.Type.GenValueOut(values, g.random, g.valueRangeConfig)
	values = ck.Type.GenValueOut(values, g.random, g.valueRangeConfig)

	return values
}

func (g *Generator) genSelectMultiplePartitionQuery(_ context.Context) (*typedef.Stmt, error) {
	numQueryPKs, pks, combined := g.selectMultiPartitionKeys()

	return &typedef.Stmt{
		PartitionKeys: pks,
		Values:        combined.ToCQLValues(g.table.PartitionKeys),
		QueryType:     typedef.SelectMultiPartitionType,
		Query:         g.selectMultiplePartitionQueries[numQueryPKs],
	}, nil
}

//nolint:unused
func (g *Generator) genClusteringRangeQuery(_ context.Context) (*typedef.Stmt, error) {
	pk := g.generator.Next()

	builder := qb.Select(g.keyspaceAndTable).Columns(g.selectColumns...)
	for _, pkCol := range g.table.PartitionKeys {
		builder = builder.Where(qb.Eq(pkCol.Name))
	}

	values := g.buildSelectClusteringRange(builder, pk.Values.ToCQLValues(g.table.PartitionKeys))

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{pk},
		Values:        values,
		QueryType:     typedef.SelectRangeStatementType,
		Query:         query,
	}, nil
}

//nolint:unused
func (g *Generator) genMultiplePartitionClusteringRangeQuery(_ context.Context) (*typedef.Stmt, error) {
	numQueryPKs, pks, combined := g.selectMultiPartitionKeys()

	builder := qb.Select(g.keyspaceAndTable).Columns(g.selectColumns...)
	for _, pk := range g.table.PartitionKeys {
		builder = builder.Where(qb.InTuple(pk.Name, numQueryPKs))
	}

	values := g.buildSelectClusteringRange(builder, combined.ToCQLValues(g.table.PartitionKeys))

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		QueryType:     typedef.SelectMultiPartitionRangeStatementType,
		PartitionKeys: pks,
		Values:        values,
		Query:         query,
	}, nil
}

func (g *Generator) genSingleIndexQuery() *typedef.Stmt {
	idxCount := g.getIndex(0)
	values := make([]any, 0, idxCount)
	builder := qb.Select(g.keyspaceAndTable).Columns(g.selectColumns...).AllowFiltering()

	g.table.RLock()
	defer g.table.RUnlock()

	for _, idx := range g.table.Indexes[:idxCount] {
		builder = builder.Where(qb.Eq(idx.ColumnName))
		values = idx.Column.Type.GenValueOut(values, g.random, g.valueRangeConfig)
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		PartitionKeys: nil,
		Values:        values,
		QueryType:     typedef.SelectByIndexStatementType,
		Query:         query,
	}
}
