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
		return g.genClusteringRangeQuery(ctx)
	case SelectMultiplePartitionClusteringRangeQuery:
		return g.genMultiplePartitionClusteringRangeQuery(ctx)
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

func (g *Generator) genSelectMultiplePartitionQuery(_ context.Context) (*typedef.Stmt, error) {
	numQueryPKs, pks, combined := g.selectMultiPartitionKeys()

	return &typedef.Stmt{
		PartitionKeys: pks,
		Values:        combined.ToCQLValues(g.table.PartitionKeys),
		QueryType:     typedef.SelectMultiPartitionType,
		Query:         g.selectMultiplePartitionQueries[numQueryPKs],
	}, nil
}

// genClusteringRangeQuery selects a clustering-key range within a single
// partition. It pops a real observed row from the row tracker (populated
// during validation SELECTs, the same mechanism deleteSingleRow/deleteClusteringSubset
// use) and brackets the range around that row's actual last-clustering-key
// value with an inclusive Gte/Lte range, guaranteeing at least that row
// matches. Bounds generated independently at random (the previous approach)
// almost never overlap real data, so the query always returned 0 rows.
//
// Falls back to a plain single-partition SELECT when there is no clustering
// key, no tracked row available, or the tracked row is too short for the
// current schema (e.g. captured before a schema change).
func (g *Generator) genClusteringRangeQuery(ctx context.Context) (*typedef.Stmt, error) {
	if len(g.table.ClusteringKeys) == 0 {
		return g.genSelectSinglePartitionQuery(ctx)
	}

	trackedRow, ok := g.generator.PopTrackedRow()
	if !ok {
		return g.genSelectSinglePartitionQuery(ctx)
	}

	maxClusteringKeys := g.getMultipleClusteringKeys()
	ckLen := g.table.ClusteringKeys[:maxClusteringKeys].LenValues()
	if len(trackedRow.ClusteringValues) < ckLen {
		return g.genSelectSinglePartitionQuery(ctx)
	}

	values := make([]any, 0, g.table.PartitionKeysLenValues()+ckLen+1)
	values, pkVals, ok := g.appendTrackedKeys(values, trackedRow, maxClusteringKeys-1)
	if !ok {
		return g.genSelectSinglePartitionQuery(ctx)
	}

	builder := qb.Select(g.keyspaceAndTable).Columns(g.selectColumns...)
	for _, pk := range g.table.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
	}
	for _, ck := range g.table.ClusteringKeys[:maxClusteringKeys-1] {
		builder = builder.Where(qb.Eq(ck.Name))
	}

	rangedCK := g.table.ClusteringKeys[maxClusteringKeys-1]
	builder = builder.Where(qb.GtOrEq(rangedCK.Name)).Where(qb.LtOrEq(rangedCK.Name))
	rangedValue := trackedRow.ClusteringValues[ckLen-1]
	values = append(values, rangedValue, rangedValue)

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{{ID: trackedRow.PartitionID, Values: pkVals}},
		Values:        values,
		QueryType:     typedef.SelectRangeStatementType,
		Query:         query,
	}, nil
}

// genMultiplePartitionClusteringRangeQuery is the multi-partition counterpart
// of genClusteringRangeQuery: it IN-tuples the tracked row's own partition
// together with numQueryPKs-1 additional random partitions, then brackets the
// range around the tracked row's actual value as above. The tracked row's
// partition is always included in the IN-tuple, so the range is guaranteed to
// match at least that row regardless of what the other partitions contain.
func (g *Generator) genMultiplePartitionClusteringRangeQuery(ctx context.Context) (*typedef.Stmt, error) {
	if len(g.table.ClusteringKeys) == 0 {
		return g.genSelectMultiplePartitionQuery(ctx)
	}

	trackedRow, ok := g.generator.PopTrackedRow()
	if !ok {
		return g.genSelectMultiplePartitionQuery(ctx)
	}

	maxClusteringKeys := g.getMultipleClusteringKeys()
	pkLen := g.table.PartitionKeysLenValues()
	ckLen := g.table.ClusteringKeys[:maxClusteringKeys].LenValues()
	if len(trackedRow.PartitionValues) < pkLen || len(trackedRow.ClusteringValues) < ckLen {
		return g.genSelectMultiplePartitionQuery(ctx)
	}

	numQueryPKs := g.getMultiplePartitionKeys()
	combined := typedef.NewPartitionValues(g.table.PartitionKeys, trackedRow.PartitionValues)
	pks := make([]typedef.PartitionKeys, 0, numQueryPKs)
	pks = append(pks, typedef.PartitionKeys{ID: trackedRow.PartitionID, Values: combined})

	for range numQueryPKs - 1 {
		pk := g.generator.Next()
		pks = append(pks, pk)
		combined.Merge(pk.Values)
	}

	builder := qb.Select(g.keyspaceAndTable).Columns(g.selectColumns...)
	for _, pk := range g.table.PartitionKeys {
		builder = builder.Where(qb.InTuple(pk.Name, numQueryPKs))
	}
	for _, ck := range g.table.ClusteringKeys[:maxClusteringKeys-1] {
		builder = builder.Where(qb.Eq(ck.Name))
	}

	rangedCK := g.table.ClusteringKeys[maxClusteringKeys-1]
	builder = builder.Where(qb.GtOrEq(rangedCK.Name)).Where(qb.LtOrEq(rangedCK.Name))

	values := combined.ToCQLValues(g.table.PartitionKeys)
	values = append(values, trackedRow.ClusteringValues[:ckLen-1]...)
	rangedValue := trackedRow.ClusteringValues[ckLen-1]
	values = append(values, rangedValue, rangedValue)

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
