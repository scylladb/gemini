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

func (g *Generator) genSelectSinglePartitionQuery(ctx context.Context) (*typedef.Stmt, error) {
	g.table.RLock()
	defer g.table.RUnlock()

	pks, builder, err := g.getSelectSinglePartitionKeys(ctx)
	if err != nil {
		return nil, err
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		QueryType:     typedef.SelectStatementType,
		Query:         query,
		PartitionKeys: pks,
		Values:        pks.Values.ToCQLValues(g.table.PartitionKeys),
	}, nil
}

func (g *Generator) getSelectSinglePartitionKeys(_ context.Context) (typedef.PartitionKeys, *qb.SelectBuilder, error) {
	partitionKeys := g.generator.Next()

	builder := qb.Select(g.keyspaceAndTable).Columns(g.selectColumns...)
	for _, pk := range g.table.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
	}

	return typedef.PartitionKeys{
		Values: partitionKeys,
	}, builder, nil
}

func (g *Generator) buildSelectMultiPartitionsKey(_ context.Context) (*typedef.Values, *qb.SelectBuilder, error) {
	builder := qb.Select(g.keyspaceAndTable).Columns(g.selectColumns...)

	numQueryPKs := g.getMultiplePartitionKeys()
	pks := typedef.NewValues(g.table.PartitionKeys.Len())

	for range numQueryPKs {
		pk := g.generator.Next()
		pks.Merge(pk)
	}

	for _, pk := range g.table.PartitionKeys {
		builder.Where(qb.InTuple(pk.Name, numQueryPKs))
	}

	return pks, builder, nil
}

//nolint:unused
func (g *Generator) buildSelectClusteringRange(builder *qb.SelectBuilder, values []any) []any {
	if len(g.table.ClusteringKeys) == 0 {
		return values
	}

	values = slices.Grow(values, g.table.ClusteringKeys.LenValues())
	maxClusteringKeys := g.getMultipleClusteringKeys()

	for _, ck := range g.table.ClusteringKeys[:maxClusteringKeys-1] {
		builder.Where(qb.Eq(ck.Name))
		values = append(values, ck.Type.GenValue(g.random, g.valueRangeConfig)...)
	}

	ck := g.table.ClusteringKeys[maxClusteringKeys-1]
	builder.Where(qb.Gt(ck.Name)).Where(qb.Lt(ck.Name))
	values = append(values, ck.Type.GenValue(g.random, g.valueRangeConfig)...)
	values = append(values, ck.Type.GenValue(g.random, g.valueRangeConfig)...)

	return values
}

func (g *Generator) genSelectMultiplePartitionQuery(ctx context.Context) (*typedef.Stmt, error) {
	pks, builder, err := g.buildSelectMultiPartitionsKey(ctx)
	if err != nil {
		return nil, err
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		PartitionKeys: typedef.PartitionKeys{Values: pks},
		Values:        pks.ToCQLValues(g.table.PartitionKeys),
		QueryType:     typedef.SelectMultiPartitionType,
		Query:         query,
	}, nil
}

//nolint:unused
func (g *Generator) genClusteringRangeQuery(ctx context.Context) (*typedef.Stmt, error) {
	pks, builder, err := g.getSelectSinglePartitionKeys(ctx)
	if err != nil {
		return nil, err
	}

	values := g.buildSelectClusteringRange(builder, pks.Values.ToCQLValues(g.table.PartitionKeys))

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		PartitionKeys: pks,
		Values:        values,
		QueryType:     typedef.SelectRangeStatementType,
		Query:         query,
	}, nil
}

//nolint:unused
func (g *Generator) genMultiplePartitionClusteringRangeQuery(ctx context.Context) (*typedef.Stmt, error) {
	pks, builder, err := g.buildSelectMultiPartitionsKey(ctx)
	if err != nil {
		return nil, err
	}

	values := g.buildSelectClusteringRange(builder, pks.ToCQLValues(g.table.PartitionKeys))

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		QueryType:     typedef.SelectMultiPartitionRangeStatementType,
		PartitionKeys: typedef.PartitionKeys{Values: pks},
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
		values = append(values, idx.Column.Type.GenValue(g.random, g.valueRangeConfig)...)
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		PartitionKeys: typedef.PartitionKeys{},
		Values:        values,
		QueryType:     typedef.SelectByIndexStatementType,
		Query:         query,
	}
}
