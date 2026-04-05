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

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
)

func (g *Generator) Select(ctx context.Context) (*typedef.Stmt, error) {
	switch n := g.ratioController.GetSelectSubtype(); n {
	case SelectSinglePartitionQuery:
		metrics.StatementsGenerated.WithLabelValues("select", "single_partition").Inc()
		return g.genSelectSinglePartitionQuery(ctx)
	case SelectMultiplePartitionQuery:
		metrics.StatementsGenerated.WithLabelValues("select", "multi_partition").Inc()
		return g.genSelectMultiplePartitionQuery(ctx)
	case SelectClusteringRangeQuery:
		metrics.StatementsGenerated.WithLabelValues("select", "single_partition").Inc()
		return g.genSelectSinglePartitionQuery(ctx)
	case SelectMultiplePartitionClusteringRangeQuery:
		metrics.StatementsGenerated.WithLabelValues("select", "multi_partition").Inc()
		return g.genSelectMultiplePartitionQuery(ctx)
	case SelectSingleIndexQuery:
		if len(g.table.Indexes) == 0 {
			metrics.StatementsGenerated.WithLabelValues("select", "single_partition").Inc()
			return g.genSelectSinglePartitionQuery(ctx)
		}

		metrics.StatementsGenerated.WithLabelValues("select", "index").Inc()
		return g.genSingleIndexQuery(), nil
	default:
		panic(fmt.Sprintf("unexpected case in GenCheckStmt, random value: %d", n))
	}
}

func (g *Generator) genSelectSinglePartitionQuery(ctx context.Context) (*typedef.Stmt, error) {
	g.table.RLock()
	defer g.table.RUnlock()

	pk, builder, err := g.getSelectSinglePartitionKeys(ctx)
	if err != nil {
		return nil, err
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		QueryType:     typedef.SelectStatementType,
		Query:         query,
		PartitionKeys: []typedef.PartitionKeys{pk},
		Values:        pk.Values.ToCQLValues(g.table.PartitionKeys),
	}, nil
}

func (g *Generator) getSelectSinglePartitionKeys(_ context.Context) (typedef.PartitionKeys, *qb.SelectBuilder, error) {
	partitionKeys := g.generator.Next()

	builder := qb.Select(g.keyspaceAndTable).Columns(g.selectColumns...)
	for _, pk := range g.table.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
	}

	return partitionKeys, builder, nil
}

func (g *Generator) buildSelectMultiPartitionsKey(_ context.Context) ([]typedef.PartitionKeys, *qb.SelectBuilder, *typedef.Values, error) {
	builder := qb.Select(g.keyspaceAndTable).Columns(g.selectColumns...)

	numQueryPKs := g.getMultiplePartitionKeys()
	pks := make([]typedef.PartitionKeys, 0, numQueryPKs)
	combined := typedef.NewValues(g.table.PartitionKeys.Len())

	for range numQueryPKs {
		pk := g.generator.Next()
		pks = append(pks, pk)
		combined.Merge(pk.Values)
	}

	for _, pk := range g.table.PartitionKeys {
		builder.Where(qb.InTuple(pk.Name, numQueryPKs))
	}

	return pks, builder, combined, nil
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
	pks, builder, combined, err := g.buildSelectMultiPartitionsKey(ctx)
	if err != nil {
		return nil, err
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		PartitionKeys: pks,
		Values:        combined.ToCQLValues(g.table.PartitionKeys),
		QueryType:     typedef.SelectMultiPartitionType,
		Query:         query,
	}, nil
}

//nolint:unused
func (g *Generator) genClusteringRangeQuery(ctx context.Context) (*typedef.Stmt, error) {
	pk, builder, err := g.getSelectSinglePartitionKeys(ctx)
	if err != nil {
		return nil, err
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
func (g *Generator) genMultiplePartitionClusteringRangeQuery(ctx context.Context) (*typedef.Stmt, error) {
	pks, builder, combined, err := g.buildSelectMultiPartitionsKey(ctx)
	if err != nil {
		return nil, err
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
		values = append(values, idx.Column.Type.GenValue(g.random, g.valueRangeConfig)...)
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		PartitionKeys: nil,
		Values:        values,
		QueryType:     typedef.SelectByIndexStatementType,
		Query:         query,
	}
}
