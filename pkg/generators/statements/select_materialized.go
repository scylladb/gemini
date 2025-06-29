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
	"github.com/scylladb/gemini/pkg/utils"
)

func (g *Generator) SelectFromMaterializedView(ctx context.Context) *typedef.Stmt {
	mv := &g.table.MaterializedViews[utils.RandInt2(g.random, 0, len(g.table.MaterializedViews))]

	switch val := g.random.IntN(SelectStatementsCount - 1); val {
	case SelectSinglePartitionQuery:
		return g.genSinglePartitionQueryMv(ctx, mv)
	case SelectMultiplePartitionQuery:
		return g.genMultiplePartitionQueryMv(ctx, mv)
	case SelectClusteringRangeQuery:
		return g.genClusteringRangeQueryMv(ctx, mv)
	case SelectMultiplePartitionClusteringRangeQuery:
		return g.genMultiplePartitionClusteringRangeQueryMv(ctx, mv)
	default:
		panic(fmt.Sprintf("unexpected case in genCheckStmtForMv, random value: %d", val))
	}
}

func (g *Generator) genSinglePartitionQueryMv(ctx context.Context, mv *typedef.MaterializedView) *typedef.Stmt {
	pk := g.generator.GetOld(ctx)

	builder := qb.Select(g.keyspace + "." + mv.Name)

	for _, p := range mv.PartitionKeys {
		builder = builder.Where(qb.Eq(p.Name))
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		PartitionKeys: pk,
		Values:        pk.Values.ToCQLValues(mv.PartitionKeys),
		QueryType:     typedef.SelectFromMaterializedViewStatementType,
		Query:         query,
	}
}

func (g *Generator) genMultiplePartitionQueryMv(ctx context.Context, mv *typedef.MaterializedView) *typedef.Stmt {
	numQueryPKs := utils.RandInt2(g.random, 2, mv.PartitionKeys.Len())
	values := make([]any, numQueryPKs*mv.PartitionKeys.LenValues())
	builder := qb.Select(g.keyspace + "." + mv.Name)

	for _, pk := range mv.PartitionKeys {
		builder = builder.Where(qb.InTuple(pk.Name, numQueryPKs))
	}

	pks := make(typedef.Values, numQueryPKs)

	for range numQueryPKs {
		pk := g.generator.GetOld(ctx)
		// TODO: Handle return value with token
		//	if vs.Token == 0 {
		//		g.GiveOlds(ctx, tokens...)
		//		return nil
		//	}

		values = append(values, pk.Values.ToCQLValues(mv.PartitionKeys))
		pks.Merge(pk.Values)
	}

	query, _ := builder.ToCql()
	return &typedef.Stmt{
		PartitionKeys: typedef.PartitionKeys{Token: 0, Values: pks},
		Values:        nil,
		QueryType:     typedef.SelectFromMaterializedViewStatementType,
		Query:         query,
	}
}

func (g *Generator) genMultiplePartitionClusteringRangeQueryMv(ctx context.Context, mv *typedef.MaterializedView) *typedef.Stmt {
	// numQueryPKs := utils.RandInt2(g.random, 2, mv.PartitionKeys.Len())
	// maxClusteringRels := utils.RandInt2(g.random, 1, mv.ClusteringKeys.Len())

	return nil
	//clusteringKeys := mv.ClusteringKeys
	//pkValues := mv.PartitionKeysLenValues()
	//valuesCount := pkValues*numQueryPKs + clusteringKeys[:maxClusteringRels].LenValues() + clusteringKeys[maxClusteringRels].Type.LenValue()*2
	//mvKey := mv.NonPrimaryKey
	//
	//var (
	//	mvKeyLen int
	//	baseID   int
	//)
	//if mvKey != nil {
	//	mvKeyLen = mvKey.Type.LenValue()
	//	baseID = 1
	//	valuesCount += mv.PartitionKeys.LenValues() * numQueryPKs
	//}
	//values := make(typedef.Values, pkValues*numQueryPKs, valuesCount)
	//typs := make(typedef.Types, pkValues*numQueryPKs, valuesCount)
	//builder := qb.Select(s.Keyspace.Name + "." + mv.Name)
	//tokens := make([]typedef.ValueWithToken, 0, numQueryPKs)
	//
	//for _, pk := range mv.PartitionKeys {
	//	builder = builder.Where(qb.InTuple(pk.Name, numQueryPKs))
	//}
	//
	//if mvKey != nil {
	//	// Fill values for Materialized view primary key
	//	for j := 0; j < numQueryPKs; j++ {
	//		typs[j] = mvKey.Type
	//		copy(values[j*mvKeyLen:], mvKey.Type.GenValue(r, p))
	//	}
	//}
	//
	//for j := 0; j < numQueryPKs; j++ {
	//	vs := g.GetOld(ctx)
	//	if vs.Token == 0 {
	//		g.GiveOlds(ctx, tokens...)
	//		return nil
	//	}
	//	tokens = append(tokens, vs)
	//	for id := range vs.Value {
	//		idx := (baseID+id)*numQueryPKs + j
	//		typs[idx] = mv.PartitionKeys[baseID+id].Type
	//		values[idx] = vs.Value[id]
	//	}
	//}
	//
	//if len(clusteringKeys) > 0 {
	//	for i := 0; i < maxClusteringRels; i++ {
	//		ck := clusteringKeys[i]
	//		builder = builder.Where(qb.Eq(ck.Name))
	//		values = append(values, ck.Type.GenValue(r, p)...)
	//		typs = append(typs, ck.Type)
	//	}
	//	ck := clusteringKeys[maxClusteringRels]
	//	builder = builder.Where(qb.Gt(ck.Name)).Where(qb.Lt(ck.Name))
	//	values = append(values, ck.Type.GenValue(r, p)...)
	//	values = append(values, ck.Type.GenValue(r, p)...)
	//	typs = append(typs, ck.Type, ck.Type)
	//}
	//return &typedef.Stmt{
	//	StmtCache: typedef.StmtCache{
	//		Query:     builder,
	//		Types:     typs,
	//		QueryType: typedef.SelectFromMaterializedViewStatementType,
	//	},
	//	Values:          values,
	//	ValuesWithToken: tokens,
	//}
}

func (g *Generator) genClusteringRangeQueryMv(ctx context.Context, mv *typedef.MaterializedView) *typedef.Stmt {
	// maxClusteringRels := utils.RandInt2(g.random, 1, mv.ClusteringKeys.Len())
	return nil
	//t.RLock()
	//defer t.RUnlock()
	//valuesWithToken := g.GetOld(ctx)
	//if valuesWithToken.Token == 0 {
	//	return nil
	//}
	//values := valuesWithToken.Value.Copy()
	//if mv.HaveNonPrimaryKey() {
	//	mvValues := append([]any{}, mv.NonPrimaryKey.Type.GenValue(r, p)...)
	//	values = append(mvValues, values...)
	//}
	//builder := qb.Select(s.Keyspace.Name + "." + mv.Name)
	//
	//allTypes := make([]typedef.Type, 0, len(mv.PartitionKeys)+maxClusteringRels+1)
	//for _, pk := range mv.PartitionKeys {
	//	builder = builder.Where(qb.Eq(pk.Name))
	//	allTypes = append(allTypes, pk.Type)
	//}
	//
	//clusteringKeys := mv.ClusteringKeys
	//if len(clusteringKeys) > 0 {
	//	for i := 0; i < maxClusteringRels; i++ {
	//		ck := clusteringKeys[i]
	//		builder = builder.Where(qb.Eq(ck.Name))
	//		values = append(values, ck.Type.GenValue(r, p)...)
	//		allTypes = append(allTypes, ck.Type)
	//	}
	//	ck := clusteringKeys[maxClusteringRels]
	//	builder = builder.Where(qb.Gt(ck.Name)).Where(qb.Lt(ck.Name))
	//	values = append(values, t.ClusteringKeys[maxClusteringRels].Type.GenValue(r, p)...)
	//	values = append(values, t.ClusteringKeys[maxClusteringRels].Type.GenValue(r, p)...)
	//	allTypes = append(allTypes, ck.Type, ck.Type)
	//}
	//return &typedef.Stmt{
	//	StmtCache: typedef.StmtCache{
	//		Query:     builder,
	//		QueryType: typedef.SelectRangeStatementType,
	//		Types:     allTypes,
	//	},
	//	Values:          values,
	//	ValuesWithToken: []typedef.ValueWithToken{valuesWithToken},
	//}
}
