// Copyright 2019 ScyllaDB
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

package jobs

import (
	"math"
	"math/rand/v2"

	"github.com/scylladb/gocqlx/v3/qb"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

func GenCheckStmt(
	s *typedef.Schema,
	table *typedef.Table,
	g generators.Interface,
	rnd *rand.Rand,
	p *typedef.PartitionRangeConfig,
) *typedef.Stmt {
	mvNum := -1
	if len(table.MaterializedViews) > 0 && rnd.Int()%2 == 0 {
		mvNum = utils.RandInt2(rnd, 0, len(table.MaterializedViews))
	}

	switch mvNum {
	case -1:
		var n int

		if len(table.Indexes) > 0 {
			n = rnd.IntN(5)
		} else {
			n = rnd.IntN(4)
		}
		switch n {
		case 0:
			return genSinglePartitionQuery(s, table, g)
		case 1:
			numQueryPKs := utils.RandInt2(rnd, 1, table.PartitionKeys.Len())
			multiplier := int(math.Pow(float64(numQueryPKs), float64(table.PartitionKeys.Len())))
			if multiplier > 100 {
				numQueryPKs = 1
			}
			return genMultiplePartitionQuery(s, table, g, numQueryPKs)
		case 2:
			maxClusteringRels := utils.RandInt2(rnd, 0, table.ClusteringKeys.Len())
			return genClusteringRangeQuery(s, table, g, rnd, p, maxClusteringRels)
		case 3:
			numQueryPKs := utils.RandInt2(rnd, 1, table.PartitionKeys.Len())
			multiplier := int(math.Pow(float64(numQueryPKs), float64(table.PartitionKeys.Len())))
			if multiplier > 100 {
				numQueryPKs = 1
			}
			maxClusteringRels := utils.RandInt2(rnd, 0, table.ClusteringKeys.Len())
			return genMultiplePartitionClusteringRangeQuery(
				s,
				table,
				g,
				rnd,
				p,
				numQueryPKs,
				maxClusteringRels,
			)
		case 4:
			// Reducing the probability to hit these since they often take a long time to run
			switch rnd.IntN(5) {
			case 0:
				idxCount := utils.RandInt2(rnd, 1, len(table.Indexes))
				return genSingleIndexQuery(s, table, g, rnd, p, idxCount)
			default:
				return genSinglePartitionQuery(s, table, g)
			}
		}
	default:
		switch rnd.IntN(4) {
		case 0:
			return genSinglePartitionQueryMv(s, table, g, rnd, p, mvNum)
		case 1:
			lenPartitionKeys := table.MaterializedViews[mvNum].PartitionKeys.Len()
			numQueryPKs := utils.RandInt2(rnd, 1, lenPartitionKeys)
			multiplier := int(math.Pow(float64(numQueryPKs), float64(lenPartitionKeys)))
			if multiplier > 100 {
				numQueryPKs = 1
			}
			return genMultiplePartitionQueryMv(s, table, g, rnd, p, mvNum, numQueryPKs)
		case 2:
			lenClusteringKeys := table.MaterializedViews[mvNum].ClusteringKeys.Len()
			maxClusteringRels := utils.RandInt2(rnd, 0, lenClusteringKeys)
			return genClusteringRangeQueryMv(s, table, g, rnd, p, mvNum, maxClusteringRels)
		case 3:
			lenPartitionKeys := table.MaterializedViews[mvNum].PartitionKeys.Len()
			numQueryPKs := utils.RandInt2(rnd, 1, lenPartitionKeys)
			multiplier := int(math.Pow(float64(numQueryPKs), float64(lenPartitionKeys)))
			if multiplier > 100 {
				numQueryPKs = 1
			}
			lenClusteringKeys := table.MaterializedViews[mvNum].ClusteringKeys.Len()
			maxClusteringRels := utils.RandInt2(rnd, 0, lenClusteringKeys)
			return genMultiplePartitionClusteringRangeQueryMv(
				s,
				table,
				g,
				rnd,
				p,
				mvNum,
				numQueryPKs,
				maxClusteringRels,
			)
		}
	}

	return nil
}

func genSinglePartitionQuery(
	s *typedef.Schema,
	t *typedef.Table,
	g generators.Interface,
) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()
	valuesWithToken := g.GetOld()
	values := valuesWithToken.Value.Copy()
	builder := qb.Select(s.Keyspace.Name + "." + t.Name)
	typs := make([]typedef.Type, 0, len(t.PartitionKeys))
	for _, pk := range t.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		typs = append(typs, pk.Type)
	}

	return &typedef.Stmt{
		StmtCache: typedef.StmtCache{
			Query:     builder,
			Types:     typs,
			QueryType: typedef.SelectStatementType,
		},
		Values:          values,
		ValuesWithToken: []typedef.ValueWithToken{valuesWithToken},
	}
}

func genSinglePartitionQueryMv(
	s *typedef.Schema,
	t *typedef.Table,
	g generators.Interface,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	mvNum int,
) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()
	valuesWithToken := g.GetOld()
	mv := t.MaterializedViews[mvNum]
	builder := qb.Select(s.Keyspace.Name + "." + mv.Name)
	typs := make([]typedef.Type, 0, 10)
	for _, pk := range mv.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		typs = append(typs, pk.Type)
	}

	values := valuesWithToken.Value.Copy()
	if mv.HaveNonPrimaryKey() {
		var mvValues []any
		mvValues = append(mvValues, mv.NonPrimaryKey.Type.GenValue(r, p)...)
		values = append(mvValues, values...)
	}
	return &typedef.Stmt{
		StmtCache: typedef.StmtCache{
			Query:     builder,
			Types:     typs,
			QueryType: typedef.SelectStatementType,
		},
		Values:          values,
		ValuesWithToken: []typedef.ValueWithToken{valuesWithToken},
	}
}

func genMultiplePartitionQuery(
	s *typedef.Schema,
	t *typedef.Table,
	g generators.Interface,
	numQueryPKs int,
) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()
	typs := make([]typedef.Type, numQueryPKs*t.PartitionKeys.Len())
	values := make([]any, numQueryPKs*t.PartitionKeys.Len())

	builder := qb.Select(s.Keyspace.Name + "." + t.Name)
	tokens := make([]typedef.ValueWithToken, 0, numQueryPKs)

	for j := 0; j < numQueryPKs; j++ {
		vs := g.GetOld()
		if vs.Token == 0 {
			g.GiveOlds(tokens...)
			return nil
		}
		tokens = append(tokens, vs)
		for i := range vs.Value {
			values[j+i*numQueryPKs] = vs.Value[i]
			typs[j+i*numQueryPKs] = t.PartitionKeys[i].Type
		}
	}
	for _, pk := range t.PartitionKeys {
		builder = builder.Where(qb.InTuple(pk.Name, numQueryPKs))
	}
	return &typedef.Stmt{
		StmtCache: typedef.StmtCache{
			Query:     builder,
			Types:     typs,
			QueryType: typedef.SelectStatementType,
		},
		Values:          values,
		ValuesWithToken: tokens,
	}
}

func genMultiplePartitionQueryMv(
	s *typedef.Schema,
	t *typedef.Table,
	g generators.Interface,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	mvNum, numQueryPKs int,
) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()

	mv := t.MaterializedViews[mvNum]
	typs := make([]typedef.Type, numQueryPKs*mv.PartitionKeys.Len())
	values := make([]any, numQueryPKs*mv.PartitionKeys.Len())

	builder := qb.Select(s.Keyspace.Name + "." + t.Name)
	tokens := make([]typedef.ValueWithToken, 0, numQueryPKs)

	for j := 0; j < numQueryPKs; j++ {
		vs := g.GetOld()
		if vs.Token == 0 {
			g.GiveOlds(tokens...)
			return nil
		}
		tokens = append(tokens, vs)
		vals := make([]any, mv.PartitionKeys.Len())
		if mv.HaveNonPrimaryKey() {
			vals[0] = mv.NonPrimaryKey.Type.GenValue(r, p)
			copy(vals[1:], vs.Value.Copy())
		} else {
			vals = vs.Value.Copy()
		}
		for i := range vals {
			values[j+i*numQueryPKs] = vals[i]
			typs[j+i*numQueryPKs] = mv.PartitionKeys[i].Type
		}
	}
	for _, pk := range t.PartitionKeys {
		builder = builder.Where(qb.InTuple(pk.Name, numQueryPKs))
	}
	return &typedef.Stmt{
		StmtCache: typedef.StmtCache{
			Query:     builder,
			Types:     typs,
			QueryType: typedef.SelectStatementType,
		},
		Values:          values,
		ValuesWithToken: tokens,
	}
}

func genClusteringRangeQuery(
	s *typedef.Schema,
	t *typedef.Table,
	g generators.Interface,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	maxClusteringRels int,
) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()
	vs := g.GetOld()
	allTypes := make([]typedef.Type, 0, len(t.PartitionKeys)+maxClusteringRels+1)
	values := vs.Value.Copy()
	builder := qb.Select(s.Keyspace.Name + "." + t.Name)

	for _, pk := range t.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		allTypes = append(allTypes, pk.Type)
	}
	clusteringKeys := t.ClusteringKeys
	if len(clusteringKeys) > 0 {
		for i := 0; i < maxClusteringRels; i++ {
			ck := clusteringKeys[i]
			builder = builder.Where(qb.Eq(ck.Name))
			values = append(values, ck.Type.GenValue(r, p)...)
			allTypes = append(allTypes, ck.Type)
		}
		ck := clusteringKeys[maxClusteringRels]
		builder = builder.Where(qb.Gt(ck.Name)).Where(qb.Lt(ck.Name))
		values = append(values, ck.Type.GenValue(r, p)...)
		values = append(values, ck.Type.GenValue(r, p)...)
		allTypes = append(allTypes, ck.Type, ck.Type)
	}
	return &typedef.Stmt{
		StmtCache: typedef.StmtCache{
			Query:     builder,
			QueryType: typedef.SelectRangeStatementType,
			Types:     allTypes,
		},
		Values:          values,
		ValuesWithToken: []typedef.ValueWithToken{vs},
	}
}

func genClusteringRangeQueryMv(
	s *typedef.Schema,
	t *typedef.Table,
	g generators.Interface,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	mvNum, maxClusteringRels int,
) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()
	vs := g.GetOld()
	values := vs.Value.Copy()
	mv := t.MaterializedViews[mvNum]
	if mv.HaveNonPrimaryKey() {
		mvValues := append([]any{}, mv.NonPrimaryKey.Type.GenValue(r, p)...)
		values = append(mvValues, values...)
	}
	builder := qb.Select(s.Keyspace.Name + "." + mv.Name)

	allTypes := make([]typedef.Type, 0, len(mv.PartitionKeys)+maxClusteringRels+1)
	for _, pk := range mv.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		allTypes = append(allTypes, pk.Type)
	}

	clusteringKeys := mv.ClusteringKeys
	if len(clusteringKeys) > 0 {
		for i := 0; i < maxClusteringRels; i++ {
			ck := clusteringKeys[i]
			builder = builder.Where(qb.Eq(ck.Name))
			values = append(values, ck.Type.GenValue(r, p)...)
			allTypes = append(allTypes, ck.Type)
		}
		ck := clusteringKeys[maxClusteringRels]
		builder = builder.Where(qb.Gt(ck.Name)).Where(qb.Lt(ck.Name))
		values = append(values, t.ClusteringKeys[maxClusteringRels].Type.GenValue(r, p)...)
		values = append(values, t.ClusteringKeys[maxClusteringRels].Type.GenValue(r, p)...)
		allTypes = append(allTypes, ck.Type, ck.Type)
	}
	return &typedef.Stmt{
		StmtCache: typedef.StmtCache{
			Query:     builder,
			QueryType: typedef.SelectRangeStatementType,
			Types:     allTypes,
		},
		Values:          values,
		ValuesWithToken: []typedef.ValueWithToken{vs},
	}
}

func genMultiplePartitionClusteringRangeQuery(
	s *typedef.Schema,
	t *typedef.Table,
	g generators.Interface,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	numQueryPKs, maxClusteringRels int,
) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()

	clusteringKeys := t.ClusteringKeys
	pkValues := t.PartitionKeysLenValues()
	valuesCount := pkValues*numQueryPKs + clusteringKeys[:maxClusteringRels].LenValues() + clusteringKeys[maxClusteringRels].Type.LenValue()*2
	values := make(typedef.Values, pkValues*numQueryPKs, valuesCount)
	typs := make(typedef.Types, pkValues*numQueryPKs, valuesCount)
	builder := qb.Select(s.Keyspace.Name + "." + t.Name)
	tokens := make([]typedef.ValueWithToken, 0, numQueryPKs)

	for _, pk := range t.PartitionKeys {
		builder = builder.Where(qb.InTuple(pk.Name, numQueryPKs))
	}

	for j := 0; j < numQueryPKs; j++ {
		vs := g.GetOld()
		if vs.Token == 0 {
			g.GiveOlds(tokens...)
			return nil
		}
		tokens = append(tokens, vs)
		for id := range vs.Value {
			idx := id*numQueryPKs + j
			typs[idx] = t.PartitionKeys[id].Type
			values[idx] = vs.Value[id]
		}
	}

	if len(clusteringKeys) > 0 {
		for i := 0; i < maxClusteringRels; i++ {
			ck := clusteringKeys[i]
			builder = builder.Where(qb.Eq(ck.Name))
			values = append(values, ck.Type.GenValue(r, p)...)
			typs = append(typs, ck.Type)
		}
		ck := clusteringKeys[maxClusteringRels]
		builder = builder.Where(qb.Gt(ck.Name)).Where(qb.Lt(ck.Name))
		values = append(values, ck.Type.GenValue(r, p)...)
		values = append(values, ck.Type.GenValue(r, p)...)
		typs = append(typs, ck.Type, ck.Type)
	}
	return &typedef.Stmt{
		StmtCache: typedef.StmtCache{
			Query:     builder,
			Types:     typs,
			QueryType: typedef.SelectRangeStatementType,
		},
		Values:          values,
		ValuesWithToken: tokens,
	}
}

func genMultiplePartitionClusteringRangeQueryMv(
	s *typedef.Schema,
	t *typedef.Table,
	g generators.Interface,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	mvNum, numQueryPKs, maxClusteringRels int,
) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()

	mv := t.MaterializedViews[mvNum]
	clusteringKeys := mv.ClusteringKeys
	pkValues := mv.PartitionKeysLenValues()
	valuesCount := pkValues*numQueryPKs + clusteringKeys[:maxClusteringRels].LenValues() + clusteringKeys[maxClusteringRels].Type.LenValue()*2
	mvKey := mv.NonPrimaryKey

	var (
		mvKeyLen int
		baseID   int
	)
	if mvKey != nil {
		mvKeyLen = mvKey.Type.LenValue()
		baseID = 1
		valuesCount += mv.PartitionKeys.LenValues() * numQueryPKs
	}
	values := make(typedef.Values, pkValues*numQueryPKs, valuesCount)
	typs := make(typedef.Types, pkValues*numQueryPKs, valuesCount)
	builder := qb.Select(s.Keyspace.Name + "." + mv.Name)
	tokens := make([]typedef.ValueWithToken, 0, numQueryPKs)

	for _, pk := range mv.PartitionKeys {
		builder = builder.Where(qb.InTuple(pk.Name, numQueryPKs))
	}

	if mvKey != nil {
		// Fill values for Materialized view primary key
		for j := 0; j < numQueryPKs; j++ {
			typs[j] = mvKey.Type
			copy(values[j*mvKeyLen:], mvKey.Type.GenValue(r, p))
		}
	}

	for j := 0; j < numQueryPKs; j++ {
		vs := g.GetOld()
		if vs.Token == 0 {
			g.GiveOlds(tokens...)
			return nil
		}
		tokens = append(tokens, vs)
		for id := range vs.Value {
			idx := (baseID+id)*numQueryPKs + j
			typs[idx] = mv.PartitionKeys[baseID+id].Type
			values[idx] = vs.Value[id]
		}
	}

	if len(clusteringKeys) > 0 {
		for i := 0; i < maxClusteringRels; i++ {
			ck := clusteringKeys[i]
			builder = builder.Where(qb.Eq(ck.Name))
			values = append(values, ck.Type.GenValue(r, p)...)
			typs = append(typs, ck.Type)
		}
		ck := clusteringKeys[maxClusteringRels]
		builder = builder.Where(qb.Gt(ck.Name)).Where(qb.Lt(ck.Name))
		values = append(values, ck.Type.GenValue(r, p)...)
		values = append(values, ck.Type.GenValue(r, p)...)
		typs = append(typs, ck.Type, ck.Type)
	}
	return &typedef.Stmt{
		StmtCache: typedef.StmtCache{
			Query:     builder,
			Types:     typs,
			QueryType: typedef.SelectFromMaterializedViewStatementType,
		},
		Values:          values,
		ValuesWithToken: tokens,
	}
}

func genSingleIndexQuery(
	s *typedef.Schema,
	t *typedef.Table,
	_ generators.Interface,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	idxCount int,
) *typedef.Stmt {
	t.RLock()
	defer t.RUnlock()

	var (
		values []any
		typs   []typedef.Type
	)

	builder := qb.Select(s.Keyspace.Name + "." + t.Name)
	builder.AllowFiltering()
	for i := 0; i < idxCount; i++ {
		builder = builder.Where(qb.Eq(t.Indexes[i].ColumnName))
		values = append(values, t.Indexes[i].Column.Type.GenValue(r, p)...)
		typs = append(typs, t.Indexes[i].Column.Type)
	}

	return &typedef.Stmt{
		StmtCache: typedef.StmtCache{
			Query:     builder,
			Types:     typs,
			QueryType: typedef.SelectByIndexStatementType,
		},
		Values: values,
	}
}
