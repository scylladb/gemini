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

	"github.com/scylladb/gocqlx/v3/qb"

	"github.com/scylladb/gemini/pkg/typedef"
)

func (g *Generator) Delete(ctx context.Context) (*typedef.Stmt, error) {
	switch g.random.IntN(DeleteStatements) {
	case DeleteWholePartition:
		return g.deleteSinglePartition(ctx)
	case DeleteSingleRow:
		return g.deleteSinglePartition(ctx)
	case DeleteSingleColumn:
		return g.deleteSinglePartition(ctx)
	case DeleteMultiplePartitions:
		return g.deleteSinglePartition(ctx)
	default:
		panic("unknown delete statement type")
	}

	//  if len(g.table.ClusteringKeys) > 0 {
	//	ck := g.table.ClusteringKeys[0]
	//	builder = builder.Where(qb.GtOrEq(ck.Name)).Where(qb.LtOrEq(ck.Name))
	//	values = append(values, ck.Type.GenValue(g.random, g.partitionConfig)...)
	//	values = append(values, ck.Type.GenValue(g.random, g.partitionConfig)...)
	//  }
	//
}

func (g *Generator) deleteMultiplePartitions(ctx context.Context) (*typedef.Stmt, error) {
	builder := qb.Delete(g.keyspaceAndTable)

	numQueryPKs := g.getMultiplePartitionKeys()
	pks := typedef.NewValues(g.table.PartitionKeys.Len())

	maybeReturn := make([]typedef.PartitionKeys, 0, numQueryPKs)

	for range numQueryPKs {
		pk, err := g.generator.GetOld(ctx)
		if err != nil {
			if len(maybeReturn) > 0 {
				g.generator.GiveOlds(ctx, maybeReturn...)
			}
			return nil, err
		}

		pks.Merge(pk.Values)
		maybeReturn = append(maybeReturn, pk)
	}

	for _, pk := range g.table.PartitionKeys {
		builder.Where(qb.InTuple(pk.Name, numQueryPKs))
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		PartitionKeys: typedef.PartitionKeys{Values: pks},
		Values:        pks.ToCQLValues(g.table.PartitionKeys),
		QueryType:     typedef.DeleteMultiplePartitionsType,
		Query:         query,
	}, nil
}

func (g *Generator) deleteSinglePartition(ctx context.Context) (*typedef.Stmt, error) {
	pks, err := g.generator.GetOld(ctx)
	if err != nil {
		return nil, err
	}

	builder := qb.Delete(g.keyspaceAndTable)
	values := make([]any, 0, g.table.PartitionKeys.LenValues())

	for _, pk := range g.table.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		values = append(values, pks.Values.Get(pk.Name)...)
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		PartitionKeys: pks,
		Values:        values,
		QueryType:     typedef.DeleteWholePartitionType,
		Query:         query,
	}, nil
}
