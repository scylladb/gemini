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
	switch g.ratioController.GetDeleteSubtype() {
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

// nolint:unused
func (g *Generator) deleteMultiplePartitions(_ context.Context) (*typedef.Stmt, error) {
	builder := qb.Delete(g.keyspaceAndTable)

	numQueryPKs := g.getMultiplePartitionKeys()
	pks := make([]typedef.PartitionKeys, 0, numQueryPKs)
	combined := typedef.NewValues(g.table.PartitionKeys.Len())

	for range numQueryPKs {
		pk := g.generator.ReplaceNext()
		pks = append(pks, pk)
		combined.Merge(pk.Values)
	}

	for _, pk := range g.table.PartitionKeys {
		builder.Where(qb.InTuple(pk.Name, numQueryPKs))
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		PartitionKeys: pks,
		Values:        combined.ToCQLValues(g.table.PartitionKeys),
		QueryType:     typedef.DeleteMultiplePartitionsType,
		Query:         query,
	}, nil
}

func (g *Generator) deleteSinglePartition(_ context.Context) (*typedef.Stmt, error) {
	pks := g.generator.ReplaceNext()

	return &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{pks},
		Values:        pks.Values.ToCQLValues(g.table.PartitionKeys),
		QueryType:     typedef.DeleteWholePartitionType,
		Query:         g.cachedDeleteQuery,
	}, nil
}
