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

func (g *Generator) Delete(ctx context.Context) *typedef.Stmt {
	pks := g.generator.GetOld(ctx)
	if pks.Token == 0 {
		return nil
	}

	builder := qb.Delete(g.keyspaceAndTable)
	values := make([]any, 0, g.table.PartitionKeys.LenValues()+2)

	for _, pk := range g.table.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		values = append(values, pks.Values[pk.Name]...)
	}

	if len(g.table.ClusteringKeys) > 0 {
		ck := g.table.ClusteringKeys[0]
		builder = builder.Where(qb.GtOrEq(ck.Name)).Where(qb.LtOrEq(ck.Name))
		values = append(values, ck.Type.GenValue(g.random, g.partitionConfig)...)
		values = append(values, ck.Type.GenValue(g.random, g.partitionConfig)...)
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		PartitionKeys: pks,
		Values:        values,
		QueryType:     typedef.DeleteStatementType,
		Query:         query,
	}
}
