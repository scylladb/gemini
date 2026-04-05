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

// Update generates a counter-table UPDATE statement. Only used when
// IsCounterTable() is true — regular updates fall through to Insert().
func (g *Generator) Update(_ context.Context) (*typedef.Stmt, error) {
	builder := qb.Update(g.keyspaceAndTable)
	values := make([]any, 0, g.table.PartitionKeys.LenValues()+g.table.ClusteringKeys.LenValues())

	for _, col := range g.table.Columns {
		switch col.Type.(type) {
		case *typedef.CounterType:
			builder.SetLit(col.Name, col.Name+"+1")
		default:
			// non-counter columns in counter tables are part of the key
		}
	}

	pks := g.generator.Next()
	for _, pk := range g.table.PartitionKeys {
		builder.Where(qb.Eq(pk.Name))
		values = append(values, pks.Values.Get(pk.Name)...)
	}

	for _, ck := range g.table.ClusteringKeys {
		builder.Where(qb.Eq(ck.Name))
		values = ck.Type.GenValueOut(values, g.random, g.valueRangeConfig)
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{pks},
		Values:        values,
		QueryType:     typedef.UpdateStatementType,
		Query:         query,
	}, nil
}
