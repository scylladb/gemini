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
	"encoding/json"
	"fmt"
	"time"

	"github.com/scylladb/gocqlx/v3/qb"

	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

func (g *Generator) MutateStatement(ctx context.Context, generateDelete bool) (*typedef.Stmt, error) {
	g.table.RLock()
	defer g.table.RUnlock()

	if !generateDelete {
		if g.table.IsCounterTable() {
			return g.Update(ctx)
		}

		return g.Insert(ctx)
	}

	if n := g.random.IntN(1000); n == 10 || n == 100 {
		return g.Delete(ctx)
	}

	switch g.random.IntN(MutationStatements) {

	case InsertStatements:
		if g.table.IsCounterTable() {
			return g.Update(ctx)
		}

		return g.Insert(ctx)
	case InsertJSONStatement:
		if g.table.IsCounterTable() {
			return g.Update(ctx)
		}

		if g.table.KnownIssues[typedef.KnownIssuesJSONWithTuples] {
			return g.Insert(ctx)
		}

		return g.InsertJSON(ctx)
	case UpdateStatement:
		// TODO(CodeLieutenant): Update statement support expected in v2.1.0
		//       Falling back to Insert for now, until everything else is stable

		if g.table.IsCounterTable() {
			return g.Update(ctx)
		}
		return g.Insert(ctx)
	default:
		panic("Invalid mutation statement type")
	}
}

func (g *Generator) Insert(ctx context.Context) (*typedef.Stmt, error) {
	builder := qb.Insert(g.keyspaceAndTable)
	if g.useLWT && g.random.Uint32()%10 == 0 {
		builder.Unique()
	}

	values := make([]any, 0, g.table.PartitionKeys.LenValues()+g.table.ClusteringKeys.LenValues()+g.table.Columns.LenValues())

	pks, err := g.generator.Get(ctx)
	if err != nil {
		return nil, err
	}

	for _, pk := range g.table.PartitionKeys {
		builder.Columns(pk.Name)
		values = append(values, pks.Values.Get(pk.Name)...)
	}

	for _, ck := range g.table.ClusteringKeys {
		builder.Columns(ck.Name)
		values = append(values, ck.Type.GenValue(g.random, g.partitionConfig)...)
	}

	for _, col := range g.table.Columns {
		switch colType := col.Type.(type) {
		case *typedef.TupleType:
			builder.TupleColumn(col.Name, len(colType.ValueTypes))
			values = append(values, col.Type.GenValue(g.random, g.partitionConfig)...)
		default:
			builder.Columns(col.Name)
			values = append(values, col.Type.GenValue(g.random, g.partitionConfig)...)
		}
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		PartitionKeys: pks,
		Values:        values,
		QueryType:     typedef.InsertStatementType,
		Query:         query,
	}, nil
}

func (g *Generator) InsertJSON(ctx context.Context) (*typedef.Stmt, error) {
	if g.table.IsCounterTable() {
		return nil, nil
	}

	pks, err := g.generator.Get(ctx)
	if err != nil {
		return nil, err
	}

	values := make(map[string]any, g.table.PartitionKeys.LenValues()+g.table.ClusteringKeys.LenValues()+g.table.Columns.LenValues())

	for _, pk := range g.table.PartitionKeys {
		switch t := pk.Type.(type) {
		case typedef.SimpleType:
			values[pk.Name] = convertForJSON(t, pks.Values.Get(pk.Name))
		case *typedef.TupleType:
			tupVals := make([]any, 0, len(t.ValueTypes))
			for _, value := range t.ValueTypes {
				tupVals = append(tupVals, convertForJSON(t, value))
			}
			values[pk.Name] = tupVals
		default:
			panic(fmt.Sprintf("unknown type: %s", t.Name()))
		}
	}

	values = g.table.ClusteringKeys.ToJSONMap(values, g.random, g.partitionConfig)
	values = g.table.Columns.ToJSONMap(values, g.random, g.partitionConfig)

	jsonString, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}

	query, _ := qb.Insert(g.keyspaceAndTable).Json().ToCql()
	return &typedef.Stmt{
		PartitionKeys: pks,
		Query:         query,
		Values:        []any{utils.UnsafeString(jsonString)},
	}, nil
}

func convertForJSON(vType typedef.Type, value any) any {
	switch vType {
	case typedef.TypeBlob:
		val, _ := value.(string)
		return "0x" + val
	case typedef.TypeTime:
		val, _ := value.(int64)
		return time.Unix(0, val).UTC().Format("15:04:05.000000000")
	}
	return value
}
