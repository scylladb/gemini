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
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v3/qb"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

func (g *Generator) Insert(_ context.Context) (*typedef.Stmt, error) {
	builder := qb.Insert(g.keyspaceAndTable)
	if g.useLWT && g.random.Uint32()%10 == 0 {
		builder.Unique()
	}

	values := make([]any, 0, g.table.PartitionKeys.LenValues()+g.table.ClusteringKeys.LenValues()+g.table.Columns.LenValues())

	pks := g.generator.Next()

	for _, pk := range g.table.PartitionKeys {
		builder.Columns(pk.Name)
		values = append(values, pks.Values.Get(pk.Name)...)
	}

	for _, ck := range g.table.ClusteringKeys {
		builder.Columns(ck.Name)
		values = append(values, ck.Type.GenValue(g.random, g.valueRangeConfig)...)
	}

	for _, col := range g.table.Columns {
		switch colType := col.Type.(type) {
		case *typedef.TupleType:
			builder.TupleColumn(col.Name, len(colType.ValueTypes))
			values = append(values, col.Type.GenValue(g.random, g.valueRangeConfig)...)
		default:
			builder.Columns(col.Name)
			values = append(values, col.Type.GenValue(g.random, g.valueRangeConfig)...)
		}
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{pks},
		Values:        values,
		QueryType:     typedef.InsertStatementType,
		Query:         query,
	}, nil
}

func (g *Generator) InsertJSON(_ context.Context) (*typedef.Stmt, error) {
	if g.table.IsCounterTable() {
		return nil, ErrCounterTableJSON
	}

	pks := g.generator.Next()
	values := make(map[string]any, g.table.PartitionKeys.LenValues()+g.table.ClusteringKeys.LenValues()+g.table.Columns.LenValues())

	for _, pk := range g.table.PartitionKeys {
		switch t := pk.Type.(type) {
		case typedef.SimpleType:
			values[pk.Name] = convertForJSON(t, pks.Values.Get(pk.Name)[0])
		case *typedef.TupleType:
			elems := pks.Values.Get(pk.Name)
			if len(elems) != len(t.ValueTypes) {
				return nil, fmt.Errorf(
					"partition key %q carries %d values, want %d for %s",
					pk.Name, len(elems), len(t.ValueTypes), t.CQLDef(),
				)
			}

			tupVals := make([]any, len(t.ValueTypes))
			for i, elemType := range t.ValueTypes {
				tupVals[i] = convertForJSON(elemType, elems[i])
			}

			values[pk.Name] = tupVals
		default:
			panic("unknown type: " + t.Name())
		}
	}

	values = g.table.ClusteringKeys.ToJSONMap(values, g.random, g.valueRangeConfig)
	values = g.table.Columns.ToJSONMap(values, g.random, g.valueRangeConfig)

	jsonString, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}

	query, _ := qb.Insert(g.keyspaceAndTable).Json().ToCql()
	return &typedef.Stmt{
		PartitionKeys: []typedef.PartitionKeys{pks},
		Query:         query,
		QueryType:     typedef.InsertJSONStatementType,
		Values:        []any{utils.UnsafeString(jsonString)},
	}, nil
}

func convertForJSON(vType typedef.Type, value any) any {
	switch vType {
	case typedef.TypeBlob:
		val := mustConvert[[]byte](vType, value)
		buffer := bytes.NewBuffer(nil)
		buffer.Grow(len(val)*2 + 2) // 2 for "0x" prefix
		buffer.WriteString("0x")
		encoder := hex.NewEncoder(buffer)
		_, _ = encoder.Write(val)
		return utils.UnsafeString(buffer.Bytes())
	case typedef.TypeDate:
		return mustConvert[time.Time](vType, value).Format(time.DateOnly)
	case typedef.TypeDuration:
		return utils.TimeDurationToScyllaDuration(mustConvert[time.Duration](vType, value))
	case typedef.TypeDecimal:
		return mustConvert[*inf.Dec](vType, value).String()
	case typedef.TypeUUID, typedef.TypeTimeuuid:
		return mustConvert[gocql.UUID](vType, value).String()
	case typedef.TypeVarint:
		return mustConvert[*big.Int](vType, value).String()
	case typedef.TypeTime:
		val := mustConvert[time.Duration](vType, value)
		return time.Unix(0, int64(val)).UTC().Format("15:04:05.000000000")
	}

	return value
}

func mustConvert[T any](vType typedef.Type, value any) T {
	val, ok := value.(T)
	if !ok {
		var want T
		panic(fmt.Sprintf("gemini generated %T for column type %s, want %T", value, vType.CQLDef(), want))
	}

	return val
}
