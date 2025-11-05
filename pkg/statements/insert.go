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
		values = append(values, pks.Get(pk.Name)...)
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
		PartitionKeys: typedef.PartitionKeys{Values: pks},
		Values:        values,
		QueryType:     typedef.InsertStatementType,
		Query:         query,
	}, nil
}

func (g *Generator) InsertJSON(_ context.Context) (*typedef.Stmt, error) {
	if g.table.IsCounterTable() {
		return nil, nil
	}

	pks := g.generator.Next()
	values := make(map[string]any, g.table.PartitionKeys.LenValues()+g.table.ClusteringKeys.LenValues()+g.table.Columns.LenValues())

	for _, pk := range g.table.PartitionKeys {
		switch t := pk.Type.(type) {
		case typedef.SimpleType:
			values[pk.Name] = convertForJSON(t, pks.Get(pk.Name)[0])
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

	values = g.table.ClusteringKeys.ToJSONMap(values, g.random, g.valueRangeConfig)
	values = g.table.Columns.ToJSONMap(values, g.random, g.valueRangeConfig)

	jsonString, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}

	query, _ := qb.Insert(g.keyspaceAndTable).Json().ToCql()
	return &typedef.Stmt{
		PartitionKeys: typedef.PartitionKeys{Values: pks},
		Query:         query,
		QueryType:     typedef.InsertJSONStatementType,
		Values:        []any{utils.UnsafeString(jsonString)},
	}, nil
}

func convertForJSON(vType typedef.Type, value any) any {
	switch vType {
	case typedef.TypeBlob:
		val, _ := value.([]byte)
		buffer := bytes.NewBuffer(nil)
		buffer.Grow(len(val)*2 + 2) // 2 for "0x" prefix
		buffer.WriteString("0x")
		encoder := hex.NewEncoder(buffer)
		_, _ = encoder.Write(val)
		return utils.UnsafeString(buffer.Bytes())
	case typedef.TypeDate:
		return value.(time.Time).Format(time.DateOnly)
	case typedef.TypeDuration:
		return utils.TimeDurationToScyllaDuration(value.(time.Duration))
	case typedef.TypeDecimal:
		return value.(*inf.Dec).String()
	case typedef.TypeUuid, typedef.TypeTimeuuid:
		return value.(gocql.UUID).String()
	case typedef.TypeVarint:
		return value.(*big.Int).String()
	case typedef.TypeTime:
		val, _ := value.(int64)
		return time.Unix(0, val).UTC().Format("15:04:05.000000000")
	}

	return value
}
