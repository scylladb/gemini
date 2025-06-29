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

package generators

import (
	"fmt"
	"math/rand/v2"

	"github.com/scylladb/gemini/pkg/typedef"
)

func GenColumnName(prefix string, idx int) string {
	return fmt.Sprintf("%s%d", prefix, idx)
}

func GenColumnType(numColumns int, sc *typedef.SchemaConfig, r *rand.Rand) typedef.Type {
	n := r.IntN(numColumns + 5)
	switch n {
	case numColumns:
		return GenTupleType(sc, r)
	case numColumns + 1:
		return GenUDTType(sc, r)
	case numColumns + 2:
		return GenSetType(sc, r)
	case numColumns + 3:
		return GenListType(sc, r)
	case numColumns + 4:
		return GenMapType(sc, r)
	default:
		return GenSimpleType(sc, r)
	}
}

func GenSimpleType(_ *typedef.SchemaConfig, r *rand.Rand) typedef.SimpleType {
	return typedef.AllTypes[r.IntN(len(typedef.AllTypes))]
}

func GenTupleType(sc *typedef.SchemaConfig, r *rand.Rand) typedef.Type {
	n := max(r.IntN(sc.MaxTupleParts), 2)
	typeList := make([]typedef.SimpleType, n)
	for i := range n {
		typeList[i] = GenSimpleType(sc, r)
	}
	return &typedef.TupleType{
		ComplexType: typedef.TypeTuple,
		ValueTypes:  typeList,
		Frozen:      r.Uint32()%2 == 0,
	}
}

func GenUDTType(sc *typedef.SchemaConfig, r *rand.Rand) *typedef.UDTType {
	udtNum := r.Uint32()
	typeName := fmt.Sprintf("udt_%d", udtNum)
	ts := make(map[string]typedef.SimpleType)

	for i := range r.IntN(sc.MaxUDTParts) + 1 {
		ts[typeName+fmt.Sprintf("_%d", i)] = GenSimpleType(sc, r)
	}

	return &typedef.UDTType{
		ComplexType: typedef.TypeUdt,
		ValueTypes:  ts,
		TypeName:    typeName,
		Frozen:      true,
	}
}

func GenSetType(sc *typedef.SchemaConfig, r *rand.Rand) *typedef.BagType {
	return genBagType(typedef.TypeSet, sc, r)
}

func GenListType(sc *typedef.SchemaConfig, r *rand.Rand) *typedef.BagType {
	return genBagType(typedef.TypeList, sc, r)
}

func genBagType(kind string, sc *typedef.SchemaConfig, r *rand.Rand) *typedef.BagType {
	var t typedef.SimpleType
	for {
		t = GenSimpleType(sc, r)
		if t != typedef.TypeDuration {
			break
		}
	}
	return &typedef.BagType{
		ComplexType: kind,
		ValueType:   t,
		Frozen:      r.Uint32()%2 == 0,
	}
}

func GenMapType(sc *typedef.SchemaConfig, r *rand.Rand) *typedef.MapType {
	t := GenSimpleType(sc, r)
	for {
		if _, ok := typedef.TypesMapKeyBlacklist[t]; !ok {
			break
		}
		t = GenSimpleType(sc, r)
	}
	return &typedef.MapType{
		ComplexType: typedef.TypeMap,
		KeyType:     t,
		ValueType:   GenSimpleType(sc, r),
		Frozen:      r.Uint32()%2 == 0,
	}
}

func GenPartitionKeyColumnType(r *rand.Rand) typedef.Type {
	return typedef.PartitionKeyTypes[r.IntN(len(typedef.PartitionKeyTypes))]
}

func GenPrimaryKeyColumnType(r *rand.Rand) typedef.Type {
	return typedef.PkTypes[r.IntN(len(typedef.PkTypes))]
}

func GenIndexName(prefix string, idx int) string {
	return fmt.Sprintf("%s_idx", GenColumnName(prefix, idx))
}

func CreateIndexesForColumn(table *typedef.Table, maxIndexes int) []typedef.IndexDef {
	createdCount := 0
	indexes := make([]typedef.IndexDef, 0, maxIndexes)
	for i, col := range table.Columns {
		if col.Type.Indexable() && typedef.TypesForIndex.Contains(col.Type) {
			indexes = append(indexes, typedef.IndexDef{
				IndexName:  GenIndexName(table.Name+"_col", i),
				ColumnName: table.Columns[i].Name,
				Column:     table.Columns[i],
			})
			createdCount++
		}
		if createdCount == maxIndexes {
			break
		}
	}
	return indexes
}
