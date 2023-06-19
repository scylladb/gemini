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

	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/typedef"
)

func GenColumnName(prefix string, idx int) string {
	return fmt.Sprintf("%s%d", prefix, idx)
}

func GenColumnType(numColumns int, sc *typedef.SchemaConfig) typedef.Type {
	n := rand.Intn(numColumns + 5)
	switch n {
	case numColumns:
		return GenTupleType(sc)
	case numColumns + 1:
		return GenUDTType(sc)
	case numColumns + 2:
		return GenSetType(sc)
	case numColumns + 3:
		return GenListType(sc)
	case numColumns + 4:
		return GenMapType(sc)
	default:
		return GenSimpleType(sc)
	}
}

func GenSimpleType(_ *typedef.SchemaConfig) typedef.SimpleType {
	return typedef.AllTypes[rand.Intn(len(typedef.AllTypes))]
}

func GenTupleType(sc *typedef.SchemaConfig) typedef.Type {
	n := rand.Intn(sc.MaxTupleParts)
	if n < 2 {
		n = 2
	}
	typeList := make([]typedef.SimpleType, n)
	for i := 0; i < n; i++ {
		typeList[i] = GenSimpleType(sc)
	}
	return &typedef.TupleType{
		ComplexType: typedef.TYPE_TUPLE,
		ValueTypes:  typeList,
		Frozen:      rand.Uint32()%2 == 0,
	}
}

func GenUDTType(sc *typedef.SchemaConfig) *typedef.UDTType {
	udtNum := rand.Uint32()
	typeName := fmt.Sprintf("udt_%d", udtNum)
	ts := make(map[string]typedef.SimpleType)

	for i := 0; i < rand.Intn(sc.MaxUDTParts)+1; i++ {
		ts[typeName+fmt.Sprintf("_%d", i)] = GenSimpleType(sc)
	}

	return &typedef.UDTType{
		ComplexType: typedef.TYPE_UDT,
		ValueTypes:  ts,
		TypeName:    typeName,
		Frozen:      true,
	}
}

func GenSetType(sc *typedef.SchemaConfig) *typedef.BagType {
	return genBagType(typedef.TYPE_SET, sc)
}

func GenListType(sc *typedef.SchemaConfig) *typedef.BagType {
	return genBagType(typedef.TYPE_LIST, sc)
}

func genBagType(kind string, sc *typedef.SchemaConfig) *typedef.BagType {
	var t typedef.SimpleType
	for {
		t = GenSimpleType(sc)
		if t != typedef.TYPE_DURATION {
			break
		}
	}
	return &typedef.BagType{
		ComplexType: kind,
		ValueType:   t,
		Frozen:      rand.Uint32()%2 == 0,
	}
}

func GenMapType(sc *typedef.SchemaConfig) *typedef.MapType {
	t := GenSimpleType(sc)
	for {
		if _, ok := typedef.TypesMapKeyBlacklist[t]; !ok {
			break
		}
		t = GenSimpleType(sc)
	}
	return &typedef.MapType{
		ComplexType: typedef.TYPE_MAP,
		KeyType:     t,
		ValueType:   GenSimpleType(sc),
		Frozen:      rand.Uint32()%2 == 0,
	}
}

func GenPartitionKeyColumnType() typedef.Type {
	return typedef.PartitionKeyTypes[rand.Intn(len(typedef.PartitionKeyTypes))]
}

func GenPrimaryKeyColumnType() typedef.Type {
	return typedef.PkTypes[rand.Intn(len(typedef.PkTypes))]
}

func GenIndexName(prefix string, idx int) string {
	return fmt.Sprintf("%s_idx", GenColumnName(prefix, idx))
}
