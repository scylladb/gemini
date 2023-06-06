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

	"github.com/scylladb/gemini/pkg/coltypes"
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

func GenSimpleType(_ *typedef.SchemaConfig) coltypes.SimpleType {
	return coltypes.AllTypes[rand.Intn(len(coltypes.AllTypes))]
}

func GenTupleType(sc *typedef.SchemaConfig) typedef.Type {
	n := rand.Intn(sc.MaxTupleParts)
	if n < 2 {
		n = 2
	}
	typeList := make([]coltypes.SimpleType, n)
	for i := 0; i < n; i++ {
		typeList[i] = GenSimpleType(sc)
	}
	return &coltypes.TupleType{
		Types:  typeList,
		Frozen: rand.Uint32()%2 == 0,
	}
}

func GenUDTType(sc *typedef.SchemaConfig) *coltypes.UDTType {
	udtNum := rand.Uint32()
	typeName := fmt.Sprintf("udt_%d", udtNum)
	ts := make(map[string]coltypes.SimpleType)

	for i := 0; i < rand.Intn(sc.MaxUDTParts)+1; i++ {
		ts[typeName+fmt.Sprintf("_%d", i)] = GenSimpleType(sc)
	}

	return &coltypes.UDTType{
		Types:    ts,
		TypeName: typeName,
		Frozen:   true,
	}
}

func GenSetType(sc *typedef.SchemaConfig) *coltypes.BagType {
	return genBagType("set", sc)
}

func GenListType(sc *typedef.SchemaConfig) *coltypes.BagType {
	return genBagType("list", sc)
}

func genBagType(kind string, sc *typedef.SchemaConfig) *coltypes.BagType {
	var t coltypes.SimpleType
	for {
		t = GenSimpleType(sc)
		if t != coltypes.TYPE_DURATION {
			break
		}
	}
	return &coltypes.BagType{
		Kind:   kind,
		Type:   t,
		Frozen: rand.Uint32()%2 == 0,
	}
}

func GenMapType(sc *typedef.SchemaConfig) *coltypes.MapType {
	t := GenSimpleType(sc)
	for {
		if _, ok := coltypes.TypesMapKeyBlacklist[t]; !ok {
			break
		}
		t = GenSimpleType(sc)
	}
	return &coltypes.MapType{
		KeyType:   t,
		ValueType: GenSimpleType(sc),
		Frozen:    rand.Uint32()%2 == 0,
	}
}

func GenPartitionKeyColumnType() typedef.Type {
	return coltypes.PartitionKeyTypes[rand.Intn(len(coltypes.PartitionKeyTypes))]
}

func GenPrimaryKeyColumnType() typedef.Type {
	return coltypes.PkTypes[rand.Intn(len(coltypes.PkTypes))]
}

func GenIndexName(prefix string, idx int) string {
	return fmt.Sprintf("%s_idx", GenColumnName(prefix, idx))
}
