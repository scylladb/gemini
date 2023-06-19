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

package typedef

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"golang.org/x/exp/rand"
)

type UDTType struct {
	ComplexType string                `json:"complex_type"`
	ValueTypes  map[string]SimpleType `json:"value_types"`
	TypeName    string                `json:"type_name"`
	Frozen      bool                  `json:"frozen"`
}

func (t *UDTType) CQLType() gocql.TypeInfo {
	return goCQLTypeMap[gocql.TypeUDT]
}

func (t *UDTType) Name() string {
	return t.TypeName
}

func (t *UDTType) CQLDef() string {
	if t.Frozen {
		return "frozen<" + t.TypeName + ">"
	}
	return t.TypeName
}

func (t *UDTType) CQLHolder() string {
	return "?"
}

func (t *UDTType) CQLPretty(query string, value []interface{}) (string, int) {
	if len(value) == 0 {
		return query, 0
	}
	if s, ok := value[0].(map[string]interface{}); ok {
		vv := "{"
		for k, v := range t.ValueTypes {
			vv += fmt.Sprintf("%s:?,", k)
			vv, _ = v.CQLPretty(vv, []interface{}{s[k]})
		}
		vv = strings.TrimSuffix(vv, ",")
		vv += "}"
		return strings.Replace(query, "?", vv, 1), 1
	}
	panic(fmt.Sprintf("udt pretty, unknown type %v", t))
}

func (t *UDTType) Indexable() bool {
	for _, t := range t.ValueTypes {
		if t == TYPE_DURATION {
			return false
		}
	}
	return true
}

func (t *UDTType) GenJSONValue(r *rand.Rand, p *PartitionRangeConfig) interface{} {
	vals := make(map[string]interface{})
	for name, typ := range t.ValueTypes {
		vals[name] = typ.GenJSONValue(r, p)
	}
	return vals
}

func (t *UDTType) GenValue(r *rand.Rand, p *PartitionRangeConfig) []interface{} {
	vals := make(map[string]interface{})
	for name, typ := range t.ValueTypes {
		vals[name] = typ.GenValue(r, p)[0]
	}
	return []interface{}{vals}
}

func (t *UDTType) LenValue() int {
	return 1
}
