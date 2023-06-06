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

package coltypes

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/gocql/gocql"
	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/typedef"
)

type BagType struct {
	Kind   string     `json:"kind"` // We need to differentiate between sets and lists
	Type   SimpleType `json:"type"`
	Frozen bool       `json:"frozen"`
}

func (ct *BagType) CQLType() gocql.TypeInfo {
	switch ct.Kind {
	case "set":
		return goCQLTypeMap[gocql.TypeSet]
	default:
		return goCQLTypeMap[gocql.TypeList]
	}
}

func (ct *BagType) Name() string {
	if ct.Frozen {
		return "frozen<" + ct.Kind + "<" + ct.Type.Name() + ">>"
	}
	return ct.Kind + "<" + ct.Type.Name() + ">"
}

func (ct *BagType) CQLDef() string {
	if ct.Frozen {
		return "frozen<" + ct.Kind + "<" + ct.Type.Name() + ">>"
	}
	return ct.Kind + "<" + ct.Type.Name() + ">"
}

func (ct *BagType) CQLHolder() string {
	return "?"
}

func (ct *BagType) CQLPretty(query string, value []interface{}) (string, int) {
	if len(value) == 0 {
		return query, 0
	}
	if reflect.TypeOf(value[0]).Kind() != reflect.Slice {
		panic(fmt.Sprintf("set cql pretty, unknown type %v", ct))
	}
	s := reflect.ValueOf(value[0])
	vv := "{"
	vv += strings.Repeat("?,", s.Len())
	vv = strings.TrimRight(vv, ",")
	vv += "}"
	for i := 0; i < s.Len(); i++ {
		vv, _ = ct.Type.CQLPretty(vv, []interface{}{s.Index(i).Interface()})
	}
	return strings.Replace(query, "?", vv, 1), 1
}

func (ct *BagType) GenValue(r *rand.Rand, p *typedef.PartitionRangeConfig) []interface{} {
	count := r.Intn(9) + 1
	out := make([]interface{}, count)
	for i := 0; i < count; i++ {
		out[i] = ct.Type.GenValue(r, p)[0]
	}
	return []interface{}{out}
}

func (ct *BagType) LenValue() int {
	return 1
}

func (ct *BagType) Indexable() bool {
	return false
}
