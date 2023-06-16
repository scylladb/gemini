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
	"strings"

	"github.com/gocql/gocql"
	"golang.org/x/exp/rand"
)

type TupleType struct {
	ComplexType string       `json:"complex_type"`
	ValueTypes  []SimpleType `json:"value_types"`
	Frozen      bool         `json:"frozen"`
}

func (t *TupleType) CQLType() gocql.TypeInfo {
	return goCQLTypeMap[gocql.TypeTuple]
}

func (t *TupleType) Name() string {
	names := make([]string, len(t.ValueTypes))
	for i, tp := range t.ValueTypes {
		names[i] = tp.Name()
	}
	return "Type: " + strings.Join(names, ",")
}

func (t *TupleType) CQLDef() string {
	names := make([]string, len(t.ValueTypes))
	for i, tp := range t.ValueTypes {
		names[i] = tp.CQLDef()
	}
	if t.Frozen {
		return "frozen<tuple<" + strings.Join(names, ",") + ">>"
	}
	return "tuple<" + strings.Join(names, ",") + ">"
}

func (t *TupleType) CQLHolder() string {
	return "(" + strings.TrimRight(strings.Repeat("?,", len(t.ValueTypes)), ",") + ")"
}

func (t *TupleType) CQLPretty(query string, value []interface{}) (string, int) {
	if len(value) == 0 {
		return query, 0
	}
	var cnt, tmp int
	for i, tp := range t.ValueTypes {
		query, tmp = tp.CQLPretty(query, value[i:])
		cnt += tmp
	}
	return query, cnt
}

func (t *TupleType) Indexable() bool {
	for _, t := range t.ValueTypes {
		if t == TYPE_DURATION {
			return false
		}
	}
	return true
}

func (t *TupleType) GenJSONValue(r *rand.Rand, p *PartitionRangeConfig) interface{} {
	out := make([]interface{}, 0, len(t.ValueTypes))
	for _, tp := range t.ValueTypes {
		out = append(out, tp.GenJSONValue(r, p))
	}
	return out
}

func (t *TupleType) GenValue(r *rand.Rand, p *PartitionRangeConfig) []interface{} {
	out := make([]interface{}, 0, len(t.ValueTypes))
	for _, tp := range t.ValueTypes {
		out = append(out, tp.GenValue(r, p)...)
	}
	return out
}

func (t *TupleType) LenValue() int {
	out := 0
	for _, tp := range t.ValueTypes {
		out += tp.LenValue()
	}
	return out
}
