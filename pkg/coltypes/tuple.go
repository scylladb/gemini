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
	"strings"

	"github.com/scylladb/gemini/pkg/typedef"

	"github.com/gocql/gocql"
	"golang.org/x/exp/rand"
)

type TupleType struct {
	Types  []SimpleType `json:"coltypes"`
	Frozen bool         `json:"frozen"`
}

func (t *TupleType) CQLType() gocql.TypeInfo {
	return goCQLTypeMap[gocql.TypeTuple]
}

func (t *TupleType) Name() string {
	names := make([]string, len(t.Types))
	for i, tp := range t.Types {
		names[i] = tp.Name()
	}
	return "Type: " + strings.Join(names, ",")
}

func (t *TupleType) CQLDef() string {
	names := make([]string, len(t.Types))
	for i, tp := range t.Types {
		names[i] = tp.CQLDef()
	}
	if t.Frozen {
		return "frozen<tuple<" + strings.Join(names, ",") + ">>"
	}
	return "tuple<" + strings.Join(names, ",") + ">"
}

func (t *TupleType) CQLHolder() string {
	return "(" + strings.TrimRight(strings.Repeat("?,", len(t.Types)), ",") + ")"
}

func (t *TupleType) CQLPretty(query string, value []interface{}) (string, int) {
	if len(value) == 0 {
		return query, 0
	}
	var cnt, tmp int
	for i, tp := range t.Types {
		query, tmp = tp.CQLPretty(query, value[i:])
		cnt += tmp
	}
	return query, cnt
}

func (t *TupleType) Indexable() bool {
	for _, t := range t.Types {
		if t == TYPE_DURATION {
			return false
		}
	}
	return true
}

func (t *TupleType) GenValue(r *rand.Rand, p *typedef.PartitionRangeConfig) []interface{} {
	out := make([]interface{}, 0, len(t.Types))
	for _, tp := range t.Types {
		out = append(out, tp.GenValue(r, p)...)
	}
	return out
}

func (t *TupleType) LenValue() int {
	out := 0
	for _, tp := range t.Types {
		out += tp.LenValue()
	}
	return out
}
