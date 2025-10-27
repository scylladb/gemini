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
	"math"

	"github.com/gocql/gocql"

	"github.com/scylladb/gemini/pkg/utils"
)

type Collection struct {
	ComplexType string     `json:"complex_type"` // We need to differentiate between sets and lists
	ValueType   SimpleType `json:"value_type"`
	Frozen      bool       `json:"frozen"`
}

func (ct *Collection) CQLType() gocql.TypeInfo {
	switch ct.ComplexType {
	case TypeSet:
		return goCQLTypeMap[gocql.TypeSet]
	default:
		return goCQLTypeMap[gocql.TypeList]
	}
}

func (ct *Collection) Name() string {
	if ct.Frozen {
		return "frozen<" + ct.ComplexType + "<" + ct.ValueType.Name() + ">>"
	}
	return ct.ComplexType + "<" + ct.ValueType.Name() + ">"
}

func (ct *Collection) CQLDef() string {
	if ct.Frozen {
		return "frozen<" + ct.ComplexType + "<" + ct.ValueType.Name() + ">>"
	}
	return ct.ComplexType + "<" + ct.ValueType.Name() + ">"
}

func (ct *Collection) CQLHolder() string {
	return "?"
}

func (ct *Collection) GenValue(r utils.Random, p *PartitionRangeConfig) []any {
	count := utils.RandInt2(r, 1, maxBagSize+1)
	out := make([]any, count)
	for i := 0; i < count; i++ {
		out[i] = ct.ValueType.GenValue(r, p)[0]
	}
	return []any{out}
}

func (ct *Collection) GenValueOut(out []any, r utils.Random, p *PartitionRangeConfig) []any {
	count := utils.RandInt2(r, 1, maxBagSize+1)
	for range count {
		out = ct.ValueType.GenValueOut(out, r, p)
	}
	return out
}

func (ct *Collection) GenJSONValue(r utils.Random, p *PartitionRangeConfig) any {
	count := utils.RandInt2(r, 1, maxBagSize+1)
	out := make([]any, count)
	for i := 0; i < count; i++ {
		out[i] = ct.ValueType.GenJSONValue(r, p)
	}
	return out
}

func (ct *Collection) LenValue() int {
	return 1
}

func (ct *Collection) Indexable() bool {
	return false
}

// ValueVariationsNumber returns number of bytes generated value holds
func (ct *Collection) ValueVariationsNumber(p *PartitionRangeConfig) float64 {
	return math.Pow(ct.ValueType.ValueVariationsNumber(p), maxBagSize)
}
