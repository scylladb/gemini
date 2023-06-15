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
	"github.com/gocql/gocql"
	"golang.org/x/exp/rand"
)

type Type interface {
	Name() string
	CQLDef() string
	CQLHolder() string
	CQLPretty(string, []interface{}) (string, int)
	GenValue(*rand.Rand, *PartitionRangeConfig) []interface{}
	GenJSONValue(*rand.Rand, *PartitionRangeConfig) interface{}
	LenValue() int
	Indexable() bool
	CQLType() gocql.TypeInfo
}

type Types []Type

func (l Types) LenValue() int {
	out := 0
	for _, t := range l {
		out += t.LenValue()
	}
	return out
}
