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
	"maps"
	"slices"
	"sync/atomic"

	"github.com/gocql/gocql"

	"github.com/scylladb/gemini/pkg/utils"
)

type UDTType struct {
	fields      atomic.Pointer[udtFields]
	ComplexType string                `json:"complex_type"`
	ValueTypes  map[string]SimpleType `json:"value_types"`
	TypeName    string                `json:"type_name"`
	Frozen      bool                  `json:"frozen"`
}

// udtFields holds the UDT fields in sorted name order. types[i] is the type
// of names[i].
type udtFields struct {
	names []string
	types []SimpleType
}

// sortedFields returns the fields in sorted name order. Value generation and
// the CREATE TYPE statement use this order, so one seed always gives the same
// values and the same DDL. The fields are sorted on the first call and shared
// after that, also for a type read from a JSON schema.
func (t *UDTType) sortedFields() *udtFields {
	if f := t.fields.Load(); f != nil {
		return f
	}

	names := slices.Sorted(maps.Keys(t.ValueTypes))
	types := make([]SimpleType, len(names))
	for i, name := range names {
		types[i] = t.ValueTypes[name]
	}

	t.fields.CompareAndSwap(nil, &udtFields{names: names, types: types})

	return t.fields.Load()
}

// FieldNames returns the field names in sorted order. The slice is shared, so
// the caller must not change it.
func (t *UDTType) FieldNames() []string {
	return t.sortedFields().names
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

func (t *UDTType) Indexable() bool {
	for _, ty := range t.ValueTypes {
		if ty == TypeDuration {
			return false
		}
	}

	return true
}

func (t *UDTType) GenJSONValue(r utils.Random, p RangeConfig) any {
	f := t.sortedFields()
	vals := make(map[string]any, len(f.names))
	for i, name := range f.names {
		vals[name] = f.types[i].GenJSONValue(r, p)
	}
	return vals
}

func (t *UDTType) genValue(r utils.Random, p RangeConfig) map[string]any {
	f := t.sortedFields()
	vals := make(map[string]any, len(f.names))
	for i, name := range f.names {
		vals[name] = f.types[i].genValue(r, p)
	}
	return vals
}

func (t *UDTType) GenValue(r utils.Random, p RangeConfig) []any {
	return []any{t.genValue(r, p)}
}

func (t *UDTType) GenValueOut(out []any, r utils.Random, p RangeConfig) []any {
	return append(out, t.genValue(r, p))
}

func (t *UDTType) LenValue() int {
	return 1
}

// ValueVariationsNumber returns number of bytes generated value holds
func (t *UDTType) ValueVariationsNumber(p RangeConfig) float64 {
	out := float64(1)
	for _, tp := range t.sortedFields().types {
		out *= tp.ValueVariationsNumber(p)
	}
	return out
}
