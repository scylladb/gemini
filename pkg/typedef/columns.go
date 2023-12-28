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
	"encoding/json"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	"golang.org/x/exp/rand"
)

type ColumnDef struct {
	Type Type   `json:"type"`
	Name string `json:"name"`
}

var ErrSchemaValidation = errors.New("validation failed")

func (cd *ColumnDef) IsValidForPrimaryKey() bool {
	for _, pkType := range PkTypes {
		if cd.Type.Name() == pkType.Name() {
			return true
		}
	}
	return false
}

func (cd *ColumnDef) UnmarshalJSON(data []byte) error {
	dataMap := make(map[string]interface{})
	if err := json.Unmarshal(data, &dataMap); err != nil {
		return err
	}
	t, err := GetSimpleTypeColumn(dataMap)
	if err != nil {
		typeMap, typeOk := dataMap["type"]
		if !typeOk {
			return errors.Wrapf(ErrSchemaValidation, "missing definition of column 'type': [%T]%+[1]v", dataMap)
		}
		complexTypeMap, typeMapOk := typeMap.(map[string]interface{})
		if !typeMapOk {
			return errors.Wrapf(ErrSchemaValidation, "unknown definition column 'type': [%T]%+[1]v", typeMap)
		}
		complexType, complexTypeOk := complexTypeMap["complex_type"]
		if !complexTypeOk {
			return errors.Wrapf(ErrSchemaValidation, "missing definition of column 'complex_type': [%T]%+[1]v", complexTypeMap)
		}
		switch complexType {
		case TYPE_LIST, TYPE_SET:
			t, err = GetBagTypeColumn(dataMap)
		case TYPE_MAP:
			t, err = GetMapTypeColumn(dataMap)
		case TYPE_TUPLE:
			t, err = GetTupleTypeColumn(dataMap)
		case TYPE_UDT:
			t, err = GetUDTTypeColumn(dataMap)
		default:
			return errors.Wrapf(ErrSchemaValidation, "unknown 'complex_type': [%T]%+[1]v", complexType)
		}
		if err != nil {
			return err
		}
	}
	*cd = ColumnDef{
		Name: t.Name,
		Type: t.Type,
	}
	return nil
}

type Columns []*ColumnDef

func (c Columns) Len() int {
	return len(c)
}

func (c Columns) Names() []string {
	names := make([]string, 0, len(c))
	for _, col := range c {
		names = append(names, col.Name)
	}
	return names
}

func (c Columns) Remove(column *ColumnDef) Columns {
	out := c
	for idx := range c {
		if c[idx].Name == column.Name {
			copy(out[idx:], out[idx+1:])
			out[len(out)-1] = nil
			out = out[:len(c)-1]
			break
		}
	}
	return out
}

func (c Columns) ToJSONMap(values map[string]interface{}, r *rand.Rand, p *PartitionRangeConfig) map[string]interface{} {
	for _, k := range c {
		values[k.Name] = k.Type.GenJSONValue(r, p)
	}
	return values
}

func (c Columns) ValidColumnsForPrimaryKey() Columns {
	validCols := make(Columns, 0, len(c))
	for _, col := range c {
		if col.IsValidForPrimaryKey() {
			validCols = append(validCols, col)
		}
	}
	return validCols
}

func (c Columns) Random(r *rand.Rand) *ColumnDef {
	return c[r.Intn(len(c))]
}

func (c Columns) LenValues() int {
	out := 0
	for _, col := range c {
		out += col.Type.LenValue()
	}
	return out
}

func (c Columns) NonCounters() Columns {
	out := make(Columns, 0, len(c))
	for _, col := range c {
		if _, ok := col.Type.(*CounterType); !ok {
			out = append(out, col)
		}
	}
	return out
}

// ValueVariationsNumber returns number of bytes generated value holds
func (c Columns) ValueVariationsNumber(p *PartitionRangeConfig) float64 {
	out := float64(1)
	for _, col := range c {
		out *= col.Type.ValueVariationsNumber(p)
	}
	return out
}

func GetMapTypeColumn(data map[string]interface{}) (out *ColumnDef, err error) {
	st := struct {
		Type map[string]interface{}
		Name string
	}{}

	if err = mapstructure.Decode(data, &st); err != nil {
		return nil, errors.Wrapf(err, "can't decode MapType value, value=%+v", data)
	}

	if _, ok := st.Type["frozen"]; !ok {
		return nil, errors.Errorf("not a map type, value=%v", st)
	}

	if _, ok := st.Type["value_type"]; !ok {
		return nil, errors.Errorf("not a map type, value=%v", st)
	}

	if _, ok := st.Type["key_type"]; !ok {
		return nil, errors.Errorf("not a map type, value=%v", st)
	}

	var frozen bool
	if err = mapstructure.Decode(st.Type["frozen"], &frozen); err != nil {
		return nil, errors.Wrapf(err, "can't decode bool value for MapType::Frozen, value=%v", st)
	}
	var valueType SimpleType
	if err = mapstructure.Decode(st.Type["value_type"], &valueType); err != nil {
		return nil, errors.Wrapf(err, "can't decode SimpleType value for MapType::ValueType, value=%v", st)
	}
	var keyType SimpleType
	if err = mapstructure.Decode(st.Type["key_type"], &keyType); err != nil {
		return nil, errors.Wrapf(err, "can't decode bool value for MapType::KeyType, value=%v", st)
	}
	return &ColumnDef{
		Name: st.Name,
		Type: &MapType{
			ComplexType: TYPE_MAP,
			Frozen:      frozen,
			ValueType:   valueType,
			KeyType:     keyType,
		},
	}, err
}

func GetBagTypeColumn(data map[string]interface{}) (out *ColumnDef, err error) {
	st := struct {
		Type map[string]interface{}
		Name string
	}{}

	if err = mapstructure.Decode(data, &st); err != nil {
		return nil, errors.Wrapf(err, "can't decode string value for BagType, value=%+v", data)
	}
	var complexType string
	if err = mapstructure.Decode(st.Type["complex_type"], &complexType); err != nil {
		return nil, errors.Wrapf(err, "can't decode string value for BagType::Frozen, value=%v", st)
	}
	var frozen bool
	if err = mapstructure.Decode(st.Type["frozen"], &frozen); err != nil {
		return nil, errors.Wrapf(err, "can't decode bool value for BagType::Frozen, value=%v", st)
	}
	var typ SimpleType
	if err = mapstructure.Decode(st.Type["value_type"], &typ); err != nil {
		return nil, errors.Wrapf(err, "can't decode SimpleType value for BagType::ValueType, value=%v", st)
	}
	return &ColumnDef{
		Name: st.Name,
		Type: &BagType{
			ComplexType: complexType,
			Frozen:      frozen,
			ValueType:   typ,
		},
	}, err
}

func GetTupleTypeColumn(data map[string]interface{}) (out *ColumnDef, err error) {
	st := struct {
		Type map[string]interface{}
		Name string
	}{}

	if err = mapstructure.Decode(data, &st); err != nil {
		return nil, errors.Wrapf(err, "can't decode []SimpleType value, value=%+v", data)
	}

	if _, ok := st.Type["value_types"]; !ok {
		return nil, errors.Errorf("not a tuple type, value=%v", st)
	}

	var dbTypes []SimpleType
	if err = mapstructure.Decode(st.Type["value_types"], &dbTypes); err != nil {
		return nil, errors.Wrapf(err, "can't decode []SimpleType value for TupleType::ValueTypes, value=%v", st)
	}
	var frozen bool
	if err = mapstructure.Decode(st.Type["frozen"], &frozen); err != nil {
		return nil, errors.Wrapf(err, "can't decode bool value for TupleType::ValueTypes, value=%v", st)
	}
	return &ColumnDef{
		Name: st.Name,
		Type: &TupleType{
			ComplexType: TYPE_TUPLE,
			ValueTypes:  dbTypes,
			Frozen:      frozen,
		},
	}, nil
}

func GetUDTTypeColumn(data map[string]interface{}) (out *ColumnDef, err error) {
	st := struct {
		Type map[string]interface{}
		Name string
	}{}

	if err = mapstructure.Decode(data, &st); err != nil {
		return nil, errors.Wrapf(err, "can't decode []SimpleType , value=%+v", data)
	}

	if _, ok := st.Type["value_types"]; !ok {
		return nil, errors.Errorf("not a UDT type, value=%v", st)
	}
	if _, ok := st.Type["type_name"]; !ok {
		return nil, errors.Errorf("not a UDT type, value=%v", st)
	}

	var dbTypes map[string]SimpleType
	if err = mapstructure.Decode(st.Type["value_types"], &dbTypes); err != nil {
		return nil, errors.Wrapf(err, "can't decode []SimpleType value for UDTType::ValueTypes, value=%v", st)
	}
	var frozen bool
	if err = mapstructure.Decode(st.Type["frozen"], &frozen); err != nil {
		return nil, errors.Wrapf(err, "can't decode bool value for UDTType::Frozen, value=%v", st)
	}
	var typeName string
	if err = mapstructure.Decode(st.Type["type_name"], &typeName); err != nil {
		return nil, errors.Wrapf(err, "can't decode string value for UDTType::TypeName, value=%v", st)
	}
	return &ColumnDef{
		Name: st.Name,
		Type: &UDTType{
			ComplexType: TYPE_UDT,
			ValueTypes:  dbTypes,
			TypeName:    typeName,
			Frozen:      frozen,
		},
	}, nil
}

func GetSimpleTypeColumn(data map[string]interface{}) (*ColumnDef, error) {
	st := struct {
		Name string
		Type SimpleType
	}{}
	err := mapstructure.Decode(data, &st)
	if err != nil {
		return nil, err
	}
	if st.Name == "" {
		return nil, errors.Wrapf(ErrSchemaValidation, "wrong definition of column 'name' [%T]%+[1]v", data)
	}
	if st.Type == "" {
		return nil, errors.Wrapf(ErrSchemaValidation, "empty definition of column 'type' [%T]%+[1]v", data)
	}

	knownType := false
	for _, sType := range AllTypes {
		if sType == st.Type {
			knownType = true
		}
	}
	if !knownType {
		return nil, errors.Wrapf(ErrSchemaValidation, "not simple type in column 'type' [%T]%+[1]v", data)
	}
	return &ColumnDef{
		Name: st.Name,
		Type: st.Type,
	}, err
}
