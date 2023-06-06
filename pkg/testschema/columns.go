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

package testschema

import (
	"encoding/json"
	"fmt"

	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"

	"github.com/scylladb/gemini/pkg/coltypes"
	"github.com/scylladb/gemini/pkg/typedef"

	"golang.org/x/exp/rand"
)

type ColumnDef struct {
	Type typedef.Type `json:"type"`
	Name string       `json:"name"`
}

func (cd *ColumnDef) IsValidForPrimaryKey() bool {
	for _, pkType := range coltypes.PkTypes {
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
		t, err = GetUDTTypeColumn(dataMap)
		if err != nil {
			t, err = GetTupleTypeColumn(dataMap)
			if err != nil {
				t, err = GetMapTypeColumn(dataMap)
				if err != nil {
					t, err = GetBagTypeColumn(dataMap)
					if err != nil {
						return err
					}
				}
			}
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

func (c Columns) Remove(idx int) Columns {
	out := c
	copy(out[idx:], out[idx+1:])
	out[len(out)-1] = nil
	return out[:len(c)-1]
}

func (c Columns) ToJSONMap(values map[string]interface{}, r *rand.Rand, p *typedef.PartitionRangeConfig) map[string]interface{} {
	for _, k := range c {
		switch t := k.Type.(type) {
		case coltypes.SimpleType:
			if t != coltypes.TYPE_BLOB {
				values[k.Name] = t.GenValue(r, p)[0]
				continue
			}
			v, ok := t.GenValue(r, p)[0].(string)
			if ok {
				values[k.Name] = "0x" + v
			}
		case *coltypes.TupleType:
			vv := t.GenValue(r, p)
			for i, val := range vv {
				if t.Types[i] == coltypes.TYPE_BLOB {
					v, ok := val.(string)
					if ok {
						v = "0x" + v
					}
					vv[i] = v
				}
			}
			values[k.Name] = vv
		default:
			panic(fmt.Sprintf("unknown type: %s", t.Name()))
		}
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

func (c Columns) Random() *ColumnDef {
	return c[rand.Intn(len(c))]
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
		if _, ok := col.Type.(*coltypes.CounterType); !ok {
			out = append(out, col)
		}
	}
	return out
}

func (c Columns) CreateMaterializedViews(tableName string, partitionKeys, clusteringKeys Columns) []MaterializedView {
	validColumns := c.ValidColumnsForPrimaryKey()
	var mvs []MaterializedView
	numMvs := 1
	for i := 0; i < numMvs; i++ {
		col := validColumns.Random()
		if col == nil {
			fmt.Printf("unable to generate valid columns for materialized view")
			continue
		}

		cols := Columns{
			col,
		}
		mv := MaterializedView{
			Name:           fmt.Sprintf("%s_mv_%d", tableName, i),
			PartitionKeys:  append(cols, partitionKeys...),
			ClusteringKeys: clusteringKeys,
			NonPrimaryKey:  *col,
		}
		mvs = append(mvs, mv)
	}
	return mvs
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
	var valueType coltypes.SimpleType
	if err = mapstructure.Decode(st.Type["value_type"], &valueType); err != nil {
		return nil, errors.Wrapf(err, "can't decode SimpleType value for MapType::ValueType, value=%v", st)
	}
	var keyType coltypes.SimpleType
	if err = mapstructure.Decode(st.Type["key_type"], &keyType); err != nil {
		return nil, errors.Wrapf(err, "can't decode bool value for MapType::KeyType, value=%v", st)
	}
	return &ColumnDef{
		Name: st.Name,
		Type: &coltypes.MapType{
			Frozen:    frozen,
			ValueType: valueType,
			KeyType:   keyType,
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

	var kind string
	if err = mapstructure.Decode(st.Type["kind"], &kind); err != nil {
		return nil, errors.Wrapf(err, "can't decode string value for BagType::Frozen, value=%v", st)
	}
	var frozen bool
	if err = mapstructure.Decode(st.Type["frozen"], &frozen); err != nil {
		return nil, errors.Wrapf(err, "can't decode bool value for BagType::Frozen, value=%v", st)
	}
	var typ coltypes.SimpleType
	if err = mapstructure.Decode(st.Type["type"], &typ); err != nil {
		return nil, errors.Wrapf(err, "can't decode SimpleType value for BagType::ValueType, value=%v", st)
	}
	return &ColumnDef{
		Name: st.Name,
		Type: &coltypes.BagType{
			Kind:   kind,
			Frozen: frozen,
			Type:   typ,
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

	if _, ok := st.Type["coltypes"]; !ok {
		return nil, errors.Errorf("not a tuple type, value=%v", st)
	}

	var dbTypes []coltypes.SimpleType
	if err = mapstructure.Decode(st.Type["coltypes"], &dbTypes); err != nil {
		return nil, errors.Wrapf(err, "can't decode []SimpleType value for TupleType::Types, value=%v", st)
	}
	var frozen bool
	if err = mapstructure.Decode(st.Type["frozen"], &frozen); err != nil {
		return nil, errors.Wrapf(err, "can't decode bool value for TupleType::Types, value=%v", st)
	}
	return &ColumnDef{
		Name: st.Name,
		Type: &coltypes.TupleType{
			Types:  dbTypes,
			Frozen: frozen,
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

	if _, ok := st.Type["coltypes"]; !ok {
		return nil, errors.Errorf("not a UDT type, value=%v", st)
	}
	if _, ok := st.Type["type_name"]; !ok {
		return nil, errors.Errorf("not a UDT type, value=%v", st)
	}

	var dbTypes map[string]coltypes.SimpleType
	if err = mapstructure.Decode(st.Type["coltypes"], &dbTypes); err != nil {
		return nil, errors.Wrapf(err, "can't decode []SimpleType value for UDTType::Types, value=%v", st)
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
		Type: &coltypes.UDTType{
			Types:    dbTypes,
			TypeName: typeName,
			Frozen:   frozen,
		},
	}, nil
}

func GetSimpleTypeColumn(data map[string]interface{}) (*ColumnDef, error) {
	st := struct {
		Name string
		Type coltypes.SimpleType
	}{}
	err := mapstructure.Decode(data, &st)
	if err != nil {
		return nil, err
	}
	return &ColumnDef{
		Name: st.Name,
		Type: st.Type,
	}, err
}
