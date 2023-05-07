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

package gemini

import (
	"encoding/json"
	"fmt"

	"golang.org/x/exp/rand"
)

type ColumnDef struct {
	Type Type   `json:"type"`
	Name string `json:"name"`
}

func (cd *ColumnDef) IsValidForPrimaryKey() bool {
	for _, pkType := range pkTypes {
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

	t, err := getSimpleTypeColumn(dataMap)
	if err != nil {
		t, err = getUDTTypeColumn(dataMap)
		if err != nil {
			t, err = getTupleTypeColumn(dataMap)
			if err != nil {
				t, err = getMapTypeColumn(dataMap)
				if err != nil {
					t, err = getBagTypeColumn(dataMap)
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

func (c Columns) Names() []string {
	names := make([]string, 0, len(c))
	for _, col := range c {
		names = append(names, col.Name)
	}
	return names
}

func (c Columns) ToJSONMap(values map[string]interface{}, r *rand.Rand, p PartitionRangeConfig) map[string]interface{} {
	for _, k := range c {
		switch t := k.Type.(type) {
		case SimpleType:
			if t != TYPE_BLOB {
				values[k.Name] = t.GenValue(r, p)[0]
				continue
			}
			v, ok := t.GenValue(r, p)[0].(string)
			if ok {
				values[k.Name] = "0x" + v
			}
		case *TupleType:
			vv := t.GenValue(r, p)
			for i, val := range vv {
				if t.Types[i] == TYPE_BLOB {
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

func (c Columns) CreateIndexes(tableName string, maxIndexes int) []IndexDef {
	createdCount := 0
	indexes := make([]IndexDef, 0, maxIndexes)
	for i, col := range c {
		if col.Type.Indexable() && typeIn(col, typesForIndex) {
			indexes = append(indexes, IndexDef{Name: genIndexName(tableName+"_col", i), Column: col.Name, ColumnIdx: i})
			createdCount++
		}
		if createdCount == maxIndexes {
			break
		}
	}
	return indexes
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
