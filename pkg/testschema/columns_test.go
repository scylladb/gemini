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

package testschema_test

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/scylladb/gemini/pkg/coltypes"
	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/testschema"
	"github.com/scylladb/gemini/pkg/typedef"
)

func TestMarshalUnmarshal(t *testing.T) {
	sc := &typedef.SchemaConfig{
		MaxPartitionKeys:  3,
		MinPartitionKeys:  2,
		MaxClusteringKeys: 3,
		MinClusteringKeys: 2,
		MaxColumns:        3,
		MinColumns:        2,
		MaxTupleParts:     2,
		MaxUDTParts:       2,
	}
	columns := testschema.Columns{
		&testschema.ColumnDef{
			Name: generators.GenColumnName("col", 0),
			Type: generators.GenMapType(sc),
		},
		&testschema.ColumnDef{
			Name: generators.GenColumnName("col", 1),
			Type: generators.GenSetType(sc),
		},
		&testschema.ColumnDef{
			Name: generators.GenColumnName("col", 2),
			Type: generators.GenListType(sc),
		},
		&testschema.ColumnDef{
			Name: generators.GenColumnName("col", 3),
			Type: generators.GenTupleType(sc),
		},
		&testschema.ColumnDef{
			Name: generators.GenColumnName("col", 4),
			Type: generators.GenUDTType(sc),
		},
	}
	s1 := &testschema.Schema{
		Tables: []*testschema.Table{
			{
				Name: "table",
				PartitionKeys: testschema.Columns{
					&testschema.ColumnDef{
						Name: generators.GenColumnName("pk", 0),
						Type: generators.GenSimpleType(sc),
					},
				},
				ClusteringKeys: testschema.Columns{
					&testschema.ColumnDef{
						Name: generators.GenColumnName("ck", 0),
						Type: generators.GenSimpleType(sc),
					},
				},
				Columns: columns,
				Indexes: []typedef.IndexDef{
					{
						Name:   generators.GenIndexName("col", 0),
						Column: columns[0].Name,
					},
					{
						Name:   generators.GenIndexName("col", 1),
						Column: columns[1].Name,
					},
				},
				MaterializedViews: []testschema.MaterializedView{
					{
						Name: "table1_mv_0",
						PartitionKeys: testschema.Columns{
							&testschema.ColumnDef{
								Name: "pk_mv_0",
								Type: generators.GenListType(sc),
							},
							&testschema.ColumnDef{
								Name: "pk_mv_1",
								Type: generators.GenTupleType(sc),
							},
						},
						ClusteringKeys: testschema.Columns{
							&testschema.ColumnDef{
								Name: "ck_mv_0",
								Type: generators.GenSetType(sc),
							},
							&testschema.ColumnDef{
								Name: "ck_mv_1",
								Type: generators.GenUDTType(sc),
							},
						},
						NonPrimaryKey: nil,
					},
				},
			},
		},
	}

	opts := cmp.Options{
		cmp.AllowUnexported(testschema.Table{}),
		cmpopts.IgnoreUnexported(testschema.Table{}),
	}

	b, err := json.MarshalIndent(s1, "  ", "  ")
	if err != nil {
		t.Fatalf("unable to marshal json, error=%s\n", err)
	}

	s2 := &testschema.Schema{}
	if err = json.Unmarshal(b, &s2); err != nil {
		t.Fatalf("unable to unmarshal json, error=%s\n", err)
	}

	if diff := cmp.Diff(s1, s2, opts); diff != "" {
		t.Errorf("schema not the same after marshal/unmarshal, diff=%s", diff)
	}
}

func TestPrimitives(t *testing.T) {
	sc := &typedef.SchemaConfig{
		MaxPartitionKeys:  3,
		MinPartitionKeys:  2,
		MaxClusteringKeys: 3,
		MinClusteringKeys: 2,
		MaxColumns:        3,
		MinColumns:        2,
		MaxTupleParts:     2,
		MaxUDTParts:       2,
	}

	cols := testschema.Columns{
		&testschema.ColumnDef{
			Name: "pk_mv_0",
			Type: generators.GenListType(sc),
		},
		&testschema.ColumnDef{
			Name: "pk_mv_1",
			Type: generators.GenTupleType(sc),
		},
		&testschema.ColumnDef{
			Name: "ct_1",
			Type: &coltypes.CounterType{},
		},
	}
	if cols.Len() != 3 {
		t.Errorf("%d != %d", cols.Len(), 3)
	}
	colNames := strings.Join(cols.Names(), ",")
	if colNames != "pk_mv_0,pk_mv_1,ct_1" {
		t.Errorf("%s != %s", colNames, "pk_mv_0,pk_mv_1,ct_1")
	}
	if cols.NonCounters().Len() != 2 {
		t.Errorf("%d != %d", cols.NonCounters().Len(), 2)
	}
	colNames = strings.Join(cols.NonCounters().Names(), ",")
	if colNames != "pk_mv_0,pk_mv_1" {
		t.Errorf("%s != %s", colNames, "pk_mv_0,pk_mv_1")
	}

	cols = cols.Remove(2)
	if cols.Len() != 2 {
		t.Errorf("%d != %d", cols.Len(), 2)
	}
	colNames = strings.Join(cols.Names(), ",")
	if colNames != "pk_mv_0,pk_mv_1" {
		t.Errorf("%s != %s", colNames, "pk_mv_0,pk_mv_1")
	}

	cols = cols.Remove(0)
	if cols.Len() != 1 {
		t.Errorf("%d != %d", cols.Len(), 1)
	}
	colNames = strings.Join(cols.Names(), ",")
	if colNames != "pk_mv_1" {
		t.Errorf("%s != %s", colNames, "pk_mv_1")
	}

	cols = cols.Remove(0)
	if cols.Len() != 0 {
		t.Errorf("%d != %d", cols.Len(), 0)
	}
	colNames = strings.Join(cols.Names(), ",")
	if colNames != "" {
		t.Errorf("%s != %s", colNames, "")
	}
}
