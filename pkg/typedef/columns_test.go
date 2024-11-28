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

package typedef_test

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/typedef"
)

var allSimpleTypes = []typedef.SimpleType{
	typedef.TYPE_ASCII,
	typedef.TYPE_BIGINT,
	typedef.TYPE_BLOB,
	typedef.TYPE_BOOLEAN,
	typedef.TYPE_DATE,
	typedef.TYPE_DECIMAL,
	typedef.TYPE_DOUBLE,
	typedef.TYPE_DURATION,
	typedef.TYPE_FLOAT,
	typedef.TYPE_INET,
	typedef.TYPE_INT,
	typedef.TYPE_SMALLINT,
	typedef.TYPE_TEXT,
	typedef.TYPE_TIME,
	typedef.TYPE_TIMESTAMP,
	typedef.TYPE_TIMEUUID,
	typedef.TYPE_TINYINT,
	typedef.TYPE_UUID,
	typedef.TYPE_VARCHAR,
	typedef.TYPE_VARINT,
}

func TestColumnMarshalUnmarshal(t *testing.T) {
	t.Parallel()
	type testCase struct {
		def      typedef.ColumnDef
		expected string
	}

	testCases := make([]testCase, 0, len(allSimpleTypes))

	for _, simpleType := range allSimpleTypes {
		testCases = append(testCases, testCase{
			def: typedef.ColumnDef{
				Name: simpleType.Name(),
				Type: simpleType,
			},
			expected: fmt.Sprintf("{\"type\":\"%s\",\"name\":\"%s\"}", simpleType.Name(), simpleType.Name()),
		})
	}
	udtTypes := map[string]typedef.SimpleType{}

	for _, simpleType := range allSimpleTypes {
		udtTypes["col_"+simpleType.Name()] = simpleType
	}

	testCases = append(testCases, testCase{
		def: typedef.ColumnDef{
			Type: &typedef.UDTType{
				ComplexType: typedef.TYPE_UDT,
				TypeName:    "udt1",
				ValueTypes:  udtTypes,
			},
			Name: "udt1",
		},
		//nolint:lll
		expected: "{\"type\":{\"complex_type\":\"udt\",\"value_types\":{\"col_ascii\":\"ascii\",\"col_bigint\":\"bigint\",\"col_blob\":\"blob\",\"col_boolean\":\"boolean\",\"col_date\":\"date\",\"col_decimal\":\"decimal\",\"col_double\":\"double\",\"col_duration\":\"duration\",\"col_float\":\"float\",\"col_inet\":\"inet\",\"col_int\":\"int\",\"col_smallint\":\"smallint\",\"col_text\":\"text\",\"col_time\":\"time\",\"col_timestamp\":\"timestamp\",\"col_timeuuid\":\"timeuuid\",\"col_tinyint\":\"tinyint\",\"col_uuid\":\"uuid\",\"col_varchar\":\"varchar\",\"col_varint\":\"varint\"},\"type_name\":\"udt1\",\"frozen\":false},\"name\":\"udt1\"}",
	})

	for id := range testCases {
		tcase := testCases[id]
		t.Run(tcase.def.Name, func(t *testing.T) {
			t.Parallel()
			marshaledData, err := json.Marshal(tcase.def)
			if err != nil {
				t.Fatal(err.Error())
			}
			fmt.Println(tcase.expected)

			if diff := cmp.Diff(string(marshaledData), tcase.expected); diff != "" {
				t.Errorf("Comparison failed: %s", diff)
			}
			var unmarshaledDef typedef.ColumnDef
			err = json.Unmarshal(marshaledData, &unmarshaledDef)
			if err != nil {
				t.Fatal(err.Error())
			}

			if diff := cmp.Diff(tcase.def, unmarshaledDef); diff != "" {
				t.Errorf("cmp.Diff failed: %s", diff)
			}
		})
	}
}

func TestMarshalUnmarshal(t *testing.T) {
	t.Parallel()
	r := rand.New(rand.NewSource(rand.Uint64()))

	s1 := getTestSchema(r)

	opts := cmp.Options{
		cmp.AllowUnexported(typedef.Table{}, typedef.MaterializedView{}),
		cmpopts.IgnoreUnexported(typedef.Table{}, typedef.MaterializedView{}),
	}

	b, err := json.MarshalIndent(s1, "  ", "  ")
	if err != nil {
		t.Fatalf("unable to marshal json, error=%s\n", err)
	}

	s2 := &typedef.Schema{}
	if err = json.Unmarshal(b, &s2); err != nil {
		t.Fatalf("unable to unmarshal json, error=%s\n", err)
	}

	if diff := cmp.Diff(s1, s2, opts); diff != "" {
		t.Errorf("schema not the same after marshal/unmarshal, diff=%s", diff)
	}
}

func TestPrimitives(t *testing.T) {
	r := rand.New(rand.NewSource(rand.Uint64()))

	t.Parallel()

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

	cols := typedef.Columns{
		&typedef.ColumnDef{
			Name: "pk_mv_0",
			Type: generators.GenListType(sc, r),
		},
		&typedef.ColumnDef{
			Name: "pk_mv_1",
			Type: generators.GenTupleType(sc, r),
		},
		&typedef.ColumnDef{
			Name: "ct_1",
			Type: &typedef.CounterType{},
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

	cols = cols.Remove(cols[2])
	if cols.Len() != 2 {
		t.Errorf("%d != %d", cols.Len(), 2)
	}
	colNames = strings.Join(cols.Names(), ",")
	if colNames != "pk_mv_0,pk_mv_1" {
		t.Errorf("%s != %s", colNames, "pk_mv_0,pk_mv_1")
	}

	cols = cols.Remove(cols[0])
	if cols.Len() != 1 {
		t.Errorf("%d != %d", cols.Len(), 1)
	}
	colNames = strings.Join(cols.Names(), ",")
	if colNames != "pk_mv_1" {
		t.Errorf("%s != %s", colNames, "pk_mv_1")
	}

	cols = cols.Remove(cols[0])
	if cols.Len() != 0 {
		t.Errorf("%d != %d", cols.Len(), 0)
	}
	colNames = strings.Join(cols.Names(), ",")
	if colNames != "" {
		t.Errorf("%s != %s", colNames, "")
	}
}

func TestValidColumnsForDelete(t *testing.T) {
	t.Parallel()

	r := rand.New(rand.NewSource(rand.Uint64()))
	s1 := getTestSchema(r)
	expected := typedef.Columns{
		s1.Tables[0].Columns[2],
		s1.Tables[0].Columns[3],
		s1.Tables[0].Columns[4],
	}

	validColsToDelete := s1.Tables[0].ValidColumnsForDelete()
	if fmt.Sprintf("%v", expected) != fmt.Sprintf("%v", validColsToDelete) {
		t.Errorf("wrong valid columns for delete. Expected:%v .Received:%v", expected, validColsToDelete)
	}

	s1.Tables[0].MaterializedViews[0].NonPrimaryKey = s1.Tables[0].Columns[4]
	expected = typedef.Columns{
		s1.Tables[0].Columns[2],
		s1.Tables[0].Columns[3],
	}
	validColsToDelete = s1.Tables[0].ValidColumnsForDelete()
	if fmt.Sprintf("%v", expected) != fmt.Sprintf("%v", validColsToDelete) {
		t.Errorf("wrong valid columns for delete. Expected:%v .Received:%v", expected, validColsToDelete)
	}

	s1.Tables[0].MaterializedViews = append(s1.Tables[0].MaterializedViews, s1.Tables[0].MaterializedViews[0])
	s1.Tables[0].MaterializedViews[1].NonPrimaryKey = s1.Tables[0].Columns[3]
	s1.Tables[0].MaterializedViews = append(s1.Tables[0].MaterializedViews, s1.Tables[0].MaterializedViews[0])
	s1.Tables[0].MaterializedViews[2].NonPrimaryKey = s1.Tables[0].Columns[2]

	expected = typedef.Columns{}
	validColsToDelete = s1.Tables[0].ValidColumnsForDelete()
	if fmt.Sprintf("%v", expected) != fmt.Sprintf("%v", validColsToDelete) {
		t.Errorf("wrong valid columns for delete. Expected:%v .Received:%v", expected, validColsToDelete)
	}
}

func getTestSchema(r *rand.Rand) *typedef.Schema {
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
	columns := typedef.Columns{
		&typedef.ColumnDef{
			Name: generators.GenColumnName("col", 0),
			Type: generators.GenMapType(sc, r),
		},
		&typedef.ColumnDef{
			Name: generators.GenColumnName("col", 1),
			Type: generators.GenSetType(sc, r),
		},
		&typedef.ColumnDef{
			Name: generators.GenColumnName("col", 2),
			Type: generators.GenListType(sc, r),
		},
		&typedef.ColumnDef{
			Name: generators.GenColumnName("col", 3),
			Type: generators.GenTupleType(sc, r),
		},
		&typedef.ColumnDef{
			Name: generators.GenColumnName("col", 4),
			Type: generators.GenUDTType(sc, r),
		},
	}

	sch := &typedef.Schema{
		Tables: []*typedef.Table{
			{
				Name: "table",
				PartitionKeys: typedef.Columns{
					&typedef.ColumnDef{
						Name: generators.GenColumnName("pk", 0),
						Type: generators.GenSimpleType(sc, r),
					},
				},
				ClusteringKeys: typedef.Columns{
					&typedef.ColumnDef{
						Name: generators.GenColumnName("ck", 0),
						Type: generators.GenSimpleType(sc, r),
					},
				},
				Columns: columns,
			},
		},
	}
	sch.Tables[0].Indexes = []typedef.IndexDef{
		{
			IndexName:  generators.GenIndexName(sch.Tables[0].Name+"_col", 0),
			ColumnName: columns[0].Name,
			Column:     columns[0],
		},
		{
			IndexName:  generators.GenIndexName(sch.Tables[0].Name+"_col", 1),
			ColumnName: columns[1].Name,
			Column:     columns[1],
		},
	}

	sch.Tables[0].MaterializedViews = []typedef.MaterializedView{
		{
			Name:           sch.Tables[0].Name + "_mv_0",
			PartitionKeys:  sch.Tables[0].PartitionKeys,
			ClusteringKeys: sch.Tables[0].ClusteringKeys,
			NonPrimaryKey:  nil,
		},
	}

	return sch
}
