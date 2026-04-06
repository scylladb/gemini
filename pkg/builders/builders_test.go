// Copyright 2025 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package builders_test

import (
	"testing"

	"github.com/scylladb/gemini/pkg/builders"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/typedef"
)

func TestAlterTableBuilder_ToCql(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		stmt string
	}{
		{name: "add-column", stmt: "ALTER TABLE ks.tbl ADD col1 int"},
		{name: "drop-column", stmt: "ALTER TABLE ks.tbl DROP col2"},
		{name: "empty", stmt: ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			atb := builders.AlterTableBuilder{Stmt: tc.stmt}
			stmt, names := atb.ToCql()
			if stmt != tc.stmt {
				t.Errorf("ToCql() stmt = %q; want %q", stmt, tc.stmt)
			}
			if names != nil {
				t.Errorf("ToCql() names = %v; want nil", names)
			}
		})
	}
}

func TestSchemaBuilder_Build_BasicKeyspaceAndConfig(t *testing.T) {
	t.Parallel()

	ks := typedef.Keyspace{
		Name:        "test_ks",
		Replication: replication.NewSimpleStrategy(),
	}
	cfg := typedef.SchemaConfig{
		MaxPartitionKeys:  2,
		MinPartitionKeys:  1,
		MaxClusteringKeys: 2,
		MinClusteringKeys: 0,
		MaxColumns:        5,
		MinColumns:        1,
	}

	var sb builders.SchemaBuilder
	schema := sb.Keyspace(ks).Config(cfg).Build()

	if schema == nil {
		t.Fatal("Build() returned nil schema")
	}
	if schema.Keyspace.Name != ks.Name {
		t.Errorf("schema.Keyspace.Name = %q; want %q", schema.Keyspace.Name, ks.Name)
	}
	if len(schema.Tables) != 0 {
		t.Errorf("schema.Tables len = %d; want 0", len(schema.Tables))
	}
}

func TestSchemaBuilder_Build_WithTables(t *testing.T) {
	t.Parallel()

	ks := typedef.Keyspace{Name: "ks1", Replication: replication.NewSimpleStrategy()}
	cfg := typedef.SchemaConfig{
		MaxPartitionKeys:  2,
		MinPartitionKeys:  1,
		MaxClusteringKeys: 2,
		MinClusteringKeys: 0,
		MaxColumns:        5,
		MinColumns:        1,
	}

	tbl := &typedef.Table{
		Name: "tbl1",
		PartitionKeys: typedef.Columns{
			{Name: "pk1", Type: typedef.TypeInt},
		},
		ClusteringKeys: typedef.Columns{
			{Name: "ck1", Type: typedef.TypeText},
		},
		Columns: typedef.Columns{
			{Name: "col1", Type: typedef.TypeBoolean},
		},
	}

	var sb builders.SchemaBuilder
	schema := sb.Keyspace(ks).Config(cfg).Table(tbl).Build()

	if schema == nil {
		t.Fatal("Build() returned nil schema")
	}
	if len(schema.Tables) != 1 {
		t.Fatalf("schema.Tables len = %d; want 1", len(schema.Tables))
	}
	got := schema.Tables[0]
	if got.Name != "tbl1" {
		t.Errorf("table name = %q; want %q", got.Name, "tbl1")
	}
	// Init() must have been called: SortKeyNames should be populated.
	if len(got.SortKeyNames) == 0 {
		t.Error("SortKeyNames is empty; Init() was not called by Build()")
	}
	wantSortKeys := []string{"pk1", "ck1"}
	if len(got.SortKeyNames) != len(wantSortKeys) {
		t.Fatalf("SortKeyNames len = %d; want %d", len(got.SortKeyNames), len(wantSortKeys))
	}
	for i, name := range wantSortKeys {
		if got.SortKeyNames[i] != name {
			t.Errorf("SortKeyNames[%d] = %q; want %q", i, got.SortKeyNames[i], name)
		}
	}
	// PartitionKeysLenValues must be set.
	if got.PartitionKeysLenValues == 0 {
		t.Error("PartitionKeysLenValues = 0; Init() should have set it")
	}
}

func TestSchemaBuilder_Build_MultipleTables(t *testing.T) {
	t.Parallel()

	ks := typedef.Keyspace{Name: "ks_multi", Replication: replication.NewSimpleStrategy()}

	makeTable := func(name string) *typedef.Table {
		return &typedef.Table{
			Name: name,
			PartitionKeys: typedef.Columns{
				{Name: "pk", Type: typedef.TypeInt},
			},
			ClusteringKeys: typedef.Columns{},
			Columns:        typedef.Columns{{Name: "v", Type: typedef.TypeText}},
		}
	}

	var sb builders.SchemaBuilder
	schema := sb.Keyspace(ks).Table(makeTable("a")).Table(makeTable("b")).Table(makeTable("c")).Build()

	if len(schema.Tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(schema.Tables))
	}

	names := map[string]bool{}
	for _, tbl := range schema.Tables {
		names[tbl.Name] = true
		if len(tbl.SortKeyNames) == 0 {
			t.Errorf("table %q: SortKeyNames empty after Build()", tbl.Name)
		}
	}
	for _, want := range []string{"a", "b", "c"} {
		if !names[want] {
			t.Errorf("table %q not found in schema", want)
		}
	}
}
