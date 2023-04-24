// Copyright 2019 ScyllaDB
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

//nolint:lll
package gemini

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/scylladb/gemini/tableopts"
)

func TestSchemaConfigValidate(t *testing.T) {
	tests := map[string]struct {
		config *SchemaConfig
		want   error
	}{
		"empty": {
			config: &SchemaConfig{},
			want:   ErrSchemaConfigInvalidPK,
		},
		"valid": {
			config: &SchemaConfig{
				MaxPartitionKeys:  3,
				MinPartitionKeys:  2,
				MaxClusteringKeys: 3,
				MinClusteringKeys: 2,
				MaxColumns:        3,
				MinColumns:        2,
			},
			want: nil,
		},
		"min_pk_gt_than_max_pk": {
			config: &SchemaConfig{
				MaxPartitionKeys: 2,
				MinPartitionKeys: 3,
			},
			want: ErrSchemaConfigInvalidPK,
		},
		"ck_missing": {
			config: &SchemaConfig{
				MaxPartitionKeys: 3,
				MinPartitionKeys: 2,
			},
			want: ErrSchemaConfigInvalidCK,
		},
		"min_ck_gt_than_max_ck": {
			config: &SchemaConfig{
				MaxPartitionKeys:  3,
				MinPartitionKeys:  2,
				MaxClusteringKeys: 2,
				MinClusteringKeys: 3,
			},
			want: ErrSchemaConfigInvalidCK,
		},
		"columns_missing": {
			config: &SchemaConfig{
				MaxPartitionKeys:  3,
				MinPartitionKeys:  2,
				MaxClusteringKeys: 3,
				MinClusteringKeys: 2,
			},
			want: ErrSchemaConfigInvalidCols,
		},
		"min_cols_gt_than_max_cols": {
			config: &SchemaConfig{
				MaxPartitionKeys:  3,
				MinPartitionKeys:  2,
				MaxClusteringKeys: 3,
				MinClusteringKeys: 2,
				MaxColumns:        2,
				MinColumns:        3,
			},
			want: ErrSchemaConfigInvalidCols,
		},
	}
	cmp.AllowUnexported()
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got := test.config.Valid()
			//nolint:errorlint
			if got != test.want {
				t.Fatalf("expected '%s', got '%s'", test.want, got)
			}
		})
	}
}

func options(cql string) []string {
	opt, _ := tableopts.FromCQL(cql)
	return []string{opt.ToCQL()}
}

func TestGetCreateSchema(t *testing.T) {
	ks := Keyspace{Name: "ks1"}
	tests := map[string]struct {
		table *Table
		want  string
	}{
		"single_partition_key": {
			table: &Table{
				Name:          "tbl0",
				PartitionKeys: createColumns(1, "pk"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text, PRIMARY KEY ((pk0)))",
		},
		"single_partition_key_compact": {
			table: &Table{
				Name:          "tbl0",
				PartitionKeys: createColumns(1, "pk"),
				TableOptions:  options("compaction = {'class':'LeveledCompactionStrategy','enabled':true,'tombstone_threshold':0.2,'tombstone_compaction_interval':86400,'sstable_size_in_mb':160}"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text, PRIMARY KEY ((pk0))) WITH compaction = {'class':'LeveledCompactionStrategy','enabled':true,'sstable_size_in_mb':160,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2};",
		},
		"single_partition_key_single_column": {
			table: &Table{
				Name:          "tbl0",
				PartitionKeys: createColumns(1, "pk"),
				Columns:       createColumns(1, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,col0 text, PRIMARY KEY ((pk0)))",
		},
		"single_partition_key_multiple_column": {
			table: &Table{
				Name:          "tbl0",
				PartitionKeys: createColumns(1, "pk"),
				Columns:       createColumns(2, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,col0 text,col1 text, PRIMARY KEY ((pk0)))",
		},
		"multiple_partition_key_multiple_column": {
			table: &Table{
				Name:          "tbl0",
				PartitionKeys: createColumns(2, "pk"),
				Columns:       createColumns(2, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,pk1 text,col0 text,col1 text, PRIMARY KEY ((pk0,pk1)))",
		},
		"single_partition_key_single_clustering_key": {
			table: &Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(1, "pk"),
				ClusteringKeys: createColumns(1, "ck"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,ck0 text, PRIMARY KEY ((pk0), ck0))",
		},
		"single_partition_key_single_clustering_key_compact": {
			table: &Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(1, "pk"),
				ClusteringKeys: createColumns(1, "ck"),
				TableOptions:   options("compaction = {'class':'LeveledCompactionStrategy','enabled':true,'tombstone_threshold':0.2,'tombstone_compaction_interval':86400,'sstable_size_in_mb':160}"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,ck0 text, PRIMARY KEY ((pk0), ck0)) WITH compaction = {'class':'LeveledCompactionStrategy','enabled':true,'sstable_size_in_mb':160,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2};",
		},
		"single_partition_key_single_clustering_key_single_column": {
			table: &Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(1, "pk"),
				ClusteringKeys: createColumns(1, "ck"),
				Columns:        createColumns(1, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,ck0 text,col0 text, PRIMARY KEY ((pk0), ck0))",
		},
		"single_partition_key_single_clustering_key_multiple_column": {
			table: &Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(1, "pk"),
				ClusteringKeys: createColumns(1, "ck"),
				Columns:        createColumns(2, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,ck0 text,col0 text,col1 text, PRIMARY KEY ((pk0), ck0))",
		},
		"multiple_partition_key_single_clustering_key": {
			table: &Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(2, "pk"),
				ClusteringKeys: createColumns(1, "ck"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,pk1 text,ck0 text, PRIMARY KEY ((pk0,pk1), ck0))",
		},
		"multiple_partition_key_single_clustering_key_single_column": {
			table: &Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(2, "pk"),
				ClusteringKeys: createColumns(1, "ck"),
				Columns:        createColumns(1, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,pk1 text,ck0 text,col0 text, PRIMARY KEY ((pk0,pk1), ck0))",
		},
		"multiple_partition_key_single_clustering_key_multiple_column": {
			table: &Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(2, "pk"),
				ClusteringKeys: createColumns(1, "ck"),
				Columns:        createColumns(2, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,pk1 text,ck0 text,col0 text,col1 text, PRIMARY KEY ((pk0,pk1), ck0))",
		},
		"multiple_partition_key_multiple_clustering_key": {
			table: &Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(2, "pk"),
				ClusteringKeys: createColumns(2, "ck"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,pk1 text,ck0 text,ck1 text, PRIMARY KEY ((pk0,pk1), ck0,ck1))",
		},
		"multiple_partition_key_multiple_clustering_key_single_column": {
			table: &Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(2, "pk"),
				ClusteringKeys: createColumns(2, "ck"),
				Columns:        createColumns(1, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,pk1 text,ck0 text,ck1 text,col0 text, PRIMARY KEY ((pk0,pk1), ck0,ck1))",
		},
		"multiple_partition_key_multiple_clustering_key_multiple_column": {
			table: &Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(2, "pk"),
				ClusteringKeys: createColumns(2, "ck"),
				Columns:        createColumns(2, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,pk1 text,ck0 text,ck1 text,col0 text,col1 text, PRIMARY KEY ((pk0,pk1), ck0,ck1))",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got := test.table.GetCreateTable(ks)
			if diff := cmp.Diff(got, test.want); diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func createColumns(cnt int, prefix string) Columns {
	var cols Columns
	for i := 0; i < cnt; i++ {
		cols = append(cols, ColumnDef{
			Name: genColumnName(prefix, i),
			Type: TYPE_TEXT,
		})
	}
	return cols
}
