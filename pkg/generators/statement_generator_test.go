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

package generators_test

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/tableopts"
	"github.com/scylladb/gemini/pkg/typedef"
)

func options(cql string) []string {
	opt, _ := tableopts.FromCQL(cql)
	return []string{opt.ToCQL()}
}

func TestGetCreateSchema(t *testing.T) {
	ks := typedef.Keyspace{Name: "ks1"}
	tests := map[string]struct {
		table *typedef.Table
		want  string
	}{
		"single_partition_key": {
			table: &typedef.Table{
				Name:          "tbl0",
				PartitionKeys: createColumns(1, "pk"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text, PRIMARY KEY ((pk0)))",
		},
		"single_partition_key_compact": {
			table: &typedef.Table{
				Name:          "tbl0",
				PartitionKeys: createColumns(1, "pk"),
				TableOptions: options("compaction = {'class':'LeveledCompactionStrategy','enabled':true,'tombstone_threshold':0.2," +
					"'tombstone_compaction_interval':86400,'sstable_size_in_mb':160}"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text, PRIMARY KEY ((pk0))) WITH compaction = " +
				"{'class':'LeveledCompactionStrategy','enabled':true,'sstable_size_in_mb':160,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2};",
		},
		"single_partition_key_single_column": {
			table: &typedef.Table{
				Name:          "tbl0",
				PartitionKeys: createColumns(1, "pk"),
				Columns:       createColumns(1, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,col0 text, PRIMARY KEY ((pk0)))",
		},
		"single_partition_key_multiple_column": {
			table: &typedef.Table{
				Name:          "tbl0",
				PartitionKeys: createColumns(1, "pk"),
				Columns:       createColumns(2, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,col0 text,col1 text, PRIMARY KEY ((pk0)))",
		},
		"multiple_partition_key_multiple_column": {
			table: &typedef.Table{
				Name:          "tbl0",
				PartitionKeys: createColumns(2, "pk"),
				Columns:       createColumns(2, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,pk1 text,col0 text,col1 text, PRIMARY KEY ((pk0,pk1)))",
		},
		"single_partition_key_single_clustering_key": {
			table: &typedef.Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(1, "pk"),
				ClusteringKeys: createColumns(1, "ck"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,ck0 text, PRIMARY KEY ((pk0), ck0))",
		},
		"single_partition_key_single_clustering_key_compact": {
			table: &typedef.Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(1, "pk"),
				ClusteringKeys: createColumns(1, "ck"),
				TableOptions: options("compaction = {'class':'LeveledCompactionStrategy','enabled':true,'tombstone_threshold':0.2," +
					"'tombstone_compaction_interval':86400,'sstable_size_in_mb':160}"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,ck0 text, PRIMARY KEY ((pk0), ck0)) WITH compaction = " +
				"{'class':'LeveledCompactionStrategy','enabled':true,'sstable_size_in_mb':160,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2};",
		},
		"single_partition_key_single_clustering_key_single_column": {
			table: &typedef.Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(1, "pk"),
				ClusteringKeys: createColumns(1, "ck"),
				Columns:        createColumns(1, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,ck0 text,col0 text, PRIMARY KEY ((pk0), ck0))",
		},
		"single_partition_key_single_clustering_key_multiple_column": {
			table: &typedef.Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(1, "pk"),
				ClusteringKeys: createColumns(1, "ck"),
				Columns:        createColumns(2, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,ck0 text,col0 text,col1 text, PRIMARY KEY ((pk0), ck0))",
		},
		"multiple_partition_key_single_clustering_key": {
			table: &typedef.Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(2, "pk"),
				ClusteringKeys: createColumns(1, "ck"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,pk1 text,ck0 text, PRIMARY KEY ((pk0,pk1), ck0))",
		},
		"multiple_partition_key_single_clustering_key_single_column": {
			table: &typedef.Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(2, "pk"),
				ClusteringKeys: createColumns(1, "ck"),
				Columns:        createColumns(1, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,pk1 text,ck0 text,col0 text, PRIMARY KEY ((pk0,pk1), ck0))",
		},
		"multiple_partition_key_single_clustering_key_multiple_column": {
			table: &typedef.Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(2, "pk"),
				ClusteringKeys: createColumns(1, "ck"),
				Columns:        createColumns(2, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,pk1 text,ck0 text,col0 text,col1 text, PRIMARY KEY ((pk0,pk1), ck0))",
		},
		"multiple_partition_key_multiple_clustering_key": {
			table: &typedef.Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(2, "pk"),
				ClusteringKeys: createColumns(2, "ck"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,pk1 text,ck0 text,ck1 text, PRIMARY KEY ((pk0,pk1), ck0,ck1))",
		},
		"multiple_partition_key_multiple_clustering_key_single_column": {
			table: &typedef.Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(2, "pk"),
				ClusteringKeys: createColumns(2, "ck"),
				Columns:        createColumns(1, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,pk1 text,ck0 text,ck1 text,col0 text, PRIMARY KEY ((pk0,pk1), ck0,ck1))",
		},
		"multiple_partition_key_multiple_clustering_key_multiple_column": {
			table: &typedef.Table{
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
			got := generators.GetCreateTable(test.table, ks)
			if diff := cmp.Diff(got, test.want); diff != "" {
				t.Fatalf(diff)
			}
		})
	}
}

func createColumns(cnt int, prefix string) typedef.Columns {
	var cols typedef.Columns
	for i := 0; i < cnt; i++ {
		cols = append(cols, &typedef.ColumnDef{
			Name: generators.GenColumnName(prefix, i),
			Type: typedef.TYPE_TEXT,
		})
	}
	return cols
}
