package gemini

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

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
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text, PRIMARY KEY (pk0))",
		},
		"single_partition_key_single_column": {
			table: &Table{
				Name:          "tbl0",
				PartitionKeys: createColumns(1, "pk"),
				Columns:       createColumns(1, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,col0 text, PRIMARY KEY (pk0))",
		},
		"single_partition_key_multiple_column": {
			table: &Table{
				Name:          "tbl0",
				PartitionKeys: createColumns(1, "pk"),
				Columns:       createColumns(2, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,col0 text,col1 text, PRIMARY KEY (pk0))",
		},
		"multiple_partition_key_multiple_column": {
			table: &Table{
				Name:          "tbl0",
				PartitionKeys: createColumns(2, "pk"),
				Columns:       createColumns(2, "col"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,pk1 text,col0 text,col1 text, PRIMARY KEY (pk0,pk1))",
		},
		"single_partition_key_single_clustering_key": {
			table: &Table{
				Name:           "tbl0",
				PartitionKeys:  createColumns(1, "pk"),
				ClusteringKeys: createColumns(1, "ck"),
			},
			want: "CREATE TABLE IF NOT EXISTS ks1.tbl0 (pk0 text,ck0 text, PRIMARY KEY ((pk0), ck0))",
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
