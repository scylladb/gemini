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
package typedef

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/tableopts"
)

func TestSchemaConfigValidate(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		config *SchemaConfig
		want   error
	}{
		"empty": {
			config: &SchemaConfig{},
			want:   ErrSchemaConfigInvalidRangePK,
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
			want: ErrSchemaConfigInvalidRangePK,
		},
		"ck_missing": {
			config: &SchemaConfig{
				MaxPartitionKeys: 3,
				MinPartitionKeys: 2,
			},
			want: ErrSchemaConfigInvalidRangeCK,
		},
		"min_ck_gt_than_max_ck": {
			config: &SchemaConfig{
				MaxPartitionKeys:  3,
				MinPartitionKeys:  2,
				MaxClusteringKeys: 2,
				MinClusteringKeys: 3,
			},
			want: ErrSchemaConfigInvalidRangeCK,
		},
		"columns_missing": {
			config: &SchemaConfig{
				MaxPartitionKeys:  3,
				MinPartitionKeys:  2,
				MaxClusteringKeys: 3,
				MinClusteringKeys: 2,
			},
			want: ErrSchemaConfigInvalidRangeCols,
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
			want: ErrSchemaConfigInvalidRangeCols,
		},
	}
	cmp.AllowUnexported()
	for name := range tests {
		test := tests[name]
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			got := test.config.Valid()
			//nolint:errorlint
			if got != test.want {
				t.Fatalf("expected '%s', got '%s'", test.want, got)
			}
		})
	}
}

// TestSchemaMarshalUnmarshalNotChanged main task of this test catch all possible changes in json representation of schema and notify QA about this.
//
// If any change was catch and if you sure that this changes really needed - you should notify QA about this changes by create new issue https://github.com/scylladb/scylla-cluster-tests/issues/new
// Only then you can rewrite expected data file
func TestSchemaMarshalUnmarshalNotChanged(t *testing.T) {
	expectedFilePath := "./test_expected_data/full_schema.json"

	fullSchemaExpected, err := os.ReadFile(expectedFilePath)
	if err != nil {
		t.Fatalf("failed to open schema json file %s, error:%s", expectedFilePath, err)
	}

	fullSchemaMarshaled, err := json.MarshalIndent(fullSchema, "", "  ")
	if err != nil {
		t.Fatalf("unable to marshal schema json, error=%s\n", err)
	}

	if diff := cmp.Diff(fullSchemaExpected, fullSchemaMarshaled); diff != "" {
		t.Errorf("schema not the same after marshal, diff=%s", diff)
		t.Error("if you sure that this changes really needed - you should notify QA about this changes by create new issue https://github.com/scylladb/scylla-cluster-tests/issues/new\n" +
			"Only then you can rewrite expected data file")
	}

	fullSchemaExpectedUnmarshalled := Schema{}

	if err = json.Unmarshal(fullSchemaExpected, &fullSchemaExpectedUnmarshalled); err != nil {
		t.Fatalf("unable to marshal schema example json, error=%s\n", err)
	}

	opts := cmp.Options{
		cmp.AllowUnexported(Table{}, MaterializedView{}),
		cmpopts.IgnoreUnexported(Table{}, MaterializedView{}),
		cmpopts.EquateEmpty(),
	}

	fullSchema.Config = SchemaConfig{}
	if diff := cmp.Diff(fullSchema, fullSchemaExpectedUnmarshalled, opts); diff != "" {
		t.Errorf("schema not the same after unmarshal, diff=%s", diff)
		t.Error("if you sure that this changes really needed - you should notify QA about this changes by create new issue https://github.com/scylladb/scylla-cluster-tests/issues/new\n" +
			"Only then you can rewrite expected data file")
	}
}

// nolint: revive
var fullSchema = Schema{
	Keyspace: Keyspace{
		Replication:       replication.NewSimpleStrategy(),
		OracleReplication: replication.NewSimpleStrategy(),
		Name:              "ks1",
	},
	Tables: []*Table{
		{
			Name: "tb0",
			PartitionKeys: Columns{
				{Name: "pk0", Type: TYPE_ASCII},
				{Name: "pk1", Type: TYPE_BIGINT},
				{Name: "pk2", Type: TYPE_BLOB},
				{Name: "pk3", Type: TYPE_BOOLEAN},
				{Name: "pk4", Type: TYPE_DATE},
				{Name: "pk5", Type: TYPE_DECIMAL},
				{Name: "pk6", Type: TYPE_DOUBLE},
			},
			ClusteringKeys: Columns{
				{Name: "ck0", Type: TYPE_FLOAT},
				{Name: "ck1", Type: TYPE_INET},
				{Name: "ck2", Type: TYPE_INT},
				{Name: "ck3", Type: TYPE_SMALLINT},
				{Name: "ck4", Type: TYPE_TEXT},
				{Name: "ck5", Type: TYPE_TIMESTAMP},
				{Name: "ck6", Type: TYPE_TIMEUUID},
			},
			Columns: Columns{
				{Name: "col0", Type: TYPE_ASCII},
				{Name: "col1", Type: TYPE_BIGINT},
				{Name: "col2", Type: TYPE_BLOB},
				{Name: "col3", Type: TYPE_BOOLEAN},
				{Name: "col4", Type: TYPE_DATE},
				{Name: "col5", Type: TYPE_DECIMAL},
				{Name: "col6", Type: TYPE_DOUBLE},
				{Name: "col7", Type: TYPE_FLOAT},
				{Name: "col8", Type: TYPE_INET},
				{Name: "col9", Type: TYPE_INT},
				{Name: "col10", Type: TYPE_SMALLINT},
				{Name: "col11", Type: TYPE_TEXT},
				{Name: "col12", Type: TYPE_TIMESTAMP},
				{Name: "col13", Type: TYPE_TIMEUUID},
				{Name: "col14", Type: TYPE_TINYINT},
				{Name: "col15", Type: TYPE_UUID},
				{Name: "col16", Type: TYPE_VARCHAR},
				{Name: "col17", Type: TYPE_VARINT},
				{Name: "col18", Type: TYPE_TIME},
				{Name: "col19", Type: TYPE_DURATION},
			},
			Indexes: []IndexDef{
				{IndexName: "col0_idx", ColumnName: "col0"},
				{IndexName: "col12_idx", ColumnName: "col12"},
			},
			MaterializedViews: nil,
			KnownIssues:       map[string]bool{KnownIssuesJSONWithTuples: true},
			TableOptions:      []string{"compression = {'sstable_compression':'LZ4Compressor'}", "read_repair_chance = 1.0", "comment = 'Important records'", "cdc = {'enabled':'true','preimage':'true'}", "compaction = {'class':'LeveledCompactionStrategy','enabled':true,'sstable_size_in_mb':160,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2}"},
		}, {
			Name: "tb1",
			PartitionKeys: Columns{
				{Name: "pk0", Type: TYPE_FLOAT},
				{Name: "pk1", Type: TYPE_INET},
				{Name: "pk2", Type: TYPE_INT},
				{Name: "pk3", Type: TYPE_SMALLINT},
				{Name: "pk4", Type: TYPE_TEXT},
				{Name: "pk5", Type: TYPE_TIMESTAMP},
				{Name: "pk6", Type: TYPE_TIMEUUID},
			},
			ClusteringKeys: Columns{
				{Name: "ck0", Type: TYPE_ASCII},
				{Name: "ck1", Type: TYPE_BIGINT},
				{Name: "ck2", Type: TYPE_BLOB},
				{Name: "ck3", Type: TYPE_BOOLEAN},
				{Name: "ck4", Type: TYPE_DATE},
				{Name: "ck5", Type: TYPE_DECIMAL},
				{Name: "ck6", Type: TYPE_DOUBLE},
			},
			Columns: Columns{
				{Name: "col0", Type: TYPE_ASCII},
				{Name: "col1", Type: TYPE_BIGINT},
				{Name: "col2", Type: TYPE_BLOB},
				{Name: "col3", Type: TYPE_BOOLEAN},
				{Name: "col4", Type: TYPE_DATE},
				{Name: "col5", Type: TYPE_DECIMAL},
				{Name: "col6", Type: TYPE_DOUBLE},
				{Name: "col7", Type: TYPE_FLOAT},
				{Name: "col8", Type: TYPE_INET},
				{Name: "col9", Type: TYPE_INT},
				{Name: "col10", Type: TYPE_SMALLINT},
				{Name: "col11", Type: TYPE_TEXT},
				{Name: "col12", Type: TYPE_TIMESTAMP},
				{Name: "col13", Type: TYPE_TIMEUUID},
				{Name: "col14", Type: TYPE_TINYINT},
				{Name: "col15", Type: TYPE_UUID},
				{Name: "col16", Type: TYPE_VARCHAR},
				{Name: "col17", Type: TYPE_VARINT},
				{Name: "col18", Type: TYPE_TIME},
				{Name: "col19", Type: TYPE_DURATION},
			},
			Indexes: []IndexDef{
				{IndexName: "col0_idx", ColumnName: "col0"},
				{IndexName: "col12_idx", ColumnName: "col12"},
			},
			MaterializedViews: []MaterializedView{
				{
					NonPrimaryKey: &ColumnDef{
						Type: TYPE_DECIMAL,
						Name: "col5",
					},
					Name: "mv0",
					PartitionKeys: Columns{
						{Name: "pk0", Type: TYPE_FLOAT},
						{Name: "pk1", Type: TYPE_INET},
						{Name: "pk2", Type: TYPE_INT},
						{Name: "pk3", Type: TYPE_SMALLINT},
						{Name: "pk4", Type: TYPE_TEXT},
						{Name: "pk5", Type: TYPE_TIMESTAMP},
						{Name: "pk6", Type: TYPE_TIMEUUID},
					},
					ClusteringKeys: Columns{
						{Name: "ck0", Type: TYPE_ASCII},
						{Name: "ck1", Type: TYPE_BIGINT},
						{Name: "ck2", Type: TYPE_BLOB},
						{Name: "ck3", Type: TYPE_BOOLEAN},
						{Name: "ck4", Type: TYPE_DATE},
						{Name: "ck5", Type: TYPE_DECIMAL},
						{Name: "ck6", Type: TYPE_DOUBLE},
					},
				}, {
					NonPrimaryKey: &ColumnDef{
						Type: TYPE_ASCII,
						Name: "col0_idx",
					},
					Name: "mv1",
					PartitionKeys: Columns{
						{Name: "pk0", Type: TYPE_FLOAT},
						{Name: "pk1", Type: TYPE_INET},
						{Name: "pk2", Type: TYPE_INT},
						{Name: "pk3", Type: TYPE_SMALLINT},
						{Name: "pk4", Type: TYPE_TEXT},
						{Name: "pk5", Type: TYPE_TIMESTAMP},
						{Name: "pk6", Type: TYPE_TIMEUUID},
					},
					ClusteringKeys: Columns{
						{Name: "ck0", Type: TYPE_ASCII},
						{Name: "ck1", Type: TYPE_BIGINT},
						{Name: "ck2", Type: TYPE_BLOB},
						{Name: "ck3", Type: TYPE_BOOLEAN},
						{Name: "ck4", Type: TYPE_DATE},
						{Name: "ck5", Type: TYPE_DECIMAL},
						{Name: "ck6", Type: TYPE_DOUBLE},
					},
				},
			},
			KnownIssues:  map[string]bool{KnownIssuesJSONWithTuples: true},
			TableOptions: []string{"compression = {'sstable_compression':'LZ4Compressor'}", "read_repair_chance = 1.0", "comment = 'Important records'", "cdc = {'enabled':'true','preimage':'true'}", "compaction = {'class':'LeveledCompactionStrategy','enabled':true,'sstable_size_in_mb':160,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2}"},
		}, {
			Name: "tb2",
			PartitionKeys: Columns{
				{Name: "pk0", Type: TYPE_TINYINT},
				{Name: "pk1", Type: TYPE_UUID},
				{Name: "pk2", Type: TYPE_VARCHAR},
				{Name: "pk3", Type: TYPE_VARINT},
				{Name: "pk4", Type: TYPE_TIME},
			},
			ClusteringKeys: Columns{
				{Name: "ck0", Type: TYPE_TINYINT},
				{Name: "ck1", Type: TYPE_UUID},
				{Name: "ck2", Type: TYPE_VARCHAR},
				{Name: "ck3", Type: TYPE_VARINT},
				{Name: "ck4", Type: TYPE_TIME},
				{Name: "ck5", Type: TYPE_DURATION},
			},
			Columns: Columns{
				{Name: "col0", Type: &UDTType{
					ComplexType: "udt",
					ValueTypes:  map[string]SimpleType{"udt_10.1": TYPE_BIGINT, "udt_10.2": TYPE_DATE, "udt_10.3": TYPE_BLOB},
					TypeName:    "udt_10",
					Frozen:      false,
				}},
				{Name: "col1", Type: &MapType{
					ComplexType: "map",
					KeyType:     TYPE_INET,
					ValueType:   TYPE_TIME,
					Frozen:      true,
				}},
				{Name: "col2", Type: &TupleType{
					ComplexType: "tuple",
					ValueTypes:  []SimpleType{TYPE_FLOAT, TYPE_DATE, TYPE_VARCHAR},
					Frozen:      false,
				}},
				{Name: "col3", Type: &BagType{
					ComplexType: "list",
					ValueType:   TYPE_UUID,
					Frozen:      true,
				}},
				{Name: "col4", Type: &BagType{
					ComplexType: "set",
					ValueType:   TYPE_TIMESTAMP,
					Frozen:      false,
				}},
				{Name: "col5", Type: TYPE_DECIMAL},
			},
			Indexes: []IndexDef{
				{IndexName: "col0_idx", ColumnName: "col0"},
				{IndexName: "col12_idx", ColumnName: "col12"},
			},
			MaterializedViews: []MaterializedView{
				{
					NonPrimaryKey: nil,
					Name:          "mv0",
					PartitionKeys: Columns{
						{Name: "pk0", Type: TYPE_TINYINT},
						{Name: "pk1", Type: TYPE_UUID},
						{Name: "pk2", Type: TYPE_VARCHAR},
						{Name: "pk3", Type: TYPE_VARINT},
						{Name: "pk4", Type: TYPE_TIME},
					},
					ClusteringKeys: Columns{
						{Name: "ck0", Type: TYPE_TINYINT},
						{Name: "ck1", Type: TYPE_UUID},
						{Name: "ck2", Type: TYPE_VARCHAR},
						{Name: "ck3", Type: TYPE_VARINT},
						{Name: "ck4", Type: TYPE_TIME},
						{Name: "ck5", Type: TYPE_DURATION},
					},
				},
			},
			KnownIssues:  map[string]bool{KnownIssuesJSONWithTuples: true},
			TableOptions: []string{"compression = {'sstable_compression':'LZ4Compressor'}", "read_repair_chance = 1.0", "comment = 'Important records'", "cdc = {'enabled':'true','preimage':'true'}", "compaction = {'class':'LeveledCompactionStrategy','enabled':true,'sstable_size_in_mb':160,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2}"},
		},
	},
	Config: SchemaConfig{
		ReplicationStrategy:       replication.NewSimpleStrategy(),
		OracleReplicationStrategy: replication.NewSimpleStrategy(),
		TableOptions: tableopts.CreateTableOptions([]string{
			"compression = {'sstable_compression':'LZ4Compressor'}",
			"read_repair_chance = 1.0", "comment = 'Important records'", "cdc = {'enabled':'true','preimage':'true'}",
			"compaction = {'class':'LeveledCompactionStrategy','enabled':true,'sstable_size_in_mb':160,'tombstone_compaction_interval':86400,'tombstone_threshold':0.2}",
		}, nil),
		MaxTables:                        10,
		MaxPartitionKeys:                 10,
		MinPartitionKeys:                 1,
		MaxClusteringKeys:                10,
		MinClusteringKeys:                1,
		MaxColumns:                       25,
		MinColumns:                       1,
		MaxUDTParts:                      20,
		MaxTupleParts:                    20,
		MaxBlobLength:                    1e4,
		MaxStringLength:                  1000,
		MinBlobLength:                    0,
		MinStringLength:                  0,
		UseCounters:                      false,
		UseLWT:                           false,
		CQLFeature:                       CQL_FEATURE_NORMAL,
		AsyncObjectStabilizationAttempts: 10,
		AsyncObjectStabilizationDelay:    100000,
	},
}
