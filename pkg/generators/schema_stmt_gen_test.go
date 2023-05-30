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

package generators

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/builders"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/routingkey"
	"github.com/scylladb/gemini/pkg/tableopts"
	"github.com/scylladb/gemini/pkg/testschema"
	. "github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

type nonRandSource uint64

func (s nonRandSource) Uint64() uint64 {
	return uint64(s)
}

func (s nonRandSource) Seed(uint64) {
}

func getAllForTestStmt(t *testing.T, caseName string) (*testschema.Schema, *PartitionRangeConfig, *MockGenerator, *rand.Rand, bool, bool) {
	utils.SetTestUUIDFromTime()
	rnd := rand.New(nonRandSource(1))
	table, useLWT, useMV := getTableAndOptionsFromName(t, caseName)

	testSchema, testSchemaCfg, err := getTestSchema(table)
	if err != nil {
		t.Errorf("getTestSchema error:%v", err)
	}

	testPRC := &PartitionRangeConfig{
		MaxBlobLength:   testSchemaCfg.MaxBlobLength,
		MinBlobLength:   testSchemaCfg.MinBlobLength,
		MaxStringLength: testSchemaCfg.MaxStringLength,
		MinStringLength: testSchemaCfg.MinStringLength,
		UseLWT:          testSchemaCfg.UseLWT,
	}

	testGenerator := NewTestGenerator(testSchema.Tables[0], rnd, testPRC, &routingkey.RoutingKeyCreator{})

	return testSchema, testPRC, testGenerator, rnd, useLWT, useMV
}

func getTestSchema(table *testschema.Table) (*testschema.Schema, *SchemaConfig, error) {
	tableOpt := createTableOptions("compaction = {'class':'LeveledCompactionStrategy','enabled':true,'tombstone_threshold':0.2," +
		"'tombstone_compaction_interval':86400,'sstable_size_in_mb':160}")
	testSchemaConfig := SchemaConfig{
		ReplicationStrategy:              replication.NewSimpleStrategy(),
		OracleReplicationStrategy:        replication.NewSimpleStrategy(),
		TableOptions:                     tableOpt,
		MaxTables:                        1,
		MaxPartitionKeys:                 40,
		MinPartitionKeys:                 1,
		MaxClusteringKeys:                40,
		MinClusteringKeys:                0,
		MaxColumns:                       40,
		MinColumns:                       0,
		MaxUDTParts:                      2,
		MaxTupleParts:                    2,
		MaxBlobLength:                    20,
		MinBlobLength:                    1,
		MaxStringLength:                  20,
		MinStringLength:                  1,
		UseCounters:                      false,
		UseLWT:                           false,
		CQLFeature:                       2,
		AsyncObjectStabilizationAttempts: 10,
		AsyncObjectStabilizationDelay:    10 * time.Millisecond,
	}

	testSchema := genTestSchema(testSchemaConfig, table)
	return testSchema, &testSchemaConfig, nil
}

func createTableOptions(cql string) []tableopts.Option {
	opt, _ := tableopts.FromCQL(cql)
	opts := []string{opt.ToCQL()}
	var tableOptions []tableopts.Option

	for _, optionString := range opts {
		o, err := tableopts.FromCQL(optionString)
		if err != nil {
			continue
		}
		tableOptions = append(tableOptions, o)
	}
	return tableOptions
}

func genTestSchema(sc SchemaConfig, table *testschema.Table) *testschema.Schema {
	builder := builders.NewSchemaBuilder()
	keyspace := Keyspace{
		Name:              "ks1",
		Replication:       sc.ReplicationStrategy,
		OracleReplication: sc.OracleReplicationStrategy,
	}
	builder.Keyspace(keyspace)
	builder.Table(table)
	return builder.Build()
}

func getTableAndOptionsFromName(t *testing.T, tableName string) (*testschema.Table, bool, bool) {
	nameParts := strings.Split(tableName, "_")
	var table testschema.Table
	var useLWT, useMV bool
	for idx := range nameParts {
		switch idx {
		case 0:
			table.PartitionKeys = genColumnsFromCase(t, partitionKeysCases, nameParts[0], "pk")
		case 1:
			table.ClusteringKeys = genColumnsFromCase(t, clusteringKeysCases, nameParts[1], "ck")
		case 2:
			table.Columns = genColumnsFromCase(t, columnsCases, nameParts[2], "col")
		case 3:
			opt, haveOpt := optionsCases[nameParts[3]]
			if !haveOpt {
				t.Fatalf("Error in getTableAndOptionsFromName OptCaseName:%s, not found", nameParts[3])
			}
			for i := range opt {
				switch opt[i] {
				case "lwt":
					useLWT = true
				case "mv":
					useMV = true
				}
			}
		}
	}
	table.Name = tableName

	return &table, useLWT, useMV
}

func genColumnsFromCase(t *testing.T, typeCases map[string][]Type, caseName, prefix string) testschema.Columns {
	typeCase, ok := typeCases[caseName]
	if !ok {
		t.Fatalf("Error caseName:%s, not found", caseName)
	}
	columns := make(testschema.Columns, 0, len(typeCase))
	for idx := range typeCase {
		columns = append(columns,
			&testschema.ColumnDef{
				Type: typeCase[idx],
				Name: fmt.Sprintf("%s%d", prefix, idx),
			})
	}
	return columns
}
