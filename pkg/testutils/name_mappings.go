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

package testutils

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/scylladb/gemini/pkg/builders"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/routingkey"
	"github.com/scylladb/gemini/pkg/tableopts"
	"github.com/scylladb/gemini/pkg/typedef"
)

type FuncOptions struct {
	AddType  typedef.ColumnDef
	IdxCount int
	UseLWT   bool
	PkCount  int
	CkCount  int
	DelNum   int
}

var (
	partitionKeysCases = map[string][]typedef.Type{
		"pk1": {typedef.TypeBigint},
		"pk3": {typedef.TypeBigint, typedef.TypeFloat, typedef.TypeInet},
		"pkAll": {
			typedef.TypeAscii, typedef.TypeBigint, typedef.TypeBlob, typedef.TypeBoolean, typedef.TypeDate,
			typedef.TypeDecimal, typedef.TypeDouble, typedef.TypeFloat,
			typedef.TypeInet, typedef.TypeInt, typedef.TypeSmallint, typedef.TypeText, typedef.TypeTimestamp,
			typedef.TypeTimeuuid, typedef.TypeTinyint, typedef.TypeUuid, typedef.TypeVarchar, typedef.TypeVarint, typedef.TypeTime,
		},
	}

	clusteringKeysCases = map[string][]typedef.Type{
		"ck0": {},
		"ck1": {typedef.TypeDate},
		"ck3": {typedef.TypeAscii, typedef.TypeDate, typedef.TypeDecimal},
		"ckAll": {
			typedef.TypeAscii, typedef.TypeBigint, typedef.TypeBlob, typedef.TypeBoolean, typedef.TypeDate,
			typedef.TypeDecimal, typedef.TypeDouble, typedef.TypeFloat,
			typedef.TypeInet, typedef.TypeInt, typedef.TypeSmallint, typedef.TypeText, typedef.TypeTimestamp,
			typedef.TypeTimeuuid, typedef.TypeTinyint, typedef.TypeUuid, typedef.TypeVarchar, typedef.TypeVarint, typedef.TypeTime,
		},
	}

	columnsCases = map[string][]typedef.Type{
		"col0": {},
		"col1": {typedef.TypeDate},
		"col5": {
			typedef.TypeAscii,
			typedef.TypeDate,
			typedef.TypeBlob,
			typedef.TypeBigint,
			typedef.TypeFloat,
		},
		"col5c":  {typedef.TypeAscii, &mapType, typedef.TypeBlob, &tupleType, typedef.TypeFloat},
		"col1cr": {&counterType},
		"col3cr": {&counterType, &counterType, &counterType},
		"colAll": {
			typedef.TypeDuration, typedef.TypeAscii, typedef.TypeBigint, typedef.TypeBlob, typedef.TypeBoolean,
			typedef.TypeDate, typedef.TypeDecimal, typedef.TypeDouble, typedef.TypeFloat,
			typedef.TypeInet, typedef.TypeInt, typedef.TypeSmallint, typedef.TypeText, typedef.TypeTimestamp,
			typedef.TypeTimeuuid, typedef.TypeTinyint, typedef.TypeUuid, typedef.TypeVarchar, typedef.TypeVarint, typedef.TypeTime,
		},
	}

	counterType typedef.CounterType
	tupleType   typedef.TupleType
	mapType     typedef.MapType

	UpdateExpectedFlag = flag.Bool("update-expected", false, "make test to update expected results")
)

type testInterface interface {
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
}

func GetAllForTestStmt(
	t testInterface,
	testCaseName string,
) (*typedef.Schema, *MockGenerator, *rand.Rand) {
	rnd := rand.New(NonRandSource(1))
	table := GetTableFromName(t, testCaseName)
	testSchema, testSchemaCfg, err := getTestSchema(table)
	if err != nil {
		t.Errorf("getTestSchema error:%v", err)
	}

	testPRC := testSchemaCfg.GetPartitionRangeConfig()

	testGenerator := NewTestGenerator(testSchema.Tables[0], rnd, &testPRC, &routingkey.Creator{})

	return testSchema, testGenerator, rnd
}

func createMv(
	t testInterface,
	table *typedef.Table,
	haveNonPrimaryKey bool,
) *typedef.MaterializedView {
	switch haveNonPrimaryKey {
	case true:
		var cols typedef.Columns
		col := table.Columns.ValidColumnsForPrimaryKey()
		if len(col) == 0 {
			t.Fatalf("no valid columns for mv primary key")
		}
		cols = append(cols, col[0])
		return &typedef.MaterializedView{
			Name:           fmt.Sprintf("%s_mv_1", table.Name),
			PartitionKeys:  append(cols, table.PartitionKeys...),
			ClusteringKeys: table.ClusteringKeys,
			NonPrimaryKey:  col[0],
		}
	default:
		return &typedef.MaterializedView{
			Name:           fmt.Sprintf("%s_mv_1", table.Name),
			PartitionKeys:  table.PartitionKeys,
			ClusteringKeys: table.ClusteringKeys,
		}
	}
}

func getTestSchema(table *typedef.Table) (*typedef.Schema, *typedef.SchemaConfig, error) {
	tableOpt := createTableOptions(
		"compaction = {'class':'LeveledCompactionStrategy','enabled':true,'tombstone_threshold':0.2," +
			"'tombstone_compaction_interval':86400,'sstable_size_in_mb':160}",
	)
	testSchemaConfig := typedef.SchemaConfig{
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

	tableOptions := make([]tableopts.Option, 0, len(opts))

	for _, optionString := range opts {
		o, err := tableopts.FromCQL(optionString)
		if err != nil {
			continue
		}
		tableOptions = append(tableOptions, o)
	}
	return tableOptions
}

func genTestSchema(sc typedef.SchemaConfig, table *typedef.Table) *typedef.Schema {
	builder := builders.NewSchemaBuilder()
	builder.Config(sc)
	keyspace := typedef.Keyspace{
		Name:              "ks1",
		Replication:       sc.ReplicationStrategy,
		OracleReplication: sc.OracleReplicationStrategy,
	}
	builder.Keyspace(keyspace)
	builder.Table(table)
	return builder.Build()
}

func GetTableFromName(t testInterface, testCaseName string) *typedef.Table {
	table := typedef.Table{Name: testCaseName}
	nameParts := strings.Split(GetTableCaseNameFromCaseName(testCaseName), "_")
	for _, chunk := range nameParts {
		switch {
		case strings.HasPrefix(chunk, "pk"):
			table.PartitionKeys = genColumnsFromCase(t, partitionKeysCases, chunk, "pk")
		case strings.HasPrefix(chunk, "ck"):
			table.ClusteringKeys = genColumnsFromCase(t, clusteringKeysCases, chunk, "ck")
		case strings.HasPrefix(chunk, "col"):
			table.Columns = genColumnsFromCase(t, columnsCases, chunk, "col")
		case chunk == "idx1":
			table.Indexes = createIndexForColumns(t, table.Columns[0])
		case chunk == "idxAll":
			table.Indexes = createIndexForColumns(t, table.Columns...)
		case chunk == "mv":
			table.MaterializedViews = append(table.MaterializedViews, *createMv(t, &table, false))
		case chunk == "mvNp":
			table.MaterializedViews = append(table.MaterializedViews, *createMv(t, &table, true))
		}
	}
	return &table
}

type TestCaseOptions []string

func (o TestCaseOptions) GetBool(name string) bool {
	for _, optionName := range o {
		if name == optionName {
			return true
		}
	}
	return false
}

func (o TestCaseOptions) GetString(name string) string {
	for _, optionName := range o {
		if strings.HasPrefix(optionName, name) {
			return optionName
		}
	}
	return ""
}

func (o TestCaseOptions) HandleOption(name string, handler func(option string)) {
	for _, optionName := range o {
		if strings.HasPrefix(optionName, name) {
			handler(optionName)
		}
	}
}

func SplitCaseName(caseName string) (string, TestCaseOptions) {
	chunks := strings.Split(caseName, ".")
	if len(chunks) == 1 {
		return chunks[0], nil
	}
	return chunks[0], chunks[1:]
}

func GetTableCaseNameFromCaseName(caseName string) string {
	tableCaseName, _ := SplitCaseName(caseName)
	return tableCaseName
}

func GetOptionsFromCaseName(caseName string) TestCaseOptions {
	_, options := SplitCaseName(caseName)
	return options
}

func createIndexForColumns(
	t testInterface,
	columns ...*typedef.ColumnDef,
) (indexes []typedef.IndexDef) {
	if len(columns) < 1 {
		t.Fatalf("wrong IdxCount case definition")
	}
	for i := range columns {
		index := typedef.IndexDef{
			IndexName:  columns[i].Name + "_idx",
			ColumnName: columns[i].Name,
			Column:     columns[i],
		}
		indexes = append(indexes, index)
	}
	return indexes
}

func genColumnsFromCase(
	t testInterface,
	typeCases map[string][]typedef.Type,
	caseName, prefix string,
) typedef.Columns {
	typeCase, ok := typeCases[caseName]
	if !ok {
		t.Fatalf("Error caseName:%s, not found", caseName)
	}
	columns := make(typedef.Columns, 0, len(typeCase))
	for idx := range typeCase {
		columns = append(columns,
			&typedef.ColumnDef{
				Type: typeCase[idx],
				Name: fmt.Sprintf("%s%d", prefix, idx),
			})
	}
	return columns
}
