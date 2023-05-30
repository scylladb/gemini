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
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/builders"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/routingkey"
	"github.com/scylladb/gemini/pkg/tableopts"
	"github.com/scylladb/gemini/pkg/testschema"
	"github.com/scylladb/gemini/pkg/typedef"
)

type expectedStore struct {
	list     ExpectedList
	filePath string
	update   bool
}

// Result description:
type Result struct {
	Token       string
	TokenValues string
	Query       string
	Names       string
	Values      string
	Types       string
	QueryType   string
}

type Results []*Result

func initExpected(t *testing.T, fileName string, cases []string, updateExpected bool) *expectedStore {
	filePath := path.Join(testDirPath, fileName)
	expected := make(ExpectedList)
	if updateExpected {
		expected.addCases(cases...)
	} else {
		err := expected.loadExpectedFromFile(filePath)
		if err != nil {
			t.Fatal(err.Error())
		}
		err = expected.checkCasesExisting(cases)
		if err != nil {
			t.Fatal(err.Error())
		}
	}
	return &expectedStore{filePath: filePath, list: expected, update: updateExpected}
}

func (f *expectedStore) CompareOrStore(t *testing.T, caseName string, stmt interface{}) {
	received := convertStmtsToResults(stmt)

	if f.update {
		f.list[caseName] = received
		return
	}
	expected := f.list[caseName]
	if len(expected) != len(received) {
		t.Fatalf("error: len received = %d , len expected = %d are different", len(received), len(expected))
	}
	for idx, res := range expected {
		res.Diff(t, received[idx])
	}
}

func (f *expectedStore) updateExpected(t *testing.T) {
	if f.update {
		data, err := json.MarshalIndent(f.list, "", "  ")
		if err != nil {
			t.Fatalf("Marshal funcStmtTests error:%v", err)
		}
		err = os.WriteFile(f.filePath, data, 0644)
		if err != nil {
			t.Fatalf("write to file %s error:%v", f.filePath, err)
		}
	}
}

func validateStmt(t *testing.T, stmt interface{}, err error) {
	if err != nil {
		t.Fatalf("error: get an error on create test inputs:%v", err)
	}
	if stmt == nil {
		t.Fatalf("error: stmt is nil")
	}
	switch stmts := stmt.(type) {
	case *typedef.Stmts:
		if stmts == nil || stmts.List == nil || len(stmts.List) == 0 {
			t.Fatalf("error: stmts is empty")
		}
		for i := range stmts.List {
			if stmts.List[i] == nil || stmts.List[i].Query == nil {
				t.Fatalf("error: stmts has nil stmt #%d", i)
			}
		}
	case *typedef.Stmt:
		if stmts == nil || stmts.Query == nil {
			t.Fatalf("error: stmt is empty")
		}
	default:
		t.Fatalf("error: unkwon type of stmt")
	}
}

func getErrorMsgIfDifferent(t *testing.T, expected, received, errMsg string) {
	if expected == received {
		return
	}
	errMsgList := make([]string, 0)
	switch len(expected) == len(received) {
	case true:
		// Inject nice row that highlights differences if length is not changed
		errMsgList = []string{
			errMsg,
			fmt.Sprintf("Expected   %s", expected),
			"           " + diffHighlightString(expected, received),
			fmt.Sprintf("Received   %s", received),
			"-------------------------------------------",
		}
	case false:
		errMsgList = []string{
			errMsg,
			fmt.Sprintf("Expected   %s", expected),
			fmt.Sprintf("Received   %s", received),
			"-------------------------------------------",
		}
	}
	t.Error(strings.Join(errMsgList, "\n"))
}

func diffHighlightString(expected, received string) string {
	out := ""
	for idx := range expected {
		if expected[idx] == received[idx] {
			out = out + " "
		} else {
			out = out + "↕"
		}
	}
	return out
}

func convertStmtsToResults(stmt interface{}) Results {
	var out Results
	switch stmts := stmt.(type) {
	case *typedef.Stmts:
		for idx := range stmts.List {
			out = append(out, convertStmtToResults(stmts.List[idx]))
		}
	case *typedef.Stmt:
		out = append(out, convertStmtToResults(stmts))

	}
	return out
}

func convertStmtToResults(stmt *typedef.Stmt) *Result {
	types := ""
	for idx := range stmt.Types {
		types = fmt.Sprintf("%s %s", types, stmt.Types[idx].Name())
	}
	query, names := stmt.Query.ToCql()
	return &Result{
		Token:       fmt.Sprintf("%v", (*stmt).ValuesWithToken.Token),
		TokenValues: strings.TrimSpace(fmt.Sprintf("%v", (*stmt).ValuesWithToken.Value)),
		Query:       strings.TrimSpace(query),
		Names:       strings.TrimSpace(fmt.Sprintf("%s", names)),
		Values:      strings.TrimSpace(fmt.Sprintf("%v", stmt.Values)),
		Types:       types,
		QueryType:   fmt.Sprintf("%v", stmt.QueryType),
	}
}

func (r *Result) Diff(t *testing.T, received *Result) {
	getErrorMsgIfDifferent(t, r.Token, received.Token, " error: value stmt.ValuesWithToken.Token expected and received are different:")
	getErrorMsgIfDifferent(t, r.TokenValues, received.TokenValues, " error: value stmt.ValuesWithToken.Value expected and received are different:")
	getErrorMsgIfDifferent(t, r.Query, received.Query, " error: value stmt.Query.ToCql().stmt expected and received are different:")
	getErrorMsgIfDifferent(t, r.Names, received.Names, " error: value stmt.Query.ToCql().Names expected and received are different:")
	getErrorMsgIfDifferent(t, r.Values, received.Values, " error: value stmt.Values expected and received are different:")
	getErrorMsgIfDifferent(t, r.Types, received.Types, " error: value stmt.Types expected and received are different:")
	getErrorMsgIfDifferent(t, r.Values, received.Values, " error: value stmt.Values expected and received are different:")
	getErrorMsgIfDifferent(t, r.QueryType, received.QueryType, " error: value stmt.QueryType expected and received are different:")
}

type ExpectedList map[string]Results

func (e *ExpectedList) checkCasesExisting(cases []string) error {
	for idx := range cases {
		exp, ok := (*e)[cases[idx]]
		if !ok || (&exp) == (&Results{}) {
			return errors.Errorf("expected for case %s not found", cases[idx])
		}
	}
	return nil
}

func (e *ExpectedList) addCases(cases ...string) {
	for idx := range cases {
		(*e)[cases[idx]] = Results{}
	}
}

func (e *ExpectedList) loadExpectedFromFile(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return errors.Wrapf(err, "failed to open file %s", filePath)
	}
	err = json.Unmarshal(data, e)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal expected from file %s", filePath)
	}
	return nil
}

type nonRandSource uint64

func (s nonRandSource) Uint64() uint64 {
	return uint64(s)
}

func (s nonRandSource) Seed(uint64) {
}

type testInterface interface {
	Errorf(format string, args ...any)
	Fatalf(format string, args ...any)
}

func getAllForTestStmt(t testInterface, caseName string) (*testschema.Schema, *typedef.PartitionRangeConfig, *MockGenerator, *rand.Rand, bool, bool) {
	rnd := rand.New(nonRandSource(1))
	table, useLWT, useMV := getTableAndOptionsFromName(t, caseName)

	testSchema, testSchemaCfg, err := getTestSchema(table)
	if err != nil {
		t.Errorf("getTestSchema error:%v", err)
	}

	testPRC := &typedef.PartitionRangeConfig{
		MaxBlobLength:   testSchemaCfg.MaxBlobLength,
		MinBlobLength:   testSchemaCfg.MinBlobLength,
		MaxStringLength: testSchemaCfg.MaxStringLength,
		MinStringLength: testSchemaCfg.MinStringLength,
		UseLWT:          testSchemaCfg.UseLWT,
	}

	testGenerator := NewTestGenerator(testSchema.Tables[0], rnd, testPRC, &routingkey.RoutingKeyCreator{})

	return testSchema, testPRC, testGenerator, rnd, useLWT, useMV
}

func getTestSchema(table *testschema.Table) (*testschema.Schema, *typedef.SchemaConfig, error) {
	tableOpt := createTableOptions("compaction = {'class':'LeveledCompactionStrategy','enabled':true,'tombstone_threshold':0.2," +
		"'tombstone_compaction_interval':86400,'sstable_size_in_mb':160}")
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

func genTestSchema(sc typedef.SchemaConfig, table *testschema.Table) *testschema.Schema {
	builder := builders.NewSchemaBuilder()
	keyspace := typedef.Keyspace{
		Name:              "ks1",
		Replication:       sc.ReplicationStrategy,
		OracleReplication: sc.OracleReplicationStrategy,
	}
	builder.Keyspace(keyspace)
	builder.Table(table)
	return builder.Build()
}

func getTableAndOptionsFromName(t testInterface, tableName string) (*testschema.Table, bool, bool) {
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

func genColumnsFromCase(t testInterface, typeCases map[string][]typedef.Type, caseName, prefix string) testschema.Columns {
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
