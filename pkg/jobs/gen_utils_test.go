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

package jobs

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/builders"
	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/routingkey"
	"github.com/scylladb/gemini/pkg/tableopts"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

type expectedStore struct {
	list     ExpectedList
	filePath string
	update   bool
	listLock sync.RWMutex
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

type funcOptions struct {
	addType  typedef.ColumnDef
	idxCount int
	mvNum    int
	useLWT   bool
	pkCount  int
	ckCount  int
	delNum   int
}

type Results []*Result

func initExpected(t *testing.T, filePath string, cases []string, updateExpected bool) *expectedStore {
	t.Helper()
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
	t.Helper()
	received := convertStmtsToResults(stmt)

	if f.update {
		f.listLock.Lock()
		f.list[caseName] = received
		f.listLock.Unlock()
		return
	}
	f.listLock.RLock()
	expected := f.list[caseName]
	f.listLock.RUnlock()
	if len(expected) != len(received) {
		t.Fatalf("error: len received = %d , len expected = %d are different", len(received), len(expected))
	}
	for idx, res := range expected {
		res.Diff(t, received[idx])
	}
}

func (f *expectedStore) updateExpected(t *testing.T) {
	t.Helper()
	if f.update {
		f.listLock.RLock()
		data, err := json.MarshalIndent(f.list, "", "  ")
		f.listLock.RUnlock()
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
	t.Helper()
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
	t.Helper()
	if expected == received {
		return
	}
	errMsgList := make([]string, 0)
	subString := " "
	if strings.Count(expected, ",\"") > strings.Count(expected, subString) {
		subString = ",\""
	}
	tmpExpected := strings.Split(expected, subString)
	tmpReceived := strings.Split(received, subString)
	switch len(tmpExpected) == len(tmpReceived) {
	case true:
		// Inject nice row that highlights differences if length is not changed
		expected, received = addDiffHighlight(tmpExpected, tmpReceived, subString)
		errMsgList = []string{
			errMsg,
			fmt.Sprintf("Expected   %s", expected),
			diffHighlightString([]rune(expected), []rune(received)),
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

func diffHighlightString(expected, received []rune) string {
	out := "Difference "
	for idx := range expected {
		if expected[idx] == received[idx] {
			out += " "
		} else {
			out += "↕"
		}
	}
	return out
}

func addDiffHighlight(expected, received []string, subString string) (string, string) {
	for idx := range expected {
		delta := len(expected[idx]) - len(received[idx])
		if delta > 0 {
			received[idx] += strings.Repeat("↔", delta)
		}
		if delta < 0 {
			expected[idx] += strings.Repeat("↔", -delta)
		}
	}
	return strings.Join(expected, subString), strings.Join(received, subString)
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
	token := ""
	tokenValues := ""
	if stmt.ValuesWithToken != nil {
		token = fmt.Sprintf("%v", stmt.ValuesWithToken.Token)
		tokenValues = strings.TrimSpace(fmt.Sprintf("%v", stmt.ValuesWithToken.Value))
	}
	return &Result{
		Token:       token,
		TokenValues: tokenValues,
		Query:       strings.TrimSpace(query),
		Names:       strings.TrimSpace(fmt.Sprintf("%s", names)),
		Values:      strings.TrimSpace(fmt.Sprintf("%v", stmt.Values)),
		Types:       types,
		QueryType:   fmt.Sprintf("%v", stmt.QueryType),
	}
}

func (r *Result) Diff(t *testing.T, received *Result) {
	t.Helper()
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

func getAllForTestStmt(t testInterface, caseName string) (*typedef.Schema, *typedef.PartitionRangeConfig, *MockGenerator, *rand.Rand, funcOptions) {
	rnd := rand.New(nonRandSource(1))
	table, options, optionsNum := getTableAndOptionsFromName(t, caseName)
	opts, mv, indexes := getFromOptions(t, table, options, optionsNum)
	table.Indexes = indexes
	if opts.mvNum >= 0 {
		table.MaterializedViews = []typedef.MaterializedView{*mv}
	}
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

	testGenerator := NewTestGenerator(testSchema.Tables[0], rnd, testPRC, &routingkey.Creator{})

	return testSchema, testPRC, testGenerator, rnd, opts
}

func createMv(t testInterface, table *typedef.Table, haveNonPrimaryKey bool) *typedef.MaterializedView {
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

func genTestSchema(sc typedef.SchemaConfig, table *typedef.Table) *typedef.Schema {
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

func getTableAndOptionsFromName(t testInterface, tableName string) (table *typedef.Table, options, optionsNum string) {
	nameParts := strings.Split(tableName, "_")
	table = &typedef.Table{}
	for idx := range nameParts {
		switch idx {
		case 0:
			table.PartitionKeys = genColumnsFromCase(t, partitionKeysCases, nameParts[0], "pk")
		case 1:
			table.ClusteringKeys = genColumnsFromCase(t, clusteringKeysCases, nameParts[1], "ck")
		case 2:
			table.Columns = genColumnsFromCase(t, columnsCases, nameParts[2], "col")
		case 3:
			options = nameParts[3]
		case 4:
			optionsNum = nameParts[4]
		}
	}
	table.Name = tableName

	return table, options, optionsNum
}

func getFromOptions(t testInterface, table *typedef.Table, option, optionsNum string) (funcOptions, *typedef.MaterializedView, []typedef.IndexDef) {
	funcOpts := funcOptions{
		mvNum: -1,
	}
	var mv *typedef.MaterializedView
	var indexes []typedef.IndexDef
	if option == "" {
		return funcOpts, nil, nil
	}
	options := strings.Split(option, ".")
	for i := range options {
		_, haveOpt := optionsCases[options[i]]
		if !haveOpt {
			t.Fatalf("Error in getTableAndOptionsFromName OptCaseName:%s, not found", options[i])
		}
		switch options[i] {
		case "lwt":
			funcOpts.useLWT = true
		case "mv":
			funcOpts.mvNum = 0
			mv = createMv(t, table, false)
		case "mvNp":
			funcOpts.mvNum = 0
			mv = createMv(t, table, true)
		case "cpk1":
			funcOpts.pkCount = 1
		case "cpkAll":
			funcOpts.pkCount = len(table.PartitionKeys)
			if funcOpts.pkCount == 0 {
				t.Fatalf("wrong pk case definition")
			}
		case "cck1":
			funcOpts.ckCount = 0
		case "cckAll":
			funcOpts.ckCount = len(table.ClusteringKeys) - 1
			if funcOpts.ckCount < 0 {
				t.Fatalf("wrong ck case definition")
			}
		case "idx1":
			indexes = createIdxFromColumns(t, table, false)
			funcOpts.idxCount = 1
		case "idxAll":
			indexes = createIdxFromColumns(t, table, true)
			funcOpts.idxCount = len(indexes)
		case "delFist":
			funcOpts.delNum = 0
		case "delLast":
			funcOpts.delNum = len(table.Columns) - 1
		case "addSt":
			funcOpts.addType = typedef.ColumnDef{
				Type: createColumnSimpleType(t, optionsNum),
				Name: generators.GenColumnName("col", len(table.Columns)+1),
			}
		}

	}
	return funcOpts, mv, indexes
}

func createColumnSimpleType(t testInterface, typeNum string) typedef.SimpleType {
	num, err := strconv.ParseInt(typeNum, 0, 8)
	if err != nil {
		t.Fatalf("wrong options case for add column definition")
	}
	return typedef.AllTypes[int(num)]
}

func createIdxFromColumns(t testInterface, table *typedef.Table, all bool) (indexes []typedef.IndexDef) {
	if len(table.Columns) < 1 {
		t.Fatalf("wrong idxCount case definition")
	}
	switch all {
	case true:
		for i := range table.Columns {
			index := typedef.IndexDef{
				IndexName:  table.Columns[i].Name + "_idx",
				ColumnName: table.Columns[i].Name,
				Column:     table.Columns[i],
			}
			indexes = append(indexes, index)
		}
	default:
		index := typedef.IndexDef{
			IndexName:  table.Columns[0].Name + "_idx",
			ColumnName: table.Columns[0].Name,
			Column:     table.Columns[0],
		}
		indexes = append(indexes, index)

	}
	return indexes
}

func genColumnsFromCase(t testInterface, typeCases map[string][]typedef.Type, caseName, prefix string) typedef.Columns {
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

func RunStmtTest(t *testing.T, filePath string, cases []string, testBody func(subT *testing.T, caseName string, expected *expectedStore)) {
	t.Helper()
	utils.SetUnderTest()
	t.Parallel()
	expected := initExpected(t, filePath, cases, *updateExpected)
	if *updateExpected {
		t.Cleanup(func() {
			expected.updateExpected(t)
		})
	}
	for idx := range cases {
		caseName := cases[idx]
		t.Run(caseName,
			func(subT *testing.T) {
				subT.Parallel()
				testBody(subT, caseName, expected)
			})
	}
}
