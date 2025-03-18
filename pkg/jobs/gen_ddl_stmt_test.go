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

//nolint:thelper
package jobs

import (
	"fmt"
	"path"
	"strconv"
	"testing"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
)

var ddlDataPath = "./test_expected_data/ddl/"

func TestGenDropColumnStmt(t *testing.T) {
	RunStmtTest[results](t, path.Join(ddlDataPath, "drop_column.json"), genDropColumnStmtCases, func(subT *testing.T, caseName string, expected *testutils.ExpectedStore[results]) {
		schema, _, _ := testutils.GetAllForTestStmt(subT, caseName)
		options := testutils.GetOptionsFromCaseName(caseName)
		columnToDelete := getColumnToDeleteFromOptions(options, schema.Tables[0].Columns)
		stmt, err := genDropColumnStmt(schema.Tables[0], schema.Keyspace.Name, columnToDelete)
		validateStmt(subT, stmt, err)
		expected.CompareOrStore(subT, caseName, convertStmtsToResults(stmt))
	})
}

func TestGenAddColumnStmt(t *testing.T) {
	RunStmtTest[results](t, path.Join(ddlDataPath, "add_column.json"), genAddColumnStmtCases, func(subT *testing.T, caseName string, expected *testutils.ExpectedStore[results]) {
		schema, _, _ := testutils.GetAllForTestStmt(subT, caseName)
		options := testutils.GetOptionsFromCaseName(caseName)
		columnToAdd := getColumnToAddFromOptions(options, len(schema.Tables[0].Columns))
		stmt, err := genAddColumnStmt(schema.Tables[0], schema.Keyspace.Name, columnToAdd)
		validateStmt(subT, stmt, err)
		expected.CompareOrStore(subT, caseName, convertStmtsToResults(stmt))
	})
}

func BenchmarkGenDropColumnStmt(t *testing.B) {

	for idx := range genDropColumnStmtCases {
		caseName := genDropColumnStmtCases[idx]
		t.Run(caseName,
			func(subT *testing.B) {
				schema, _, _ := testutils.GetAllForTestStmt(subT, caseName)
				options := testutils.GetOptionsFromCaseName(caseName)
				columnToDelete := getColumnToDeleteFromOptions(options, schema.Tables[0].Columns)
				subT.ResetTimer()
				for x := 0; x < subT.N; x++ {
					_, _ = genDropColumnStmt(schema.Tables[0], schema.Keyspace.Name, columnToDelete)
				}
			})
	}
}

func BenchmarkGenAddColumnStmt(t *testing.B) {

	for idx := range genAddColumnStmtCases {
		caseName := genAddColumnStmtCases[idx]
		t.Run(caseName,
			func(subT *testing.B) {
				schema, _, _ := testutils.GetAllForTestStmt(subT, caseName)
				options := testutils.GetOptionsFromCaseName(caseName)
				columnToAdd := getColumnToAddFromOptions(options, len(schema.Tables[0].Columns))
				subT.ResetTimer()
				for x := 0; x < subT.N; x++ {
					_, _ = genAddColumnStmt(schema.Tables[0], schema.Keyspace.Name, columnToAdd)
				}
			})
	}
}

func getColumnToDeleteFromOptions(options testutils.TestCaseOptions, columns typedef.Columns) *typedef.ColumnDef {
	optVal := options.GetString("del")
	switch optVal {
	case "delFist":
		return columns[0]
	case "delLast":
		return columns[len(columns)-1]
	default:
		panic(fmt.Sprintf("unexpected value %s", optVal))
	}
}

func getColumnToAddFromOptions(options testutils.TestCaseOptions, columnsLen int) *typedef.ColumnDef {
	optVal := options.GetString("addSt")
	if optVal == "" || len(optVal) <= 5 {
		panic(fmt.Sprintf("wrong addSt parameter '%s'", optVal))
	}
	typeNum, err := strconv.Atoi(optVal[5:])
	if err != nil {
		panic(fmt.Sprintf("wrong addSt parameter '%s'", optVal))
	}
	return &typedef.ColumnDef{
		Type: typedef.AllTypes[typeNum],
		Name: generators.GenColumnName("col", columnsLen+1),
	}
}
