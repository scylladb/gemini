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
	"path"
	"strconv"
	"strings"
	"testing"

	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

var ddlDataPath = "./test_expected_data/ddl/"

func TestGenDropColumnStmt(t *testing.T) {
	RunStmtTest(t, path.Join(ddlDataPath, "drop_column.json"), genDropColumnStmtCases, func(subT *testing.T, caseName string, expected *expectedStore) {
		schema, _, _, _, opts := getAllForTestStmt(subT, caseName)
		stmt, err := genDropColumnStmt(schema.Tables[0], schema.Keyspace.Name, schema.Tables[0].Columns[opts.delNum])
		validateStmt(subT, stmt, err)
		expected.CompareOrStore(subT, caseName, stmt)
	})
}

func TestGenAddColumnStmt(t *testing.T) {
	RunStmtTest(t, path.Join(ddlDataPath, "add_column.json"), genAddColumnStmtCases, func(subT *testing.T, caseName string, expected *expectedStore) {
		schema, _, _, _, opts := getAllForTestStmt(subT, caseName)
		stmt, err := genAddColumnStmt(schema.Tables[0], schema.Keyspace.Name, &opts.addType)
		validateStmt(subT, stmt, err)
		expected.CompareOrStore(subT, caseName, stmt)
	})
}

func BenchmarkGenDropColumnStmt(t *testing.B) {
	utils.SetUnderTest()
	for idx := range genDropColumnStmtCases {
		caseName := genDropColumnStmtCases[idx]
		t.Run(caseName,
			func(subT *testing.B) {
				schema, _, _, _, opts := getAllForTestStmt(subT, caseName)
				subT.ResetTimer()
				for x := 0; x < subT.N; x++ {
					_, _ = genDropColumnStmt(schema.Tables[0], schema.Keyspace.Name, schema.Tables[0].Columns[opts.delNum])
				}
			})
	}
}

func BenchmarkGenAddColumnStmt(t *testing.B) {
	utils.SetUnderTest()
	for idx := range genAddColumnStmtCases {
		caseName := genAddColumnStmtCases[idx]
		t.Run(caseName,
			func(subT *testing.B) {
				schema, _, _, _, opts := getAllForTestStmt(subT, caseName)
				subT.ResetTimer()
				for x := 0; x < subT.N; x++ {
					_, _ = genAddColumnStmt(schema.Tables[0], schema.Keyspace.Name, &opts.addType)
				}
			})
	}
}

//nolint:unused
func checkOnAllTypesInAddColumnCases(t *testing.T, cases []string) {
	founded := 0
	for j := 0; j < len(typedef.AllTypes); j++ {
		for i := range cases {
			caseName := cases[i]
			_, caseNum, _ := strings.Cut(caseName, ".")
			caseN, _ := strconv.ParseInt(caseNum, 0, 8)
			num := int(caseN)
			if num == j {
				founded++
			}
		}
	}
	if founded != len(typedef.AllTypes) || founded != len(cases) {
		t.Error("not all column types in genAddColumnStmtCases")
	}
}
