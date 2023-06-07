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
	"testing"

	"github.com/scylladb/gemini/pkg/utils"
)

var mutateDataPath = "./test_expected_data/mutate/"

func TestGenInsertStmt(t *testing.T) {
	RunStmtTest(t, path.Join(mutateDataPath, "insert.json"), genInsertStmtCases, func(t *testing.T, caseName string, expected *expectedStore) {
		schema, prc, gen, rnd, opts := getAllForTestStmt(t, caseName)
		stmt, err := genInsertStmt(schema, schema.Tables[0], gen.Get(), rnd, prc, opts.useLWT)
		validateStmt(t, stmt, err)
		expected.CompareOrStore(t, caseName, stmt)
	})
}

func TestGenInsertJSONStmt(t *testing.T) {
	RunStmtTest(t, path.Join(mutateDataPath, "insert_j.json"), genInsertJSONStmtCases, func(t *testing.T, caseName string, expected *expectedStore) {
		schema, prc, gen, rnd, _ := getAllForTestStmt(t, caseName)
		stmt, err := genInsertJSONStmt(schema, schema.Tables[0], gen.Get(), rnd, prc)
		validateStmt(t, stmt, err)
		expected.CompareOrStore(t, caseName, stmt)
	})
}

func TestGenUpdateStmt(t *testing.T) {
	RunStmtTest(t, path.Join(mutateDataPath, "update.json"), genUpdateStmtCases, func(t *testing.T, caseName string, expected *expectedStore) {
		schema, prc, gen, rnd, _ := getAllForTestStmt(t, caseName)
		stmt, err := genUpdateStmt(schema, schema.Tables[0], gen.Get(), rnd, prc)
		validateStmt(t, stmt, err)
		expected.CompareOrStore(t, caseName, stmt)
	})
}

func TestGenDeleteRows(t *testing.T) {
	RunStmtTest(t, path.Join(mutateDataPath, "delete.json"), genDeleteStmtCases, func(t *testing.T, caseName string, expected *expectedStore) {
		schema, prc, gen, rnd, _ := getAllForTestStmt(t, caseName)
		stmt, err := genDeleteRows(schema, schema.Tables[0], gen.Get(), rnd, prc)
		validateStmt(t, stmt, err)
		expected.CompareOrStore(t, caseName, stmt)
	})
}

func BenchmarkGenInsertStmt(t *testing.B) {
	utils.SetUnderTest()
	for idx := range genInsertStmtCases {
		caseName := genInsertStmtCases[idx]
		t.Run(caseName,
			func(t *testing.B) {
				schema, prc, gen, rnd, opts := getAllForTestStmt(t, caseName)
				t.ResetTimer()
				for x := 0; x < t.N; x++ {
					_, _ = genInsertStmt(schema, schema.Tables[0], gen.Get(), rnd, prc, opts.useLWT)
				}
			})
	}
}

func BenchmarkGenInsertJSONStmt(t *testing.B) {
	utils.SetUnderTest()
	for idx := range genInsertJSONStmtCases {
		caseName := genInsertJSONStmtCases[idx]
		t.Run(caseName,
			func(t *testing.B) {
				schema, prc, gen, rnd, _ := getAllForTestStmt(t, caseName)
				t.ResetTimer()
				for x := 0; x < t.N; x++ {
					_, _ = genInsertJSONStmt(schema, schema.Tables[0], gen.Get(), rnd, prc)
				}
			})
	}
}

func BenchmarkGenUpdateStmt(t *testing.B) {
	utils.SetUnderTest()
	for idx := range genUpdateStmtCases {
		caseName := genUpdateStmtCases[idx]
		t.Run(caseName,
			func(t *testing.B) {
				schema, prc, gen, rnd, _ := getAllForTestStmt(t, caseName)
				t.ResetTimer()
				for x := 0; x < t.N; x++ {
					_, _ = genUpdateStmt(schema, schema.Tables[0], gen.Get(), rnd, prc)
				}
			})
	}
}

func BenchmarkGenDeleteRows(t *testing.B) {
	utils.SetUnderTest()
	for idx := range genDeleteStmtCases {
		caseName := genDeleteStmtCases[idx]
		t.Run(caseName,
			func(t *testing.B) {
				schema, prc, gen, rnd, _ := getAllForTestStmt(t, caseName)
				t.ResetTimer()
				for x := 0; x < t.N; x++ {
					_, _ = genDeleteRows(schema, schema.Tables[0], gen.Get(), rnd, prc)
				}
			})
	}
}
