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
	"testing"
)

var (
	genInsertStmtCases = []string{
		"pk1_ck0_col0",
		"pk1_ck1_col1",
		"pk3_ck3_col5",
		"pkAll_ckAll_colAll",
		"pk1_ck1_col1cr",
		"pk3_ck3_col3cr",
		"pk1_ck0_col0_lwt",
		"pk1_ck1_col1_lwt",
		"pk1_ck1_col1cr_lwt",
		"pkAll_ckAll_colAll_lwt",
	}
	genUpdateStmtCases = []string{
		"pk1_ck0_col0",
		"pk1_ck1_col1",
		"pk3_ck3_col5",
		"pkAll_ckAll_colAll",
		"pk1_ck1_col1cr",
		"pk3_ck3_col3cr",
	}

	genDeleteStmtCases = []string{
		"pk1_ck0_col1",
		"pk1_ck1_col1",
		"pk3_ck3_col5",
		"pkAll_ckAll_colAll",
		"pk1_ck1_col1cr",
		"pk3_ck3_col3cr",
	}
)

func TestGenInsertStmt(t *testing.T) {
	expected := initExpected(t, "insert.json", genInsertStmtCases, *updateExpected)
	if *updateExpected {
		defer expected.updateExpected(t)
	}
	for idx := range genInsertStmtCases {
		caseName := genInsertStmtCases[idx]
		t.Run(caseName,
			func(subT *testing.T) {
				schema, prc, gen, rnd, useLWT, _ := getAllForTestStmt(subT, genInsertStmtCases[idx])
				stmt, err := genInsertStmt(schema, schema.Tables[0], gen.Get(), rnd, prc, useLWT)
				validateStmt(subT, stmt, err)
				expected.CompareOrStore(subT, caseName, stmt)
			})
	}
}

func TestGenUpdateStmt(t *testing.T) {
	expected := initExpected(t, "update.json", genUpdateStmtCases, *updateExpected)
	if *updateExpected {
		defer expected.updateExpected(t)
	}
	for idx := range genUpdateStmtCases {
		caseName := genUpdateStmtCases[idx]
		t.Run(caseName,
			func(subT *testing.T) {
				schema, prc, gen, rnd, _, _ := getAllForTestStmt(subT, genUpdateStmtCases[idx])
				stmt, err := genUpdateStmt(schema, schema.Tables[0], gen.Get(), rnd, prc)
				validateStmt(subT, stmt, err)
				expected.CompareOrStore(subT, caseName, stmt)
			})
	}
}

func TestGenDeleteRows(t *testing.T) {
	expected := initExpected(t, "delete.json", genDeleteStmtCases, *updateExpected)
	if *updateExpected {
		defer expected.updateExpected(t)
	}
	for idx := range genDeleteStmtCases {
		caseName := genDeleteStmtCases[idx]
		t.Run(caseName,
			func(subT *testing.T) {
				schema, prc, gen, rnd, _, _ := getAllForTestStmt(subT, genDeleteStmtCases[idx])
				stmt, err := genDeleteRows(schema, schema.Tables[0], gen.Get(), rnd, prc)
				validateStmt(subT, stmt, err)
				expected.CompareOrStore(subT, caseName, stmt)
			})
	}
}

func BenchmarkGenInsertStmt(t *testing.B) {
	for idx := range genInsertStmtCases {
		caseName := genInsertStmtCases[idx]
		t.Run(caseName,
			func(subT *testing.B) {
				schema, prc, gen, rnd, useLWT, _ := getAllForTestStmt(subT, caseName)
				subT.ResetTimer()
				for x := 0; x < subT.N; x++ {
					_, _ = genInsertStmt(schema, schema.Tables[0], gen.Get(), rnd, prc, useLWT)
				}
			})
	}
}

func BenchmarkGenUpdateStmt(t *testing.B) {
	for idx := range genUpdateStmtCases {
		caseName := genUpdateStmtCases[idx]
		t.Run(caseName,
			func(subT *testing.B) {
				schema, prc, gen, rnd, _, _ := getAllForTestStmt(subT, caseName)
				subT.ResetTimer()
				for x := 0; x < subT.N; x++ {
					_, _ = genUpdateStmt(schema, schema.Tables[0], gen.Get(), rnd, prc)
				}
			})
	}
}

func BenchmarkGenDeleteRows(t *testing.B) {
	for idx := range genDeleteStmtCases {
		caseName := genDeleteStmtCases[idx]
		t.Run(caseName,
			func(subT *testing.B) {
				schema, prc, gen, rnd, _, _ := getAllForTestStmt(subT, caseName)
				subT.ResetTimer()
				for x := 0; x < subT.N; x++ {
					_, _ = genDeleteRows(schema, schema.Tables[0], gen.Get(), rnd, prc)
				}
			})
	}
}
