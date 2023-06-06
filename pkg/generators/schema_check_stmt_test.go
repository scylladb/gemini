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

	"github.com/scylladb/gemini/pkg/utils"
)

var checkDataPath = "./test_expected_data/check/"

// GenCheck functions tests
func TestGenSinglePartitionQuery(t *testing.T) {
	utils.SetUnderTest()
	t.Parallel()
	expected := initExpected(t, checkDataPath, "single_partition.json", genSinglePartitionQueryCases, *updateExpected)
	if *updateExpected {
		defer expected.updateExpected(t)
	}
	for idx := range genSinglePartitionQueryCases {
		caseName := genSinglePartitionQueryCases[idx]
		t.Run(caseName,
			func(subT *testing.T) {
				schema, prc, gen, rnd, opts := getAllForTestStmt(subT, caseName)
				stmt := genSinglePartitionQuery(schema, schema.Tables[0], gen, rnd, prc, opts.mvNum)
				validateStmt(subT, stmt, nil)
				expected.CompareOrStore(subT, caseName, stmt)
			})
	}
}

func TestGenMultiplePartitionQuery(t *testing.T) {
	utils.SetUnderTest()
	t.Parallel()
	expected := initExpected(t, checkDataPath, "multiple_partition.json", genMultiplePartitionQueryCases, *updateExpected)
	if *updateExpected {
		defer expected.updateExpected(t)
	}
	for idx := range genMultiplePartitionQueryCases {
		caseName := genMultiplePartitionQueryCases[idx]
		t.Run(caseName,
			func(subT *testing.T) {
				schema, prc, gen, rnd, opts := getAllForTestStmt(subT, caseName)
				stmt := genMultiplePartitionQuery(schema, schema.Tables[0], gen, rnd, prc, opts.mvNum, opts.pkCount)
				validateStmt(subT, stmt, nil)
				expected.CompareOrStore(subT, caseName, stmt)
			})
	}
}

func TestGenClusteringRangeQuery(t *testing.T) {
	utils.SetUnderTest()
	t.Parallel()
	expected := initExpected(t, checkDataPath, "clustering_range.json", genClusteringRangeQueryCases, *updateExpected)
	if *updateExpected {
		defer expected.updateExpected(t)
	}
	for idx := range genClusteringRangeQueryCases {
		caseName := genClusteringRangeQueryCases[idx]
		t.Run(caseName,
			func(subT *testing.T) {
				schema, prc, gen, rnd, opts := getAllForTestStmt(subT, caseName)
				stmt := genClusteringRangeQuery(schema, schema.Tables[0], gen, rnd, prc, opts.mvNum, opts.ckCount)
				validateStmt(subT, stmt, nil)
				expected.CompareOrStore(subT, caseName, stmt)
			})
	}
}

func TestGenMultiplePartitionClusteringRangeQuery(t *testing.T) {
	utils.SetUnderTest()
	t.Parallel()
	expected := initExpected(t, checkDataPath, "multiple_partition_clustering_range.json", genMultiplePartitionClusteringRangeQueryCases, *updateExpected)
	if *updateExpected {
		defer expected.updateExpected(t)
	}
	for idx := range genMultiplePartitionClusteringRangeQueryCases {
		caseName := genMultiplePartitionClusteringRangeQueryCases[idx]
		t.Run(caseName,
			func(subT *testing.T) {
				schema, prc, gen, rnd, opts := getAllForTestStmt(subT, caseName)
				stmt := genMultiplePartitionClusteringRangeQuery(schema, schema.Tables[0], gen, rnd, prc, opts.mvNum, opts.pkCount, opts.ckCount)
				validateStmt(subT, stmt, nil)
				expected.CompareOrStore(subT, caseName, stmt)
			})
	}
}

func TestGenSingleIndexQuery(t *testing.T) {
	utils.SetUnderTest()
	t.Parallel()
	expected := initExpected(t, checkDataPath, "single_index.json", genSingleIndexQueryCases, *updateExpected)
	if *updateExpected {
		defer expected.updateExpected(t)
	}
	for idx := range genSingleIndexQueryCases {
		caseName := genSingleIndexQueryCases[idx]
		t.Run(caseName,
			func(subT *testing.T) {
				schema, prc, gen, rnd, opts := getAllForTestStmt(subT, caseName)
				stmt := genSingleIndexQuery(schema, schema.Tables[0], gen, rnd, prc, opts.idxCount)
				validateStmt(subT, stmt, nil)
				expected.CompareOrStore(subT, caseName, stmt)
			})
	}
}

func BenchmarkGenSinglePartitionQuery(t *testing.B) {
	utils.SetUnderTest()
	for idx := range genSinglePartitionQueryCases {
		caseName := genSinglePartitionQueryCases[idx]
		t.Run(caseName,
			func(subT *testing.B) {
				schema, prc, gen, rnd, opts := getAllForTestStmt(subT, caseName)
				subT.ResetTimer()
				for x := 0; x < subT.N; x++ {
					_ = genSinglePartitionQuery(schema, schema.Tables[0], gen, rnd, prc, opts.mvNum)
				}
			})
	}
}

func BenchmarkGenMultiplePartitionQuery(t *testing.B) {
	utils.SetUnderTest()
	for idx := range genMultiplePartitionQueryCases {
		caseName := genMultiplePartitionQueryCases[idx]
		t.Run(caseName,
			func(subT *testing.B) {
				schema, prc, gen, rnd, opts := getAllForTestStmt(subT, caseName)
				subT.ResetTimer()
				for x := 0; x < subT.N; x++ {
					_ = genMultiplePartitionQuery(schema, schema.Tables[0], gen, rnd, prc, opts.mvNum, opts.pkCount)
				}
			})
	}
}

func BenchmarkGenClusteringRangeQuery(t *testing.B) {
	utils.SetUnderTest()
	for idx := range genClusteringRangeQueryCases {
		caseName := genClusteringRangeQueryCases[idx]
		t.Run(caseName,
			func(subT *testing.B) {
				schema, prc, gen, rnd, opts := getAllForTestStmt(subT, caseName)
				subT.ResetTimer()
				for x := 0; x < subT.N; x++ {
					_ = genClusteringRangeQuery(schema, schema.Tables[0], gen, rnd, prc, opts.mvNum, opts.ckCount)
				}
			})
	}
}

func BenchmarkGenMultiplePartitionClusteringRangeQuery(t *testing.B) {
	utils.SetUnderTest()
	for idx := range genMultiplePartitionClusteringRangeQueryCases {
		caseName := genMultiplePartitionClusteringRangeQueryCases[idx]
		t.Run(caseName,
			func(subT *testing.B) {
				schema, prc, gen, rnd, opts := getAllForTestStmt(subT, caseName)
				subT.ResetTimer()
				for x := 0; x < subT.N; x++ {
					_ = genMultiplePartitionClusteringRangeQuery(schema, schema.Tables[0], gen, rnd, prc, opts.mvNum, opts.pkCount, opts.ckCount)
				}
			})
	}
}

func BenchmarkGenSingleIndexQuery(t *testing.B) {
	utils.SetUnderTest()
	for idx := range genSingleIndexQueryCases {
		caseName := genSingleIndexQueryCases[idx]
		t.Run(caseName,
			func(subT *testing.B) {
				schema, prc, gen, rnd, opts := getAllForTestStmt(subT, caseName)
				subT.ResetTimer()
				for x := 0; x < subT.N; x++ {
					_ = genSingleIndexQuery(schema, schema.Tables[0], gen, rnd, prc, opts.idxCount)
				}
			})
	}
}
