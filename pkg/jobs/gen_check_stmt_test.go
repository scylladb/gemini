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

var checkDataPath = "./test_expected_data/check/"

//
//func TestGenSinglePartitionQuery(t *testing.T) {
//	RunStmtTest[results](
//		t,
//		path.Join(checkDataPath, "single_partition.json"),
//		genSinglePartitionQueryCases,
//		func(subT *testing.T, caseName string, expected *testutils.ExpectedStore[results]) {
//			schema, gen, _ := testutils.GetAllForTestStmt(subT, caseName)
//			stmt := genSinglePartitionQuery(t.Context(), schema, schema.Tables[0], gen)
//			validateStmt(subT, stmt, nil)
//			expected.CompareOrStore(subT, caseName, convertStmtsToResults(stmt))
//		},
//	)
//}
//
//func TestGenSinglePartitionQueryMv(t *testing.T) {
//	RunStmtTest[results](
//		t,
//		path.Join(checkDataPath, "single_partition_mv.json"),
//		genSinglePartitionQueryMvCases,
//		func(subT *testing.T, caseName string, expected *testutils.ExpectedStore[results]) {
//			schema, gen, rnd := testutils.GetAllForTestStmt(subT, caseName)
//			prc := schema.Config.GetPartitionRangeConfig()
//			stmt := genSinglePartitionQueryMv(
//				t.Context(),
//				schema,
//				schema.Tables[0],
//				gen,
//				rnd,
//				&prc,
//				&schema.Tables[0].MaterializedViews[len(schema.Tables[0].MaterializedViews)-1],
//			)
//			validateStmt(subT, stmt, nil)
//			expected.CompareOrStore(subT, caseName, convertStmtsToResults(stmt))
//		},
//	)
//}
//
//func TestGenMultiplePartitionQuery(t *testing.T) {
//	RunStmtTest[results](
//		t,
//		path.Join(checkDataPath, "multiple_partition.json"),
//		genMultiplePartitionQueryCases,
//		func(subT *testing.T, caseName string, expected *testutils.ExpectedStore[results]) {
//			schema, gen, _ := testutils.GetAllForTestStmt(subT, caseName)
//			options := testutils.GetOptionsFromCaseName(caseName)
//			stmt := genMultiplePartitionQuery(
//				t.Context(),
//				schema,
//				schema.Tables[0],
//				gen,
//				GetPkCountFromOptions(options, len(schema.Tables[0].PartitionKeys)),
//			)
//			validateStmt(subT, stmt, nil)
//			expected.CompareOrStore(subT, caseName, convertStmtsToResults(stmt))
//		},
//	)
//}
//
//func TestGenMultiplePartitionQueryMv(t *testing.T) {
//	RunStmtTest[results](
//		t,
//		path.Join(checkDataPath, "multiple_partition_mv.json"),
//		genMultiplePartitionQueryMvCases,
//		func(subT *testing.T, caseName string, expected *testutils.ExpectedStore[results]) {
//			options := testutils.GetOptionsFromCaseName(caseName)
//			schema, gen, _ := testutils.GetAllForTestStmt(subT, caseName)
//			stmt := genMultiplePartitionQuery(
//				t.Context(),
//				schema,
//				schema.Tables[0],
//				gen,
//				GetPkCountFromOptions(options, len(schema.Tables[0].PartitionKeys)),
//			)
//			validateStmt(subT, stmt, nil)
//			expected.CompareOrStore(subT, caseName, convertStmtsToResults(stmt))
//		},
//	)
//}
//
//func TestGenClusteringRangeQuery(t *testing.T) {
//	RunStmtTest[results](
//		t,
//		path.Join(checkDataPath, "clustering_range.json"),
//		genClusteringRangeQueryCases,
//		func(subT *testing.T, caseName string, expected *testutils.ExpectedStore[results]) {
//			schema, gen, rnd := testutils.GetAllForTestStmt(subT, caseName)
//			options := testutils.GetOptionsFromCaseName(caseName)
//			prc := schema.Config.GetPartitionRangeConfig()
//			stmt := genClusteringRangeQuery(
//				t.Context(),
//				schema,
//				schema.Tables[0],
//				gen,
//				rnd,
//				&prc,
//				GetCkCountFromOptions(options, len(schema.Tables[0].ClusteringKeys)-1),
//			)
//			validateStmt(subT, stmt, nil)
//			expected.CompareOrStore(subT, caseName, convertStmtsToResults(stmt))
//		},
//	)
//}
//
//func TestGenClusteringRangeQueryMv(t *testing.T) {
//	RunStmtTest[results](
//		t,
//		path.Join(checkDataPath, "clustering_range_mv.json"),
//		genClusteringRangeQueryMvCases,
//		func(subT *testing.T, caseName string, expected *testutils.ExpectedStore[results]) {
//			schema, gen, rnd := testutils.GetAllForTestStmt(subT, caseName)
//			options := testutils.GetOptionsFromCaseName(caseName)
//			prc := schema.Config.GetPartitionRangeConfig()
//			stmt := genClusteringRangeQueryMv(
//				t.Context(),
//				schema,
//				schema.Tables[0],
//				gen,
//				rnd,
//				&prc,
//				&schema.Tables[0].MaterializedViews[len(schema.Tables[0].MaterializedViews)-1],
//				GetCkCountFromOptions(options, len(schema.Tables[0].ClusteringKeys)-1))
//			validateStmt(subT, stmt, nil)
//			expected.CompareOrStore(subT, caseName, convertStmtsToResults(stmt))
//		},
//	)
//}
//
//func TestGenMultiplePartitionClusteringRangeQuery(t *testing.T) {
//	RunStmtTest[results](
//		t,
//		path.Join(checkDataPath, "multiple_partition_clustering_range.json"),
//		genMultiplePartitionClusteringRangeQueryCases,
//		func(subT *testing.T, caseName string, expected *testutils.ExpectedStore[results]) {
//			schema, gen, rnd := testutils.GetAllForTestStmt(subT, caseName)
//			options := testutils.GetOptionsFromCaseName(caseName)
//			prc := schema.Config.GetPartitionRangeConfig()
//			stmt := genMultiplePartitionClusteringRangeQuery(
//				t.Context(),
//				schema,
//				schema.Tables[0],
//				gen,
//				rnd,
//				&prc,
//				GetPkCountFromOptions(options, len(schema.Tables[0].PartitionKeys)),
//				GetCkCountFromOptions(options, len(schema.Tables[0].ClusteringKeys)-1))
//			validateStmt(subT, stmt, nil)
//			expected.CompareOrStore(subT, caseName, convertStmtsToResults(stmt))
//		},
//	)
//}
//
//func TestGenMultiplePartitionClusteringRangeQueryMv(t *testing.T) {
//	RunStmtTest[results](
//		t,
//		path.Join(checkDataPath, "multiple_partition_clustering_range_mv.json"),
//		genMultiplePartitionClusteringRangeQueryMvCases,
//		func(subT *testing.T, caseName string, expected *testutils.ExpectedStore[results]) {
//			options := testutils.GetOptionsFromCaseName(caseName)
//			schema, gen, rnd := testutils.GetAllForTestStmt(subT, caseName)
//			prc := schema.Config.GetPartitionRangeConfig()
//			stmt := genMultiplePartitionClusteringRangeQueryMv(
//				t.Context(),
//				schema,
//				gen,
//				rnd,
//				&prc,
//				&schema.Tables[0].MaterializedViews[len(schema.Tables[0].MaterializedViews)-1],
//				GetPkCountFromOptions(options, len(schema.Tables[0].PartitionKeys)),
//				GetCkCountFromOptions(options, len(schema.Tables[0].ClusteringKeys)-1))
//			validateStmt(subT, stmt, nil)
//			expected.CompareOrStore(subT, caseName, convertStmtsToResults(stmt))
//		},
//	)
//}
//
//func TestGenSingleIndexQuery(t *testing.T) {
//	RunStmtTest[results](t, path.Join(checkDataPath, "single_index.json"), genSingleIndexQueryCases,
//		func(subT *testing.T, caseName string, expected *testutils.ExpectedStore[results]) {
//			schema, gen, rnd := testutils.GetAllForTestStmt(subT, caseName)
//			prc := schema.Config.GetPartitionRangeConfig()
//			stmt := genSingleIndexQuery(
//				schema,
//				schema.Tables[0],
//				gen,
//				rnd,
//				&prc,
//				len(schema.Tables[0].Indexes),
//			)
//			validateStmt(subT, stmt, nil)
//			expected.CompareOrStore(subT, caseName, convertStmtsToResults(stmt))
//		})
//}
//
//func BenchmarkGenSinglePartitionQuery(t *testing.B) {
//	for idx := range genSinglePartitionQueryCases {
//		caseName := genSinglePartitionQueryCases[idx]
//		t.Run(caseName,
//			func(subT *testing.B) {
//				schema, gen, _ := testutils.GetAllForTestStmt(subT, caseName)
//				subT.ResetTimer()
//				for x := 0; x < subT.N; x++ {
//					_ = genSinglePartitionQuery(t.Context(), schema, schema.Tables[0], gen)
//				}
//			})
//	}
//}
//
//func BenchmarkGenSinglePartitionQueryMv(t *testing.B) {
//	for idx := range genSinglePartitionQueryMvCases {
//		caseName := genSinglePartitionQueryMvCases[idx]
//		t.Run(caseName,
//			func(subT *testing.B) {
//				schema, gen, rnd := testutils.GetAllForTestStmt(subT, caseName)
//				prc := schema.Config.GetPartitionRangeConfig()
//				subT.ResetTimer()
//				for x := 0; x < subT.N; x++ {
//					_ = genSinglePartitionQueryMv(
//						t.Context(),
//						schema,
//						schema.Tables[0],
//						gen,
//						rnd,
//						&prc,
//						&schema.Tables[0].MaterializedViews[len(schema.Tables[0].MaterializedViews)-1],
//					)
//				}
//			})
//	}
//}
//
//func BenchmarkGenMultiplePartitionQuery(t *testing.B) {
//	for idx := range genMultiplePartitionQueryCases {
//		caseName := genMultiplePartitionQueryCases[idx]
//		t.Run(caseName,
//			func(subT *testing.B) {
//				options := testutils.GetOptionsFromCaseName(caseName)
//				schema, gen, _ := testutils.GetAllForTestStmt(subT, caseName)
//				subT.ResetTimer()
//				for x := 0; x < subT.N; x++ {
//					_ = genMultiplePartitionQuery(
//						t.Context(),
//						schema,
//						schema.Tables[0],
//						gen,
//						GetPkCountFromOptions(options, len(schema.Tables[0].PartitionKeys)),
//					)
//				}
//			})
//	}
//}
//
//func BenchmarkGenMultiplePartitionQueryMv(t *testing.B) {
//	for idx := range genMultiplePartitionQueryMvCases {
//		caseName := genMultiplePartitionQueryMvCases[idx]
//		t.Run(caseName,
//			func(subT *testing.B) {
//				options := testutils.GetOptionsFromCaseName(caseName)
//				schema, gen, rnd := testutils.GetAllForTestStmt(subT, caseName)
//				prc := schema.Config.GetPartitionRangeConfig()
//				subT.ResetTimer()
//				for x := 0; x < subT.N; x++ {
//					_ = genMultiplePartitionQueryMv(
//						t.Context(),
//						schema,
//						schema.Tables[0],
//						gen,
//						rnd,
//						&prc,
//						&schema.Tables[0].MaterializedViews[len(schema.Tables[0].MaterializedViews)-1],
//						GetPkCountFromOptions(options, len(schema.Tables[0].PartitionKeys)))
//				}
//			})
//	}
//}
//
//func BenchmarkGenClusteringRangeQuery(t *testing.B) {
//	for idx := range genClusteringRangeQueryCases {
//		caseName := genClusteringRangeQueryCases[idx]
//		t.Run(caseName,
//			func(subT *testing.B) {
//				options := testutils.GetOptionsFromCaseName(caseName)
//				schema, gen, rnd := testutils.GetAllForTestStmt(subT, caseName)
//				prc := schema.Config.GetPartitionRangeConfig()
//				subT.ResetTimer()
//				for x := 0; x < subT.N; x++ {
//					_ = genClusteringRangeQuery(
//						t.Context(),
//						schema,
//						schema.Tables[0],
//						gen,
//						rnd,
//						&prc,
//						GetCkCountFromOptions(options, len(schema.Tables[0].ClusteringKeys)-1),
//					)
//				}
//			})
//	}
//}
//
//func BenchmarkGenClusteringRangeQueryMv(t *testing.B) {
//	for idx := range genClusteringRangeQueryMvCases {
//		caseName := genClusteringRangeQueryMvCases[idx]
//		t.Run(caseName,
//			func(subT *testing.B) {
//				options := testutils.GetOptionsFromCaseName(caseName)
//				schema, gen, rnd := testutils.GetAllForTestStmt(subT, caseName)
//				prc := schema.Config.GetPartitionRangeConfig()
//				subT.ResetTimer()
//				for x := 0; x < subT.N; x++ {
//					_ = genClusteringRangeQueryMv(
//						t.Context(),
//						schema,
//						schema.Tables[0],
//						gen,
//						rnd,
//						&prc,
//						&schema.Tables[0].MaterializedViews[len(schema.Tables[0].MaterializedViews)-1],
//						GetCkCountFromOptions(options, len(schema.Tables[0].ClusteringKeys)-1))
//				}
//			})
//	}
//}
//
//func BenchmarkGenMultiplePartitionClusteringRangeQuery(t *testing.B) {
//	for idx := range genMultiplePartitionClusteringRangeQueryCases {
//		caseName := genMultiplePartitionClusteringRangeQueryCases[idx]
//		t.Run(caseName,
//			func(subT *testing.B) {
//				options := testutils.GetOptionsFromCaseName(caseName)
//				schema, gen, rnd := testutils.GetAllForTestStmt(subT, caseName)
//				prc := schema.Config.GetPartitionRangeConfig()
//				subT.ResetTimer()
//				for x := 0; x < subT.N; x++ {
//					_ = genMultiplePartitionClusteringRangeQuery(
//						t.Context(),
//						schema,
//						schema.Tables[0],
//						gen,
//						rnd,
//						&prc,
//						GetPkCountFromOptions(options, len(schema.Tables[0].PartitionKeys)),
//						GetCkCountFromOptions(options, len(schema.Tables[0].ClusteringKeys)-1))
//				}
//			})
//	}
//}
//
//func BenchmarkGenMultiplePartitionClusteringRangeQueryMv(t *testing.B) {
//	for idx := range genMultiplePartitionClusteringRangeQueryMvCases {
//		caseName := genMultiplePartitionClusteringRangeQueryMvCases[idx]
//		t.Run(caseName,
//			func(subT *testing.B) {
//				options := testutils.GetOptionsFromCaseName(caseName)
//				schema, gen, rnd := testutils.GetAllForTestStmt(subT, caseName)
//				prc := schema.Config.GetPartitionRangeConfig()
//				subT.ResetTimer()
//				for subT.Loop() {
//					_ = genMultiplePartitionClusteringRangeQueryMv(
//						t.Context(),
//						schema,
//						gen,
//						rnd,
//						&prc,
//						&schema.Tables[0].MaterializedViews[len(schema.Tables[0].MaterializedViews)-1],
//						GetPkCountFromOptions(options, len(schema.Tables[0].PartitionKeys)),
//						GetCkCountFromOptions(options, len(schema.Tables[0].ClusteringKeys)-1))
//				}
//			})
//	}
//}
//
//func BenchmarkGenSingleIndexQuery(t *testing.B) {
//	for idx := range genSingleIndexQueryCases {
//		caseName := genSingleIndexQueryCases[idx]
//		t.Run(caseName,
//			func(subT *testing.B) {
//				schema, gen, rnd := testutils.GetAllForTestStmt(subT, caseName)
//				prc := schema.Config.GetPartitionRangeConfig()
//				subT.ResetTimer()
//				for x := 0; x < subT.N; x++ {
//					_ = genSingleIndexQuery(
//						schema,
//						schema.Tables[0],
//						gen,
//						rnd,
//						&prc,
//						len(schema.Tables[0].Indexes))
//				}
//			})
//	}
//}
