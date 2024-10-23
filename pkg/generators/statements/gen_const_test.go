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

package statements

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
	genInsertJSONStmtCases = []string{
		"pk1_ck0_col0",
		"pk1_ck1_col1",
		"pk3_ck3_col5",
		"pkAll_ckAll_colAll",
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

var (
	genSinglePartitionQueryCases = []string{
		"pk1_ck0_col0",
		"pk1_ck1_col1",
		"pk3_ck3_col5",
		"pkAll_ckAll_colAll",
		"pk1_ck1_col1cr",
		"pk3_ck3_col3cr",
	}
	genSinglePartitionQueryMvCases = []string{
		"pk1_ck0_col0_mv",
		"pk1_ck1_col1_mv",
		"pkAll_ckAll_colAll_mv",
		"pk1_ck1_col1_mvNp",
		"pkAll_ckAll_colAll_mvNp",
	}
	genMultiplePartitionQueryCases = []string{
		"pk1_ck0_col0.cpk1",
		"pk1_ck1_col1.cpk1",
		"pk3_ck3_col5.cpk1",
		"pkAll_ckAll_colAll.cpk1",
		"pk1_ck1_col1cr.cpkAll",
		"pk3_ck3_col3cr.cpkAll",
		"pk3_ck3_col5.cpkAll",
		"pkAll_ckAll_colAll.cpkAll",
	}
	genMultiplePartitionQueryMvCases = []string{
		"pk1_ck0_col0_mv.cpk1",
		"pk1_ck1_col1_mv.cpk1",
		"pk3_ck3_col5_mv.cpk1",
		"pkAll_ckAll_colAll_mv.cpk1",
		"pk1_ck1_col1cr_mv.cpkAll",
		"pk3_ck3_col3cr_mv.cpkAll",
		"pk3_ck3_col5_mv.cpkAll",
		"pkAll_ckAll_colAll_mv.cpkAll",

		"pk1_ck0_col1_mvNp.cpk1",
		"pk1_ck1_col1_mvNp.cpk1",
		"pk3_ck3_col5_mvNp.cpk1",
		"pkAll_ckAll_colAll_mvNp.cpk1",
		"pk3_ck3_col5_mvNp.cpkAll",
		"pkAll_ckAll_colAll_mvNp.cpkAll",
	}
	genClusteringRangeQueryCases = []string{
		"pk1_ck1_col1.cck1",
		"pk3_ck3_col5.cck1",
		"pkAll_ckAll_colAll.cck1",
		"pk1_ck1_col1cr.cckAll",
		"pk3_ck3_col3cr.cckAll",
		"pk3_ck3_col5.cckAll",
		"pkAll_ckAll_colAll.cckAll",
	}
	genClusteringRangeQueryMvCases = []string{
		"pk1_ck1_col1_mv.cck1",
		"pk3_ck3_col5_mv.cck1",
		"pkAll_ckAll_colAll_mv.cck1",
		"pk1_ck1_col1cr_mv.cckAll",
		"pk3_ck3_col3cr_mv.cckAll",
		"pk3_ck3_col5_mv.cckAll",
		"pkAll_ckAll_colAll_mv.cckAll",

		"pk1_ck1_col1_mvNp.cck1",
		"pk3_ck3_col5_mvNp.cck1",
		"pkAll_ckAll_colAll_mvNp.cck1",
		"pk3_ck3_col5_mvNp.cckAll",
		"pkAll_ckAll_colAll_mvNp.cckAll",
	}
	genMultiplePartitionClusteringRangeQueryCases = []string{
		"pk1_ck1_col1.cpk1.cck1",
		"pk3_ck3_col5.cpk1.cck1",
		"pkAll_ckAll_colAll.cpk1.cck1",
		"pk1_ck1_col1cr.cpkAll.cck1",
		"pk3_ck3_col3cr.cpkAll.cck1",
		"pk3_ck3_col5.cpkAll.cck1",
		"pkAll_ckAll_colAll.cpkAll.cck1",

		"pk1_ck1_col1.cpk1.cckAll",
		"pk3_ck3_col5.cpk1.cckAll",
		"pkAll_ckAll_colAll.cpk1.cckAll",
		"pk1_ck1_col1cr.cpkAll.cckAll",
		"pk3_ck3_col3cr.cpkAll.cckAll",
		"pk3_ck3_col5.cpkAll.cckAll",
		"pkAll_ckAll_colAll.cpkAll.cckAll",
	}

	genMultiplePartitionClusteringRangeQueryMvCases = []string{
		"pk1_ck1_col1_mv.cpk1.cck1",
		"pk3_ck3_col5_mv.cpk1.cck1",
		"pkAll_ckAll_colAll_mv.cpk1.cck1",
		"pk1_ck1_col1cr_mv.cpkAll.cck1",
		"pk3_ck3_col3cr_mv.cpkAll.cck1",
		"pk3_ck3_col5_mv.cpkAll.cck1",
		"pkAll_ckAll_colAll_mv.cpkAll.cck1",

		"pk1_ck1_col1_mv.cpk1.cckAll",
		"pk3_ck3_col5_mv.cpk1.cckAll",
		"pkAll_ckAll_colAll_mv.cpk1.cckAll",
		"pk1_ck1_col1cr_mv.cpkAll.cckAll",
		"pk3_ck3_col3cr_mv.cpkAll.cckAll",
		"pk3_ck3_col5_mv.cpkAll.cckAll",
		"pkAll_ckAll_colAll_mv.cpkAll.cckAll",

		"pk1_ck1_col1_mvNp.cpk1.cck1",
		"pk3_ck3_col5_mvNp.cpk1.cck1",
		"pkAll_ckAll_colAll_mvNp.cpk1.cck1",
		"pk3_ck3_col5_mvNp.cpkAll.cck1",
		"pkAll_ckAll_colAll_mvNp.cpkAll.cck1",

		"pk1_ck1_col1_mvNp.cpk1.cckAll",
		"pk3_ck3_col5_mvNp.cpk1.cckAll",
		"pkAll_ckAll_colAll_mvNp.cpk1.cckAll",
		"pk3_ck3_col5_mvNp.cpkAll.cckAll",
		"pkAll_ckAll_colAll_mvNp.cpkAll.cckAll",
	}

	genSingleIndexQueryCases = []string{
		"pk1_ck0_col1_idx1",
		"pk3_ck3_col5_idx1",
		"pkAll_ckAll_colAll_idxAll",
	}
)

var (
	genDropColumnStmtCases = []string{
		"pk1_ck1_col1.delFist",
		"pkAll_ckAll_colAll.delLast",
	}
	genAddColumnStmtCases = []string{
		"pk1_ck1_col1.addSt0",
		"pk1_ck1_col1.addSt1",
		"pk1_ck1_col1.addSt2",
		"pk1_ck1_col1.addSt3",
		"pk1_ck1_col1.addSt4",
		"pk1_ck1_col1.addSt5",
		"pk1_ck1_col1.addSt6",
		"pk1_ck1_col1.addSt7",
		"pk1_ck1_col1.addSt8",
		"pk1_ck1_col1.addSt9",
		"pk1_ck1_col1.addSt10",
		"pk1_ck1_col1.addSt11",
		"pk1_ck1_col1.addSt12",
		"pk1_ck1_col1.addSt13",
		"pk1_ck1_col1.addSt14",
		"pk1_ck1_col1.addSt15",
		"pk1_ck1_col1.addSt16",
		"pk1_ck1_col1.addSt17",
		"pk1_ck1_col1.addSt18",
	}
)
