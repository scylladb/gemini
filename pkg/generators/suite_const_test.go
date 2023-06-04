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
	"flag"

	. "github.com/scylladb/gemini/pkg/coltypes"
	. "github.com/scylladb/gemini/pkg/typedef"
)

var (
	partitionKeysCases = map[string][]Type{
		"pk1": {TYPE_BIGINT},
		"pk3": {TYPE_BIGINT, TYPE_FLOAT, TYPE_INET},
		"pkAll": {
			TYPE_ASCII, TYPE_BIGINT, TYPE_BLOB, TYPE_BOOLEAN, TYPE_DATE, TYPE_DECIMAL, TYPE_DOUBLE, TYPE_FLOAT,
			TYPE_INET, TYPE_INT, TYPE_SMALLINT, TYPE_TEXT, TYPE_TIMESTAMP, TYPE_TIMEUUID, TYPE_TINYINT, TYPE_UUID, TYPE_VARCHAR, TYPE_VARINT, TYPE_TIME,
		},
	}

	clusteringKeysCases = map[string][]Type{
		"ck0": {},
		"ck1": {TYPE_DATE},
		"ck3": {TYPE_ASCII, TYPE_DATE, TYPE_DECIMAL},
		"ckAll": {
			TYPE_ASCII, TYPE_BIGINT, TYPE_BLOB, TYPE_BOOLEAN, TYPE_DATE, TYPE_DECIMAL, TYPE_DOUBLE, TYPE_FLOAT,
			TYPE_INET, TYPE_INT, TYPE_SMALLINT, TYPE_TEXT, TYPE_TIMESTAMP, TYPE_TIMEUUID, TYPE_TINYINT, TYPE_UUID, TYPE_VARCHAR, TYPE_VARINT, TYPE_TIME,
		},
	}

	columnsCases = map[string][]Type{
		"col0":   {},
		"col1":   {TYPE_DATE},
		"col5":   {TYPE_ASCII, TYPE_DATE, TYPE_BLOB, TYPE_BIGINT, TYPE_FLOAT},
		"col5c":  {TYPE_ASCII, &mapType, TYPE_BLOB, &tupleType, TYPE_FLOAT},
		"col1cr": {&counterType},
		"col3cr": {&counterType, &counterType, &counterType},
		"colAll": {
			TYPE_DURATION, TYPE_ASCII, TYPE_BIGINT, TYPE_BLOB, TYPE_BOOLEAN, TYPE_DATE, TYPE_DECIMAL, TYPE_DOUBLE, TYPE_FLOAT,
			TYPE_INET, TYPE_INT, TYPE_SMALLINT, TYPE_TEXT, TYPE_TIMESTAMP, TYPE_TIMEUUID, TYPE_TINYINT, TYPE_UUID, TYPE_VARCHAR, TYPE_VARINT, TYPE_TIME,
		},
	}

	optionsCases = map[string]bool{
		"mv":      true,
		"mvNp":    true,
		"cpk1":    true,
		"cpkAll":  true,
		"cck1":    true,
		"cckAll":  true,
		"lwt":     true,
		"idx1":    true,
		"idxAll":  true,
		"delFist": true,
		"delLast": true,
		"addSt":   true,
	}

	counterType CounterType
	tupleType   TupleType
	mapType     MapType

	updateExpected = flag.Bool("update-expected", false, "make test to update expected results")
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
		"pk1_ck0_col0_mv",
		"pk1_ck1_col1_mv",
		"pkAll_ckAll_colAll_mv",
		"pk1_ck1_col1_mvNp",
		"pkAll_ckAll_colAll_mvNp",
	}
	genMultiplePartitionQueryCases = []string{
		"pk1_ck0_col0_cpk1",
		"pk1_ck1_col1_cpk1",
		"pk3_ck3_col5_cpk1",
		"pkAll_ckAll_colAll_cpk1",
		"pk1_ck1_col1cr_cpkAll",
		"pk3_ck3_col3cr_cpkAll",
		"pk3_ck3_col5_cpkAll",
		"pkAll_ckAll_colAll_cpkAll",

		"pk1_ck0_col0_cpk1.mv",
		"pk1_ck1_col1_cpk1.mv",
		"pk3_ck3_col5_cpk1.mv",
		"pkAll_ckAll_colAll_cpk1.mv",
		"pk1_ck1_col1cr_cpkAll.mv",
		"pk3_ck3_col3cr_cpkAll.mv",
		"pk3_ck3_col5_cpkAll.mv",
		"pkAll_ckAll_colAll_cpkAll.mv",

		"pk1_ck0_col1_cpk1.mvNp",
		"pk1_ck1_col1_cpk1.mvNp",
		"pk3_ck3_col5_cpk1.mvNp",
		"pkAll_ckAll_colAll_cpk1.mvNp",
		"pk3_ck3_col5_cpkAll.mvNp",
		"pkAll_ckAll_colAll_cpkAll.mvNp",
	}
	genClusteringRangeQueryCases = []string{
		"pk1_ck1_col1_cck1",
		"pk3_ck3_col5_cck1",
		"pkAll_ckAll_colAll_cck1",
		"pk1_ck1_col1cr_cckAll",
		"pk3_ck3_col3cr_cckAll",
		"pk3_ck3_col5_cckAll",
		"pkAll_ckAll_colAll_cckAll",

		"pk1_ck1_col1_cck1.mv",
		"pk3_ck3_col5_cck1.mv",
		"pkAll_ckAll_colAll_cck1.mv",
		"pk1_ck1_col1cr_cckAll.mv",
		"pk3_ck3_col3cr_cckAll.mv",
		"pk3_ck3_col5_cckAll.mv",
		"pkAll_ckAll_colAll_cckAll.mv",

		"pk1_ck1_col1_cck1.mvNp",
		"pk3_ck3_col5_cck1.mvNp",
		"pkAll_ckAll_colAll_cck1.mvNp",
		"pk3_ck3_col5_cckAll.mvNp",
		"pkAll_ckAll_colAll_cckAll.mvNp",
	}
	genMultiplePartitionClusteringRangeQueryCases = []string{
		"pk1_ck1_col1_cpk1.cck1",
		"pk3_ck3_col5_cpk1.cck1",
		"pkAll_ckAll_colAll_cpk1.cck1",
		"pk1_ck1_col1cr_cpkAll.cck1",
		"pk3_ck3_col3cr_cpkAll.cck1",
		"pk3_ck3_col5_cpkAll.cck1",
		"pkAll_ckAll_colAll_cpkAll.cck1",

		"pk1_ck1_col1_cpk1.cckAll",
		"pk3_ck3_col5_cpk1.cckAll",
		"pkAll_ckAll_colAll_cpk1.cckAll",
		"pk1_ck1_col1cr_cpkAll.cckAll",
		"pk3_ck3_col3cr_cpkAll.cckAll",
		"pk3_ck3_col5_cpkAll.cckAll",
		"pkAll_ckAll_colAll_cpkAll.cckAll",

		"pk1_ck1_col1_cpk1.cck1.mv",
		"pk3_ck3_col5_cpk1.cck1.mv",
		"pkAll_ckAll_colAll_cpk1.cck1.mv",
		"pk1_ck1_col1cr_cpkAll.cck1.mv",
		"pk3_ck3_col3cr_cpkAll.cck1.mv",
		"pk3_ck3_col5_cpkAll.cck1.mv",
		"pkAll_ckAll_colAll_cpkAll.cck1.mv",

		"pk1_ck1_col1_cpk1.cckAll.mv",
		"pk3_ck3_col5_cpk1.cckAll.mv",
		"pkAll_ckAll_colAll_cpk1.cckAll.mv",
		"pk1_ck1_col1cr_cpkAll.cckAll.mv",
		"pk3_ck3_col3cr_cpkAll.cckAll.mv",
		"pk3_ck3_col5_cpkAll.cckAll.mv",
		"pkAll_ckAll_colAll_cpkAll.cckAll.mv",

		"pk1_ck1_col1_cpk1.cck1.mvNp",
		"pk3_ck3_col5_cpk1.cck1.mvNp",
		"pkAll_ckAll_colAll_cpk1.cck1.mvNp",
		"pk3_ck3_col5_cpkAll.cck1.mvNp",
		"pkAll_ckAll_colAll_cpkAll.cck1.mvNp",

		"pk1_ck1_col1_cpk1.cckAll.mvNp",
		"pk3_ck3_col5_cpk1.cckAll.mvNp",
		"pkAll_ckAll_colAll_cpk1.cckAll.mvNp",
		"pk3_ck3_col5_cpkAll.cckAll.mvNp",
		"pkAll_ckAll_colAll_cpkAll.cckAll.mvNp",
	}

	genSingleIndexQueryCases = []string{
		"pk1_ck0_col1_idx1",
		"pk3_ck3_col5_idx1",
		"pkAll_ckAll_colAll_idxAll",
	}
)

var (
	genDropColumnStmtCases = []string{
		"pk1_ck1_col1_delFist",
		"pkAll_ckAll_colAll_delLast",
	}
	genAddColumnStmtCases = []string{
		"pk1_ck1_col1_addSt_0",
		"pk1_ck1_col1_addSt_1",
		"pk1_ck1_col1_addSt_2",
		"pk1_ck1_col1_addSt_3",
		"pk1_ck1_col1_addSt_4",
		"pk1_ck1_col1_addSt_5",
		"pk1_ck1_col1_addSt_6",
		"pk1_ck1_col1_addSt_7",
		"pk1_ck1_col1_addSt_8",
		"pk1_ck1_col1_addSt_9",
		"pk1_ck1_col1_addSt_10",
		"pk1_ck1_col1_addSt_11",
		"pk1_ck1_col1_addSt_12",
		"pk1_ck1_col1_addSt_13",
		"pk1_ck1_col1_addSt_14",
		"pk1_ck1_col1_addSt_15",
		"pk1_ck1_col1_addSt_16",
		"pk1_ck1_col1_addSt_17",
		"pk1_ck1_col1_addSt_18",
	}
)
