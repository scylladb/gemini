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

const (
	testDirPath = "./test_expected_data/"
)

var (
	// TODO: complex types excepted from all cases until it testing
	// TODO: TYPE_TIME excepted from pk keys cases until fix issue #321
	partitionKeysCases = map[string][]Type{
		"pk1": {TYPE_BIGINT},
		"pk3": {TYPE_BIGINT, TYPE_FLOAT, TYPE_INET},
		"pkAll": {
			TYPE_ASCII, TYPE_BIGINT, TYPE_BLOB, TYPE_BOOLEAN, TYPE_DATE, TYPE_DECIMAL, TYPE_DOUBLE, TYPE_FLOAT,
			TYPE_INET, TYPE_INT, TYPE_SMALLINT, TYPE_TEXT, TYPE_TIMESTAMP, TYPE_TIMEUUID, TYPE_TINYINT, TYPE_UUID, TYPE_VARCHAR, TYPE_VARINT,
		},
	}

	clusteringKeysCases = map[string][]Type{
		"ck0": {},
		"ck1": {TYPE_DATE},
		"ck3": {TYPE_ASCII, TYPE_DATE, TYPE_DECIMAL},
		"ckAll": {
			TYPE_ASCII, TYPE_BIGINT, TYPE_BLOB, TYPE_BOOLEAN, TYPE_DATE, TYPE_DECIMAL, TYPE_DOUBLE, TYPE_FLOAT,
			TYPE_INET, TYPE_INT, TYPE_SMALLINT, TYPE_TEXT, TYPE_TIME, TYPE_TIMESTAMP, TYPE_TIMEUUID, TYPE_TINYINT, TYPE_UUID, TYPE_VARCHAR, TYPE_VARINT,
		},
	}

	// TODO: counterType excepted from columns cases until it testing
	columnsCases = map[string][]Type{
		"col0":   {},
		"col1":   {TYPE_DATE},
		"col5":   {TYPE_ASCII, TYPE_DATE, TYPE_BLOB, TYPE_BIGINT, TYPE_FLOAT},
		"col5c":  {TYPE_ASCII, &mapType, TYPE_BLOB, &tupleType, TYPE_FLOAT},
		"col1cr": {&counterType},
		"col3cr": {&counterType, &counterType, &counterType},
		"colAll": {
			TYPE_DURATION, TYPE_ASCII, TYPE_BIGINT, TYPE_BLOB, TYPE_BOOLEAN, TYPE_DATE, TYPE_DECIMAL, TYPE_DOUBLE, TYPE_FLOAT,
			TYPE_INET, TYPE_INT, TYPE_SMALLINT, TYPE_TEXT, TYPE_TIME, TYPE_TIMESTAMP, TYPE_TIMEUUID, TYPE_TINYINT, TYPE_UUID, TYPE_VARCHAR, TYPE_VARINT,
		},
	}

	optionsCases = map[string][]string{
		"MV":     {"MV"},
		"lwt":    {"lwt"},
		"lwt_MV": {"lwt", "MV"},
	}

	counterType CounterType
	tupleType   TupleType
	mapType     MapType

	updateExpected = flag.Bool("update-expected", false, "make test to update expected results")
)
