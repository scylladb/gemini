// Copyright 2025 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package store

import "fmt"

type ErrorRowDifference struct {
	OracleRow       Row
	TestRow         Row
	Diff            string
	MissingInTest   []string
	MissingInOracle []string
	TestRows        int
	OracleRows      int
}

func (e ErrorRowDifference) Error() string {
	switch {
	case len(e.MissingInTest) > 0 || len(e.MissingInOracle) > 0:
		return fmt.Sprintf(
			"row count differ (missing_in_test=%v, missing_in_oracle=%v)",
			e.MissingInTest,
			e.MissingInOracle,
		)
	case e.Diff != "":
		return e.Diff
	default:
		return fmt.Sprintf(
			"row count differ (test store rows %d, oracle store rows %d)",
			e.TestRows,
			e.OracleRows,
		)
	}
}
