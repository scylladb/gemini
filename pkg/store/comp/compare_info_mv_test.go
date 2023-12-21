// Copyright 2023 ScyllaDB
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

package comp

import (
	"fmt"
	"testing"

	"github.com/scylladb/gemini/pkg/store/mv"
	"github.com/scylladb/gemini/pkg/store/sv"
)

func TestGetCompareInfoMV(t *testing.T) {
	var test mv.Results
	for idx := range testCases {
		tests := 2
		if testCases[idx].diffs > 0 {
			tests = 4
		}
		for tests > 0 {
			test.Test.Rows, test.Oracle.Rows, test.Test.Types, test.Oracle.Types = rndSameRowsMV(testCases[idx].test, testCases[idx].oracle)
			// Add names
			if len(test.Test.Rows) > 0 {
				test.Test.Names = make([]string, len(test.Test.Rows[0]))
				for col := range test.Test.Rows[0] {
					test.Test.Names[col] = fmt.Sprintf("col%d", col)
				}
			}
			if len(test.Oracle.Rows) > 0 {
				test.Oracle.Names = make([]string, len(test.Oracle.Rows[0]))
				for col := range test.Oracle.Rows[0] {
					test.Oracle.Names[col] = fmt.Sprintf("col%d", col)
				}
			}
			if testCases[idx].diffs > 0 {
				if tests%2 == 0 {
					corruptRows(&test.Test.Rows, testCases[idx].diffs)
				} else {
					corruptRows(&test.Oracle.Rows, testCases[idx].diffs)
				}
			}
			result := Info{}
			errFuncName := "GetCompareInfoDetailed"
			if tests%2 == 0 {
				errFuncName = "GetCompareInfoSimple"
				result = GetCompareInfoSimple(&test)
			} else {
				result = GetCompareInfoDetailed(&test)
			}
			if len(result) > 0 && !testCases[idx].haveDif {
				t.Fatalf("wrong %s work. test case:%+v \nresult should be empty, but have:%s\n"+
					"mv.Results.Test.Rows:%+v\n"+
					"mv.Results.Oracle.Rows:%+v", errFuncName, testCases[idx], result, test.Test.Rows, test.Oracle.Rows)
			}
			if len(result) == 0 && testCases[idx].haveDif {
				t.Fatalf("wrong %s work. test case:%+v \nresult should be not empty\n"+
					"mv.Results.Test.Rows:%+v\n"+
					"mv.Results.Oracle.Rows:%+v", errFuncName, testCases[idx], test.Test.Rows, test.Oracle.Rows)
			}
			if len(result) > 0 {
				fmt.Printf("%s from mv.Results of test case:%+v\n", errFuncName, testCases[idx])
				fmt.Println(result.String())
			}
			tests--
		}
	}
}

func TestGetCompareInfoSV(t *testing.T) {
	var test sv.Results
	for idx := range testCases {
		tests := 2
		if testCases[idx].diffs > 0 {
			tests = 4
		}
		for tests > 0 {
			test.Test.Rows, test.Oracle.Rows, test.Test.Types, test.Oracle.Types = rndSameRowsSV(testCases[idx].test, testCases[idx].oracle)
			// Add names
			if len(test.Test.Rows) > 0 {
				test.Test.Names = make([]string, len(test.Test.Rows[0]))
				for col := range test.Test.Rows[0] {
					test.Test.Names[col] = fmt.Sprintf("col%d", col)
				}
			}
			if len(test.Oracle.Rows) > 0 {
				test.Oracle.Names = make([]string, len(test.Oracle.Rows[0]))
				for col := range test.Oracle.Rows[0] {
					test.Oracle.Names[col] = fmt.Sprintf("col%d", col)
				}
			}
			if testCases[idx].diffs > 0 {
				if tests%2 == 0 {
					corruptRowsSV(&test.Test.Rows, testCases[idx].diffs)
				} else {
					corruptRowsSV(&test.Oracle.Rows, testCases[idx].diffs)
				}
			}
			result := Info{}
			errFuncName := "GetCompareInfoDetailed"
			if tests%2 == 0 {
				errFuncName = "GetCompareInfoSimple"
				result = GetCompareInfoSimple(&test)
			} else {
				result = GetCompareInfoDetailed(&test)
			}
			if len(result) > 0 && !testCases[idx].haveDif {
				t.Fatalf("wrong %s work. test case:%+v \nresult should be empty, but have:%s\n"+
					"mv.Results.Test.Rows:%+v\n"+
					"mv.Results.Oracle.Rows:%+v", errFuncName, testCases[idx], result, test.Test.Rows, test.Oracle.Rows)
			}
			if len(result) == 0 && testCases[idx].haveDif {
				t.Fatalf("wrong %s work. test case:%+v \nresult should be not empty\n"+
					"mv.Results.Test.Rows:%+v\n"+
					"mv.Results.Oracle.Rows:%+v", errFuncName, testCases[idx], test.Test.Rows, test.Oracle.Rows)
			}
			if len(result) > 0 {
				fmt.Printf("%s from mv.Results of test case:%+v\n", errFuncName, testCases[idx])
				fmt.Println(result.String())
			}
			tests--
		}
	}
}
