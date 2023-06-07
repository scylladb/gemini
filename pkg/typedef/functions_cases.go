// Copyright 2019 ScyllaDB
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

package typedef

import (
	"sort"

	"golang.org/x/exp/rand"
)

type (
	Functions struct {
		Check  CasesInfo
		Mutate CasesInfo
		DDL    CasesInfo
	}
	CasesInfo struct {
		List          Cases
		SumCasesRatio int
	}
	Cases []Case
	Case  struct {
		ID   int
		From int
		To   int
	}

	// CasesConditions contains functions that defines the conditions under which case functions can be executed
	//
	// If function return true, it means that case function can be executed for this table
	CasesConditions map[int]func(table *Table) bool

	// CasesRatios contains ratios that defines case function execution ratio in relation to other case functions
	//
	// If function #1 have ratio 1 and function #2 have ratio 9 sumRatio will be 10
	//
	// It means that from 10 executions function #1 will be executed 1 times and function #2 - 9 times
	CasesRatios map[int]int
)

func UpdateFuncCases(table *Table, conditions CasesConditions, ratios CasesRatios) CasesInfo {
	cases := make(Cases, 0, len(conditions))
	for CaseNum, condition := range conditions {
		if condition(table) {
			cases = append(cases, Case{
				ID: CaseNum,
			})
		}
	}
	sort.Slice(cases, func(i, j int) bool {
		if ratios[cases[i].ID] == ratios[cases[j].ID] {
			return cases[i].ID < cases[j].ID
		}
		return ratios[cases[i].ID] > ratios[cases[j].ID]
	})
	sumRatio := 0
	for idx := range cases {
		cases[idx].From = sumRatio
		sumRatio += ratios[cases[idx].ID]
		cases[idx].To = sumRatio
	}
	return CasesInfo{List: cases, SumCasesRatio: sumRatio}
}

func (c CasesInfo) RandomCase(rnd *rand.Rand) int {
	spin := rnd.Intn(c.SumCasesRatio - 1)
	for idx := range c.List {
		if spin >= c.List[idx].From && spin < c.List[idx].To {
			return c.List[idx].ID
		}
	}
	return 0
}
