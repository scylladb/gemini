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
package jobs_test

import (
	"testing"

	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/jobs"
	"github.com/scylladb/gemini/pkg/typedef"
)

var genCheckfullCases = []int{
	jobs.GenSinglePartitionID,
	jobs.GenSinglePartitionMvID,
	jobs.GenMultiplePartitionID,
	jobs.GenMultiplePartitionMvID,
	jobs.GenClusteringRangeID,
	jobs.GenClusteringRangeMvID,
	jobs.GenMultiplePartitionClusteringRangeID,
	jobs.GenMultiplePartitionClusteringRangeMvID,
	jobs.GenSingleIndexQueryID,
}

var genCheckNoIndex = []int{
	jobs.GenSinglePartitionID,
	jobs.GenSinglePartitionMvID,
	jobs.GenMultiplePartitionID,
	jobs.GenMultiplePartitionMvID,
	jobs.GenClusteringRangeID,
	jobs.GenClusteringRangeMvID,
	jobs.GenMultiplePartitionClusteringRangeID,
	jobs.GenMultiplePartitionClusteringRangeMvID,
}

var genCheckNoMV = []int{
	jobs.GenSinglePartitionID,
	jobs.GenMultiplePartitionID,
	jobs.GenClusteringRangeID,
	jobs.GenMultiplePartitionClusteringRangeID,
	jobs.GenSingleIndexQueryID,
}

var genCheckNoClustering = []int{
	jobs.GenSinglePartitionID,
	jobs.GenSinglePartitionMvID,
	jobs.GenMultiplePartitionID,
	jobs.GenMultiplePartitionMvID,
	jobs.GenSingleIndexQueryID,
}

var genCheckMin = []int{
	jobs.GenSinglePartitionID,
	jobs.GenMultiplePartitionID,
}

type comparer struct {
	expected []int
	received typedef.CasesInfo
}

func TestGetFuncCases(t *testing.T) {
	tableFull := getTestTable()

	tableNoIndexes := getTestTable()
	tableNoIndexes.Indexes = nil

	tableNoMV := getTestTable()
	tableNoMV.MaterializedViews = nil

	tableNoClustering := getTestTable()
	tableNoClustering.ClusteringKeys = nil

	tableMin := getTestTable()
	tableMin.Indexes = nil
	tableMin.MaterializedViews = nil
	tableMin.ClusteringKeys = nil

	genCheckList := map[string]comparer{
		"genCheck_fullCases":    {received: typedef.UpdateFuncCases(&tableFull, jobs.GenCheckStmtConditions, jobs.GenCheckStmtRatios), expected: genCheckfullCases},
		"genCheck_NoIndex":      {received: typedef.UpdateFuncCases(&tableNoIndexes, jobs.GenCheckStmtConditions, jobs.GenCheckStmtRatios), expected: genCheckNoIndex},
		"genCheck_NoMV":         {received: typedef.UpdateFuncCases(&tableNoMV, jobs.GenCheckStmtConditions, jobs.GenCheckStmtRatios), expected: genCheckNoMV},
		"genCheck_NoClustering": {received: typedef.UpdateFuncCases(&tableNoClustering, jobs.GenCheckStmtConditions, jobs.GenCheckStmtRatios), expected: genCheckNoClustering},
		"genCheck_Min":          {received: typedef.UpdateFuncCases(&tableMin, jobs.GenCheckStmtConditions, jobs.GenCheckStmtRatios), expected: genCheckMin},
	}
	compareResults(t, genCheckList)

	funcsList := typedef.UpdateFuncCases(&tableFull, jobs.GenCheckStmtConditions, jobs.GenCheckStmtRatios)
	idx := funcsList.RandomCase(rand.New(rand.NewSource(123)))
	_ = idx
}

func compareResults(t *testing.T, results map[string]comparer) {
	for caseName := range results {
		checkPresenceCases(t, caseName, results[caseName].received, results[caseName].expected...)
	}
}

func checkPresenceCases(t *testing.T, caseName string, funcs typedef.CasesInfo, expected ...int) {
	received := make([]int, 0, len(expected))
	for i := range expected {
		for j := range funcs.List {
			if expected[i] == funcs.List[j].ID {
				received = append(received, expected[i])
				break
			}
		}
	}
	if len(received) != len(expected) {
		t.Errorf("wrong function cases for case:%s \nexpected:%v \nreceived:%v ", caseName, expected, received)
	}
}

func getTestTable() typedef.Table {
	col := typedef.ColumnDef{
		Name: "col_0",
		Type: typedef.TYPE_INT,
	}
	cols := typedef.Columns{&col, &col}
	index := typedef.IndexDef{
		Name:   "id_1",
		Column: cols[0],
	}
	return typedef.Table{
		Name:              "tb1",
		PartitionKeys:     cols,
		ClusteringKeys:    cols,
		Columns:           cols,
		Indexes:           typedef.Indexes{index, index},
		MaterializedViews: generators.CreateMaterializedViews(cols, "mv1", cols, cols),
		KnownIssues: map[string]bool{
			typedef.KnownIssuesJSONWithTuples: true,
		},
		TableOptions: nil,
	}
}
