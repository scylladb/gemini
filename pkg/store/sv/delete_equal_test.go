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

package sv

import (
	"reflect"
	"testing"
	"time"

	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/utils"
)

var rnd = rand.New(rand.NewSource(uint64(time.Now().UnixMilli())))

func TestEasyDeleteEqualRows(t *testing.T) {
	test := Results{
		Oracle: Result{Rows: getRandomRawRows(20, 20, 20)},
	}
	test.Test.Rows = make(RowsSV, 20)
	copy(test.Test.Rows, test.Oracle.Rows)
	test.Test.Rows[10] = rndRow(20, 20)

	expectedOracle := test.Oracle.Rows[10:]
	expectedTest := test.Test.Rows[10:]
	// Test EasyDeleteEqualRows with middle unequal row
	runEasyDeleteEqualRowsAndCheck(t, &test, expectedOracle, expectedTest, 10)

	// Test EasyDeleteEqualRows with fist unequal row
	runEasyDeleteEqualRowsAndCheck(t, &test, test.Oracle.Rows, test.Test.Rows, 0)

	test.Test.Rows[0] = test.Oracle.Rows[0]
	test.Oracle.Rows[9] = rndRow(20, 20)
	expectedOracle = test.Oracle.Rows[9:]
	expectedTest = test.Test.Rows[9:]

	// Test EasyDeleteEqualRows with last unequal row
	runEasyDeleteEqualRowsAndCheck(t, &test, expectedOracle, expectedTest, 9)

	test.Test.Rows[0] = test.Oracle.Rows[0]

	// Test EasyDeleteEqualRows with one equal row
	runEasyDeleteEqualRowsAndCheck(t, &test, RowsSV{}, RowsSV{}, 1)

	// Test EasyDeleteEqualRows with nil rows
	runEasyDeleteEqualRowsAndCheck(t, &test, RowsSV{}, RowsSV{}, 0)
}

func runEasyDeleteEqualRowsAndCheck(t *testing.T, test *Results, expectedOracle, expectedTest RowsSV, expectedDeletes int) {
	t.Helper()
	deleteCount := test.EasyEqualRowsTest()
	if !reflect.DeepEqual(test.Oracle.Rows, expectedOracle) {
		t.Fatalf("wrong EasyDeleteEqualRows work. \nreceived:%+v \nexpected:%+v", test.Oracle.Rows, expectedOracle)
	}
	if !reflect.DeepEqual(test.Test.Rows, expectedTest) {
		t.Fatalf("wrong EasyDeleteEqualRows work. \nreceived:%+v \nexpected:%+v", test.Test.Rows, expectedTest)
	}
	if deleteCount != expectedDeletes {
		t.Fatalf("wrong EasyDeleteEqualRows work. deletes count %d, but should %d ", deleteCount, expectedDeletes)
	}
}

func TestDeleteEqualRows(t *testing.T) {
	test := Results{
		Oracle: Result{Rows: getRandomRawRows(20, 20, 20)},
	}
	test.Test.Rows = make(RowsSV, 20)
	copy(test.Test.Rows, test.Oracle.Rows)
	test.Test.Rows[10] = rndRow(20, 20)
	test.Test.Rows[11] = rndRow(20, 20)
	test.Test.Rows[12] = rndRow(20, 20)

	expectedOracle := RowsSV{test.Oracle.Rows[12], test.Oracle.Rows[11], test.Oracle.Rows[10]}
	expectedTest := RowsSV{test.Test.Rows[12], test.Test.Rows[11], test.Test.Rows[10]}
	// Test DeleteEqualRows with middle unequal row
	runDeleteEqualRows(t, &test, expectedOracle, expectedTest, 17)

	test.Test.Rows[1] = test.Oracle.Rows[1]
	test.Test.Rows[2] = test.Oracle.Rows[2]
	expectedOracle = RowsSV{test.Oracle.Rows[0]}
	expectedTest = RowsSV{test.Test.Rows[0]}

	// Test DeleteEqualRows with fist unequal row
	runDeleteEqualRows(t, &test, expectedOracle, expectedTest, 2)

	test.Test.Rows[0] = test.Oracle.Rows[0]
	// Test DeleteEqualRows with one equal row
	runDeleteEqualRows(t, &test, RowsSV{}, RowsSV{}, 1)

	// Test DeleteEqualRows with nil rows
	runDeleteEqualRows(t, &test, RowsSV{}, RowsSV{}, 0)
}

func runDeleteEqualRows(t *testing.T, test *Results, expectedOracle, expectedTest RowsSV, expectedDeletes int) {
	t.Helper()
	deleteCount := test.EqualRowsTest()
	if !reflect.DeepEqual(test.Oracle.Rows, expectedOracle) {
		t.Fatalf("wrong DeleteEqualRows work. \nreceived:%+v \nexpected:%+v", test.Oracle.Rows, expectedOracle)
	}
	if !reflect.DeepEqual(test.Test.Rows, expectedTest) {
		t.Fatalf("wrong DeleteEqualRows work. \nreceived:%+v \nexpected:%+v", test.Test.Rows, expectedTest)
	}
	if deleteCount != expectedDeletes {
		t.Fatalf("wrong DeleteEqualRows work. deletes count %d, but should %d ", deleteCount, expectedDeletes)
	}
}

func getRandomRawRows(rowsCount, columns, columnLen int) RowsSV {
	out := make(RowsSV, rowsCount)
	for idx := range out {
		tmp := rndRow(columns, columnLen)
		out[idx] = tmp
	}
	return out
}

func rndRow(columns, columnLen int) RowSV {
	out := make(RowSV, columns)
	for idx := range out {
		col := utils.RandBytes(rnd, columnLen)
		out[idx] = *(*ColumnRaw)(&col)
	}
	return out
}
