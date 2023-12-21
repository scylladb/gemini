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

package mv

import (
	"reflect"
	"testing"
)

func TestEasyDeleteEqualRows(t *testing.T) {
	var test Results
	test.Oracle.Rows, test.Test.Rows = rndSameRows(20, 20, 20)
	test.Test.Rows[10] = rndRow(20, 20)

	expectedOracle := test.Oracle.Rows[10:]
	expectedTest := test.Test.Rows[10:]
	// Test EasyDeleteEqualRows with middle unequal row
	runEasyDeleteEqualRowsTest(t, &test, expectedOracle, expectedTest, 10)

	// Test EasyDeleteEqualRows with fist unequal row
	runEasyDeleteEqualRowsTest(t, &test, test.Oracle.Rows, test.Test.Rows, 0)

	test.Test.Rows[0] = test.Oracle.Rows[0]
	test.Oracle.Rows[9] = rndRow(20, 20)
	expectedOracle = test.Oracle.Rows[9:]
	expectedTest = test.Test.Rows[9:]

	// Test EasyDeleteEqualRows with last unequal row
	runEasyDeleteEqualRowsTest(t, &test, expectedOracle, expectedTest, 9)

	test.Test.Rows[0] = test.Oracle.Rows[0]

	// Test EasyDeleteEqualRows with one equal row
	runEasyDeleteEqualRowsTest(t, &test, RowsMV{}, RowsMV{}, 1)

	// Test EasyDeleteEqualRows with nil rows
	runEasyDeleteEqualRowsTest(t, &test, RowsMV{}, RowsMV{}, 0)
}

func TestDeleteEqualRows(t *testing.T) {
	var test Results
	test.Oracle.Rows, test.Test.Rows = rndSameRows(20, 20, 20)
	test.Test.Rows[10] = rndRow(20, 20)
	test.Test.Rows[11] = rndRow(20, 20)
	test.Test.Rows[12] = rndRow(20, 20)

	expectedOracle := RowsMV{test.Oracle.Rows[12], test.Oracle.Rows[11], test.Oracle.Rows[10]}
	expectedTest := RowsMV{test.Test.Rows[12], test.Test.Rows[11], test.Test.Rows[10]}
	// Test DeleteEqualRows with middle unequal row
	runDeleteEqualRowsTest(t, &test, expectedOracle, expectedTest, 17)

	test.Test.Rows[1] = test.Oracle.Rows[1]
	test.Test.Rows[2] = test.Oracle.Rows[2]
	expectedOracle = RowsMV{test.Oracle.Rows[0]}
	expectedTest = RowsMV{test.Test.Rows[0]}

	// Test DeleteEqualRows with fist unequal row
	runDeleteEqualRowsTest(t, &test, expectedOracle, expectedTest, 2)

	test.Test.Rows[0] = test.Oracle.Rows[0]
	// Test DeleteEqualRows with one equal row
	runDeleteEqualRowsTest(t, &test, RowsMV{}, RowsMV{}, 1)

	// Test DeleteEqualRows with nil rows
	runDeleteEqualRowsTest(t, &test, RowsMV{}, RowsMV{}, 0)
}

func runEasyDeleteEqualRowsTest(t *testing.T, test *Results, expectedOracle, expectedTest RowsMV, expectedDeletes int) {
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

func runDeleteEqualRowsTest(t *testing.T, test *Results, expectedOracle, expectedTest RowsMV, expectedDeletes int) {
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
