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

	"github.com/gocql/gocql"
)

func TestTuple_UnmarshalCQL(t *testing.T) {
	errorMsg := "wrong Tuple.UnmarshalCQL work:"

	testsCount := 1000
	for i := 0; i < testsCount; i++ {
		elems := 20
		maxElemLen := 10
		if i == 0 {
			elems = 0
			maxElemLen = 0
		}
		expected, data := rndDataElems(elems, maxElemLen, false, false)
		// Tuple initialization.
		testColumn := make(Tuple, elems)
		for idx := range testColumn {
			tmp := ColumnRaw("")
			testColumn[idx] = &tmp
		}
		// Unmarshall.
		err := testColumn.UnmarshalCQL(gocql.TupleTypeInfo{Elems: make([]gocql.TypeInfo, elems)}, data)
		// Check results.
		if err != nil {
			t.Fatalf("%s error:%s", errorMsg, err)
		}
		if len(testColumn) != len(expected) {
			t.Fatalf("%s\nreceived len:%d \nexpected len:%d", errorMsg, len(testColumn), len(expected))
		}
		for idx := range testColumn {
			if !reflect.DeepEqual(expected[idx], testColumn[idx]) {
				t.Fatalf("%s\nreceived:%+v \nexpected:%+v", errorMsg, testColumn[idx], expected[idx])
			}
		}
		testColumn = make(Tuple, 0)
	}
}

func TestTuple_Equal(t *testing.T) {
	testColumn1 := make(Tuple, 0)
	testColumn2 := make(Tuple, 0)
	for i := range testCeases {
		testColumn1, testColumn2 = rndSameElems(testCeases[i].elems, testCeases[i].elemLen)
		// EqualColumn test on equal
		if !testColumn1.EqualColumn(testColumn2) {
			t.Fatal("Tuple.EqualColumn should return true")
		}
		// EqualElem test on equal
		if !testColumn1.EqualElem(&testColumn2) {
			t.Fatal("Tuple.EqualElem should return true")
		}
		tmp := ColumnRaw("123")
		testColumn2 = []Elem{
			&tmp,
		}
		// EqualColumn test on unequal
		if testColumn1.EqualColumn(testColumn2) {
			t.Fatal("Tuple.EqualColumn should return false")
		}
		// EqualElem test on unequal
		if testColumn1.EqualElem(&testColumn2) {
			t.Fatal("Tuple.EqualElem should return false")
		}
	}
	_ = testColumn1
	_ = testColumn2
}
