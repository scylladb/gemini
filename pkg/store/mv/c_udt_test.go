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

func TestUDT_UnmarshalCQL(t *testing.T) {
	errorMsg := "wrong UDT.UnmarshalCQL work:"
	testColumn := UDT{}
	testsCount := 1000
	for i := 0; i < testsCount; i++ {
		elems := 20
		maxElemLen := 10
		if i == 0 {
			elems = 0
			maxElemLen = 0
		}
		expected, data := rndDataElems(elems, maxElemLen, false, false)
		// UDT initialization.
		testColumn.Names = make([]string, elems)
		testColumn.Values = make([]Elem, elems)
		for idx := range testColumn.Values {
			tmp := ColumnRaw("")
			testColumn.Values[idx] = &tmp
		}
		// Unmarshall.
		err := testColumn.UnmarshalCQL(gocql.UDTTypeInfo{Elements: make([]gocql.UDTField, elems)}, data)
		// Check results.
		if err != nil {
			t.Fatalf("%s error:%s", errorMsg, err)
		}
		if len(testColumn.Values) != len(expected) {
			t.Fatalf("%s\nreceived len:%d \nexpected len:%d", errorMsg, len(testColumn.Values), len(expected))
		}
		for idx := range testColumn.Values {
			if !reflect.DeepEqual(expected[idx], testColumn.Values[idx]) {
				t.Fatalf("%s\nreceived:%+v \nexpected:%+v", errorMsg, testColumn.Values[idx], expected[idx])
			}
		}
		testColumn.Names = make([]string, 0)
		testColumn.Values = make([]Elem, 0)
	}
}

func TestUDT_Equal(t *testing.T) {
	var testColumn1, testColumn2 UDT
	for i := range testCeases {
		testColumn1.Names, testColumn2.Names = rndSameStrings(testCeases[i].elems, testCeases[i].elemLen)
		testColumn1.Values, testColumn2.Values = rndSameElems(testCeases[i].elems, testCeases[i].elemLen)
		// EqualColumn test on equal
		if !testColumn1.EqualColumn(testColumn2) {
			t.Fatal("UDT.EqualColumn should return true")
		}
		// EqualElem test on equal
		if !testColumn1.EqualElem(&testColumn2) {
			t.Fatal("UDT.EqualElem should return true")
		}
		// Corrupt values
		tmp := ColumnRaw("123")
		testColumn2.Values = []Elem{
			&tmp,
		}
		// EqualColumn test on unequal
		if testColumn1.EqualColumn(testColumn2) {
			t.Fatal("UDT.EqualColumn should return false")
		}
		// EqualElem test on unequal
		if testColumn1.EqualElem(&testColumn2) {
			t.Fatal("UDT.EqualElem should return false")
		}

		// Corrupt names
		testColumn1.Values, testColumn2.Values = rndSameElems(testCeases[i].elems, testCeases[i].elemLen)
		testColumn2.Names = []string{
			"corrupt names",
		}
		// EqualColumn test on unequal
		if testColumn1.EqualColumn(testColumn2) {
			t.Fatal("UDT.EqualColumn should return false")
		}
		// EqualElem test on unequal
		if testColumn1.EqualElem(&testColumn2) {
			t.Fatal("UDT.EqualElem should return false")
		}
	}
}
