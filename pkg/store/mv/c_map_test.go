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

func TestMap_UnmarshalCQL(t *testing.T) {
	errorMsg := "wrong Map.UnmarshalCQL work:"
	testColumn := Map{}
	var err error
	testsCount := 1000
	old := true
	for i := 0; i < testsCount; i++ {
		elems := 20
		maxElemLen := 10
		if i == 0 {
			elems = 0
			maxElemLen = 0
		}
		if i/2 > testsCount/2 {
			old = false
			elems = 0
			maxElemLen = 0
		}
		expectedKeys, expectedValues, data := rndDataElemsMap(elems, maxElemLen, old)
		// Map initialization.
		testColumn.Keys = make([]Elem, 1)
		testColumn.Values = make([]Elem, 1)
		tmpKey := ColumnRaw("")
		tmpValue := ColumnRaw("")
		testColumn.Keys[0] = &tmpKey
		testColumn.Values[0] = &tmpValue
		// Unmarshall.
		if old {
			err = testColumn.oldUnmarshalCQL(gocql.CollectionType{}, data)
		} else {
			err = testColumn.newUnmarshalCQL(gocql.CollectionType{}, data)
		}

		// Check results.
		if err != nil {
			t.Fatalf("%s error:%s", errorMsg, err)
		}
		// Check Keys
		if len(testColumn.Keys) != len(expectedKeys) {
			t.Fatalf("%s\nreceived keys len:%d \nexpected keys len:%d", errorMsg, len(testColumn.Keys), len(expectedKeys))
		}
		for idx := range testColumn.Keys {
			if !reflect.DeepEqual(expectedKeys[idx], testColumn.Keys[idx]) {
				t.Fatalf("%s\nreceived keys:%+v \nexpected keys:%+v", errorMsg, testColumn.Keys[idx], expectedKeys[idx])
			}
		}
		// Check Values
		if len(testColumn.Values) != len(expectedValues) {
			t.Fatalf("%s\nreceived keys len:%d \nexpected keys len:%d", errorMsg, len(testColumn.Values), len(expectedValues))
		}
		for idx := range testColumn.Values {
			if !reflect.DeepEqual(expectedValues[idx], testColumn.Values[idx]) {
				t.Fatalf("%s\nreceived keys:%+v \nexpected keys:%+v", errorMsg, testColumn.Values[idx], expectedValues[idx])
			}
		}
	}
}

func TestMap_Equal(t *testing.T) {
	var testColumn1, testColumn2 Map

	for i := range testCeases {
		testColumn1.Keys, testColumn2.Keys = rndSameElems(testCeases[i].elems, testCeases[i].elemLen)
		testColumn1.Values, testColumn2.Values = rndSameElems(testCeases[i].elems, testCeases[i].elemLen)
		// EqualColumn test on equal
		if !testColumn1.EqualColumn(testColumn2) {
			t.Fatal("Map.EqualColumn should return true")
		}
		// EqualElem test on equal
		if !testColumn1.EqualElem(&testColumn2) {
			t.Fatal("Map.EqualElem should return true")
		}

		// Corrupt values
		tmp := ColumnRaw("123")
		testColumn2.Values = []Elem{
			&tmp,
		}
		// EqualColumn test on unequal
		if testColumn1.EqualColumn(testColumn2) {
			t.Fatal("Map.EqualColumn should return false")
		}
		// EqualElem test on unequal
		if testColumn1.EqualElem(&testColumn2) {
			t.Fatal("Map.EqualElem should return false")
		}

		// Corrupt keys
		testColumn1.Values, testColumn2.Values = rndSameElems(testCeases[i].elems, testCeases[i].elemLen)
		testColumn2.Keys = []Elem{
			&tmp,
		}
		// EqualColumn test on unequal
		if testColumn1.EqualColumn(testColumn2) {
			t.Fatal("Map.EqualColumn should return false")
		}
		// EqualElem test on unequal
		if testColumn1.EqualElem(&testColumn2) {
			t.Fatal("Map.EqualElem should return false")
		}
	}
}
