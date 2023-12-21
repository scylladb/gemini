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

func TestList_UnmarshalCQL(t *testing.T) {
	errorMsg := "wrong List.UnmarshalCQL work:"
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
		expected, data := rndDataElems(elems, maxElemLen, old, true)
		// List initialization.
		testColumn := make(List, 1)
		tmp := ColumnRaw("")
		testColumn[0] = &tmp
		// Unmarshall.
		if old {
			err = testColumn.unmarshalOld(gocql.CollectionType{}, data)
		} else {
			err = testColumn.unmarshalNew(gocql.CollectionType{}, data)
		}

		// Check results.
		if err != nil {
			t.Fatalf("%s error:%s", errorMsg, err)
		}
		// With correction needed, because List and Map initialization required fist elem
		if len(expected) == 0 && len(testColumn) == 1 && testColumn[0].EqualColumn(ColumnRaw("")) {
			continue
		}
		if len(testColumn) != len(expected) {
			t.Fatalf("%s\nreceived len:%d \nexpected len:%d", errorMsg, len(testColumn), len(expected))
		}
		for idx := range testColumn {
			if !reflect.DeepEqual(expected[idx], testColumn[idx]) {
				t.Fatalf("%s\nreceived:%+v \nexpected:%+v", errorMsg, testColumn[idx], expected[idx])
			}
		}
	}
}

func TestList_Equal(t *testing.T) {
	testColumn1 := make(List, 0)
	testColumn2 := make(List, 0)
	for i := range testCeases {
		testColumn1, testColumn2 = rndSameElems(testCeases[i].elems, testCeases[i].elemLen)
		// EqualColumn test on equal
		if !testColumn1.EqualColumn(testColumn2) {
			t.Fatal("List.EqualColumn should return true")
		}
		// EqualElem test on equal
		if !testColumn1.EqualElem(&testColumn2) {
			t.Fatal("List.EqualElem should return true")
		}
		tmp := ColumnRaw("123")
		testColumn2 = []Elem{
			&tmp,
		}
		// EqualColumn test on unequal
		if testColumn1.EqualColumn(testColumn2) {
			t.Fatal("List.EqualColumn should return false")
		}
		// EqualElem test on unequal
		if testColumn1.EqualElem(&testColumn2) {
			t.Fatal("List.EqualElem should return false")
		}
	}
	_ = testColumn1
	_ = testColumn2
}
