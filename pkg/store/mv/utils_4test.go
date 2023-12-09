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

package mv

import (
	"time"
	"unsafe"

	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/utils"
)

var rnd = rand.New(rand.NewSource(uint64(time.Now().UnixMilli())))

type testCase struct {
	elems, elemLen int
}

var testCeases = []testCase{
	{0, 0},
	{1, 0},
	{1, 1},
	{1, 10},
	{10, 10},
	{10, 0},
}

func rndDataElems(elems, elemLenMax int, old, collection bool) ([]Elem, []byte) {
	out1 := make([]Elem, elems)
	out2 := make([]byte, 0)
	elemLen := 0
	elemLenArr := unsafe.Slice((*byte)(unsafe.Pointer(&elemLen)), 4)
	if old {
		elemLenArr = unsafe.Slice((*byte)(unsafe.Pointer(&elemLen)), 2)
	}
	if collection {
		elemLen = elems
		out2 = append(out2, elemLenArr...)
	}
	for idx := range out1 {
		elemLen = rand.Intn(elemLenMax)
		rndData := ColumnRaw(utils.RandBytes(rnd, elemLen))
		out2 = append(out2, elemLenArr...)
		out2 = append(out2, rndData...)
		out1[idx] = &rndData
	}
	return out1, out2
}

func rndDataElemsMap(elems, elemLenMax int, old bool) ([]Elem, []Elem, []byte) {
	outKeys := make([]Elem, elems)
	outValues := make([]Elem, elems)
	outData := make([]byte, 0)
	elemLen := elems
	elemLenArr := unsafe.Slice((*byte)(unsafe.Pointer(&elemLen)), 4)
	if old {
		elemLenArr = unsafe.Slice((*byte)(unsafe.Pointer(&elemLen)), 2)
	}
	outData = append(outData, elemLenArr...)
	for idx := range outKeys {
		// Add key
		elemLen = rand.Intn(elemLenMax)
		rndKeyData := ColumnRaw(utils.RandBytes(rnd, elemLen))
		outData = append(outData, elemLenArr...)
		outData = append(outData, rndKeyData...)
		outKeys[idx] = &rndKeyData
		// Add value
		elemLen = rand.Intn(elemLenMax)
		rndValueData := ColumnRaw(utils.RandBytes(rnd, elemLen))
		outData = append(outData, elemLenArr...)
		outData = append(outData, rndValueData...)
		outValues[idx] = &rndValueData
	}
	if elems < 1 {
		// With correction needed, because List and Map initialization required fist elem.
		outKeys = append(outKeys, &ColumnRaw{})
		outValues = append(outValues, &ColumnRaw{})
	}
	return outKeys, outValues, outData
}

func rndRow(columns, columnLen int) RowMV {
	out := make(RowMV, columns)
	for idx := range out {
		col := utils.RandBytes(rnd, columnLen)
		out[idx] = *(*ColumnRaw)(&col)
	}
	return out
}

func rndSameRows(rowsCount, columns, columnLen int) (RowsMV, RowsMV) {
	out1 := make(RowsMV, rowsCount)
	out2 := make(RowsMV, rowsCount)
	for idx := range out1 {
		out1[idx], out2[idx] = rndSameRow(columns, columnLen)
	}
	return out1, out2
}

func rndSameRow(columns, columnLen int) (RowMV, RowMV) {
	out1 := make(RowMV, columns)
	out2 := make(RowMV, columns)
	for id := 0; id < columns; id++ {
		switch id % 5 {
		case 0:
			out1[id], out2[id] = rndSameRaw(columnLen)
		case 1:
			out1[id], out2[id] = rndSameLists(2, columnLen)
		case 2:
			out1[id], out2[id] = rndSameMaps(2, columnLen)
		case 3:
			out1[id], out2[id] = rndSameTuples(2, columnLen)
		case 4:
			out1[id], out2[id] = rndSameUDTs(2, columnLen)
		}
	}
	return out1, out2
}

func rndSameRaw(columnLen int) (ColumnRaw, ColumnRaw) {
	out1 := make(ColumnRaw, columnLen)
	for idx := range out1 {
		out1[idx] = byte(rnd.Intn(256))
	}
	out2 := make(ColumnRaw, columnLen)
	copy(out2, out1)
	return out1, out2
}

func rndSameStrings(elems, elemLen int) ([]string, []string) {
	out1 := make([]string, elems)
	for idx := range out1 {
		out1[idx] = utils.RandString(rnd, elemLen)
	}
	out2 := make([]string, elems)
	copy(out2, out1)
	return out1, out2
}

func rndSameElems(elems, elemLen int) ([]Elem, []Elem) {
	out1 := make([]Elem, elems)
	out2 := make([]Elem, elems)
	for id := range out1 {
		tmp1, tmp2 := rndSameRaw(elemLen)
		out1[id], out2[id] = &tmp1, &tmp2
	}
	return out1, out2
}

func rndSameLists(elems, elemLen int) (List, List) {
	var out1, out2 List
	out1, out2 = rndSameElems(elems, elemLen)
	return out1, out2
}

func rndSameMaps(elems, elemLen int) (Map, Map) {
	var out1, out2 Map
	out1.Keys, out2.Keys = rndSameElems(elems, elemLen)
	out1.Values, out2.Values = rndSameElems(elems, elemLen)
	return out1, out2
}

func rndSameTuples(elems, elemLen int) (Tuple, Tuple) {
	var out1, out2 Tuple
	out1, out2 = rndSameElems(elems, elemLen)
	return out1, out2
}

func rndSameUDTs(elems, elemLen int) (UDT, UDT) {
	var out1, out2 UDT
	out1.Names, out2.Names = rndSameStrings(elems, elemLen)
	out1.Values, out2.Values = rndSameElems(elems, elemLen)
	return out1, out2
}
