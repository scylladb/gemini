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
	"time"

	"github.com/gocql/gocql"
	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/store/mv"
	"github.com/scylladb/gemini/pkg/store/sv"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

var rnd = rand.New(rand.NewSource(uint64(time.Now().UnixMilli())))

type testCase struct {
	test, oracle, diffs int
	haveDif             bool
}

var testCases = []testCase{
	{test: 0, oracle: 0, diffs: 0, haveDif: false},
	{test: 0, oracle: 1, diffs: 0, haveDif: true},
	{test: 0, oracle: 10, diffs: 0, haveDif: true},
	{test: 1, oracle: 0, diffs: 0, haveDif: true},
	{test: 1, oracle: 1, diffs: 0, haveDif: false},
	{test: 1, oracle: 1, diffs: 1, haveDif: true},
	{test: 1, oracle: 10, diffs: 0, haveDif: true},
	{test: 1, oracle: 10, diffs: 1, haveDif: true},
	{test: 10, oracle: 0, diffs: 0, haveDif: true},
	{test: 10, oracle: 1, diffs: 0, haveDif: true},
	{test: 10, oracle: 1, diffs: 1, haveDif: true},
	{test: 10, oracle: 10, diffs: 0, haveDif: false},
	{test: 10, oracle: 10, diffs: 1, haveDif: true},
	{test: 10, oracle: 10, diffs: 5, haveDif: true},
	{test: 10, oracle: 10, diffs: 10, haveDif: true},
}

func corruptRows(rows *mv.RowsMV, diffs int) {
	corrupted := make(map[int]struct{})
	corruptCols := [2][]int{
		{0, 1, 2},
		{4, 5, 6},
	}
	corruptType := 0
	for diffs > 0 {
		row := rnd.Intn(len(*rows))
		_, have := corrupted[row]
		if have {
			continue
		}
		if corruptType == 0 {
			corruptType = 1
		} else {
			corruptType = 0
		}
		for _, idx := range corruptCols[corruptType] {
			switch (*rows)[row][idx].(type) {
			case mv.ColumnRaw:
				(*rows)[row][idx], _ = rndSameRaw(19)
			case mv.List:
				(*rows)[row][idx], _, _, _ = rndSameLists(2, 19)
			case mv.Map:
				(*rows)[row][idx], _, _, _ = rndSameMaps(2, 19)
			case mv.Tuple:
				(*rows)[row][idx], _, _, _ = rndSameTuples(2, 19)
			case mv.UDT:
				(*rows)[row][idx], _, _, _ = rndSameUDTs(2, 19)
			}
		}
		corrupted[row] = struct{}{}
		diffs--
	}
	_ = corrupted
}

func corruptRowsSV(rows *sv.RowsSV, diffs int) {
	corrupted := make(map[int]struct{})
	corruptCols := [2][]int{
		{0, 1, 2},
		{4, 5, 6},
	}
	corruptType := 0
	for diffs > 0 {
		row := rnd.Intn(len(*rows))
		_, have := corrupted[row]
		if have {
			continue
		}
		if corruptType == 0 {
			corruptType = 1
		} else {
			corruptType = 0
		}
		for _, idx := range corruptCols[corruptType] {
			(*rows)[row][idx], _ = rndSameRawSV(19)
		}
		corrupted[row] = struct{}{}
		diffs--
	}
	_ = corrupted
}

func rndSameRowsMV(test, oracle int) (mv.RowsMV, mv.RowsMV, []gocql.TypeInfo, []gocql.TypeInfo) {
	testRows := make(mv.RowsMV, test)
	oracleRows := make(mv.RowsMV, oracle)
	testTypes := make([]gocql.TypeInfo, 20)
	oracleTypes := make([]gocql.TypeInfo, 20)
	list := oracleRows
	if test > oracle {
		list = testRows
	}
	oracle--
	test--
	for range list {
		switch {
		case oracle < 0:
			testRows[test], _, testTypes, _ = rndSameRow(20, 20)
			test--
		case test < 0:
			_, oracleRows[oracle], _, oracleTypes = rndSameRow(20, 20)
			oracle--
		default:
			testRows[test], oracleRows[oracle], testTypes, oracleTypes = rndSameRow(20, 20)
			test--
			oracle--
		}
	}

	return testRows, oracleRows, testTypes, oracleTypes
}

func rndSameRowsSV(test, oracle int) (sv.RowsSV, sv.RowsSV, []gocql.TypeInfo, []gocql.TypeInfo) {
	testRows := make(sv.RowsSV, test)
	oracleRows := make(sv.RowsSV, oracle)
	testTypes := make([]gocql.TypeInfo, 20)
	oracleTypes := make([]gocql.TypeInfo, 20)
	list := oracleRows
	if test > oracle {
		list = testRows
	}
	for id := range testTypes {
		testTypes[id] = allTypes[gocql.TypeText]
		oracleTypes[id] = allTypes[gocql.TypeText]
	}
	oracle--
	test--
	for range list {
		switch {
		case oracle < 0:
			testRows[test], _ = rndSameRowSV(20, 20)
			test--
		case test < 0:
			_, oracleRows[oracle] = rndSameRowSV(20, 20)
			oracle--
		default:
			testRows[test], oracleRows[oracle] = rndSameRowSV(20, 20)
			test--
			oracle--
		}
	}

	return testRows, oracleRows, testTypes, oracleTypes
}

var allTypes = typedef.GetGoCQLTypeMap()

func rndSameRow(columns, columnLen int) (mv.RowMV, mv.RowMV, []gocql.TypeInfo, []gocql.TypeInfo) {
	out1 := make(mv.RowMV, columns)
	out2 := make(mv.RowMV, columns)
	out1Type, out2Type := make([]gocql.TypeInfo, columns), make([]gocql.TypeInfo, columns)
	for id := 0; id < columns; id++ {
		switch id % 5 {
		case 0:
			out1[id], out2[id] = rndSameRaw(columnLen)
			out1Type[id], out2Type[id] = allTypes[gocql.TypeText], allTypes[gocql.TypeText]
		case 1:
			out1[id], out2[id], out1Type[id], out2Type[id] = rndSameLists(2, columnLen)
		case 2:
			out1[id], out2[id], out1Type[id], out2Type[id] = rndSameMaps(2, columnLen)
		case 3:
			out1[id], out2[id], out1Type[id], out2Type[id] = rndSameTuples(2, columnLen)
		case 4:
			out1[id], out2[id], out1Type[id], out2Type[id] = rndSameUDTs(2, columnLen)
		}
	}
	return out1, out2, out1Type, out2Type
}

func rndSameRowSV(columns, columnLen int) (sv.RowSV, sv.RowSV) {
	out1 := make(sv.RowSV, columns)
	out2 := make(sv.RowSV, columns)
	for id := range out1 {
		out1[id], out2[id] = rndSameRawSV(columnLen)
	}
	return out1, out2
}

func rndSameRawSV(columnLen int) (sv.ColumnRaw, sv.ColumnRaw) {
	out1 := sv.ColumnRaw(utils.RandString(rnd, columnLen))
	out2 := out1
	return out1, out2
}

func rndSameRaw(columnLen int) (mv.ColumnRaw, mv.ColumnRaw) {
	out1 := mv.ColumnRaw(utils.RandString(rnd, columnLen))
	out2 := out1
	return out1, out2
}

func rndSameStrings(elems, columnLen int) ([]string, []string) {
	out1 := make([]string, elems)
	for idx := range out1 {
		out1[idx] = utils.RandString(rnd, columnLen)
	}
	out2 := make([]string, elems)
	copy(out2, out1)
	return out1, out2
}

func rndSameElems(elems, columnLen int) ([]mv.Elem, []mv.Elem) {
	out1 := make([]mv.Elem, elems)
	out2 := make([]mv.Elem, elems)
	for id := range out1 {
		tmp1, tmp2 := rndSameRaw(columnLen)
		out1[id], out2[id] = &tmp1, &tmp2
	}
	return out1, out2
}

func rndSameLists(elems, columnLen int) (mv.List, mv.List, gocql.TypeInfo, gocql.TypeInfo) {
	var out1, out2 mv.List
	outType := gocql.CollectionType{Elem: gocql.NewNativeType(4, gocql.TypeText, "")}
	out1, out2 = rndSameElems(elems, columnLen)
	return out1, out2, outType, outType
}

func rndSameMaps(elems, columnLen int) (mv.Map, mv.Map, gocql.TypeInfo, gocql.TypeInfo) {
	var out1, out2 mv.Map
	outType := gocql.CollectionType{Elem: gocql.NewNativeType(4, gocql.TypeText, ""), Key: gocql.NewNativeType(4, gocql.TypeText, "")}
	out1.Keys, out2.Keys = rndSameElems(elems, columnLen)
	out1.Values, out2.Values = rndSameElems(elems, columnLen)
	return out1, out2, outType, outType
}

func rndSameTuples(elems, columnLen int) (mv.Tuple, mv.Tuple, gocql.TypeInfo, gocql.TypeInfo) {
	var out1, out2 mv.Tuple
	outType := gocql.TupleTypeInfo{Elems: make([]gocql.TypeInfo, elems)}
	out1, out2 = rndSameElems(elems, columnLen)
	for idx := range out1 {
		outType.Elems[idx] = gocql.NewNativeType(4, gocql.TypeText, "")
	}
	return out1, out2, outType, outType
}

func rndSameUDTs(elems, columnLen int) (mv.UDT, mv.UDT, gocql.TypeInfo, gocql.TypeInfo) {
	var out1, out2 mv.UDT
	outType := gocql.UDTTypeInfo{Elements: make([]gocql.UDTField, elems)}
	outType.NativeType = gocql.NewNativeType(4, gocql.TypeUDT, "")
	out1.Names, out2.Names = rndSameStrings(elems, columnLen)
	out1.Values, out2.Values = rndSameElems(elems, columnLen)
	for idx := range out1.Names {
		outType.Elements[idx] = gocql.UDTField{
			Name: out1.Names[idx],
			Type: gocql.NewNativeType(4, gocql.TypeText, ""),
		}
	}
	return out1, out2, outType, outType
}
