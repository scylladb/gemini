// Copyright 2023 ScyllaDB
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

package sv

import (
	"fmt"

	"github.com/gocql/gocql"
)

// GetResult scan gocql.Iter and returns Result.
func GetResult(iter *gocql.Iter) Result {
	switch iter.NumRows() {
	case 0:
		return Result{}
	case 1:
		out := initResult(iter)
		out.Rows = initRows(len(out.Names), iter.NumRows())
		iter.Scan(out.Rows[0].ToInterfaces()...)
		return out
	default:
		out := initResult(iter)
		out.Rows = initRows(len(out.Names), iter.NumRows())
		count := 0
		for iter.Scan(out.Rows[count].ToInterfaces()...) {
			count++
			if count >= len(out.Rows) {
				out.Rows = append(out.Rows, initRows(len(out.Names), out.LenRows())...)
			}
		}
		out.Rows = out.Rows[:count]
		return out
	}
}

type Result struct {
	Types []gocql.TypeInfo
	Names []string
	Rows  RowsSV
}

func (d Result) LenColumns() int {
	return len(d.Names)
}

func (d Result) LenRows() int {
	return len(d.Rows)
}

func (d Result) StringAllRows() []string {
	return d.Rows.StringsRows(d.Types, d.Names)
}

// initResult returns Result with filled Types and Names and initiated Rows.
// Only Rows[0] have proper all column's initiation.
func initResult(iter *gocql.Iter) Result {
	out := Result{}
	out.Types = make([]gocql.TypeInfo, len(iter.Columns()))
	out.Names = make([]string, len(iter.Columns()))
	idx := 0
	for _, column := range iter.Columns() {
		if col, ok := column.TypeInfo.(gocql.TupleTypeInfo); ok {
			tmpTypes := make([]gocql.TypeInfo, len(col.Elems)-1)
			tmpNames := make([]string, len(col.Elems)-1)
			out.Types = append(out.Types, tmpTypes...)
			out.Names = append(out.Names, tmpNames...)
			for i := range col.Elems {
				out.Types[idx] = col.Elems[i]
				out.Names[idx] = fmt.Sprintf("%s.t[%d]", column.Name, i)
				idx++
			}
		} else {
			out.Types[idx] = column.TypeInfo
			out.Names[idx] = column.Name
			idx++
		}
	}
	return out
}
