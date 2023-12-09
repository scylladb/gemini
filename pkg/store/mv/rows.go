// Copyright 2019 ScyllaDB
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

package mv

import (
	"fmt"

	"github.com/gocql/gocql"
)

const columnsSeparator = ";"

func initRows(types []gocql.TypeInfo, rows int) RowsMV {
	out := make(RowsMV, rows)
	baseRow := make(RowMV, len(types))
	for idx := range types {
		baseRow[idx] = initColumn(types[idx])
	}
	out[0] = baseRow
	return out
}

type RowsMV []RowMV

func (l RowsMV) LenRows() int {
	return len(l)
}

func (l RowsMV) StringsRows(types []gocql.TypeInfo, names []string) []string {
	out := make([]string, len(l))
	for idx := range l {
		out[idx] = fmt.Sprintf("row%d:%s", idx, l[idx].String(types, names))
	}
	return out
}

func (l RowsMV) FindEqualRow(row RowMV) int {
	for idx := range l {
		if row.Equal(l[idx]) {
			return idx
		}
	}
	return -1
}
