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

type RowMV []Column

func (row RowMV) Len() int {
	return len(row)
}

func (row RowMV) String(types []gocql.TypeInfo, names []string) string {
	out := ""
	if len(row) == 0 {
		return out
	}
	for idx := range row {
		out += fmt.Sprintf("%s:%+v", names[idx], row[idx].ToString(types[idx])) + columnsSeparator
	}
	return out[:len(out)-1]
}

func (row RowMV) Equal(row2 RowMV) bool {
	if len(row) != len(row2) {
		return false
	}
	if len(row) == 0 {
		return true
	}
	for idx := range row {
		if !row2[idx].EqualColumn(row[idx]) {
			return false
		}
	}
	return true
}

func (row RowMV) ToUnmarshal() []interface{} {
	out := make([]interface{}, len(row))
	for idx := range row {
		out[idx] = row[idx].ToUnmarshal()
	}
	return out
}

func (row RowMV) NewSameRow() RowMV {
	out := make(RowMV, len(row))
	for idx := range row {
		out[idx] = row[idx].NewSameColumn()
	}
	return out
}
