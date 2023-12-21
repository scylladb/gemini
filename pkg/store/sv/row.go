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

type RowSV []ColumnRaw

const columnsSeparator = ";"

func (row RowSV) Len() int {
	return len(row)
}

func (row RowSV) String(types []gocql.TypeInfo, names []string) string {
	out := ""
	if len(row) == 0 {
		return out
	}
	for idx := range row {
		out += fmt.Sprintf("%s:%+v", names[idx], row[idx].ToString(types[idx])) + columnsSeparator
	}
	return out[:len(out)-1]
}

func (row RowSV) Equal(row2 RowSV) bool {
	if len(row) != len(row2) {
		// If rows len are different - means rows are unequal
		return false
	}
	if len(row) < 1 {
		// If rows len are same and len==0 - means rows are equal
		// Conditions "<" or ">" works faster that "=="
		return true
	}
	for idx := range row {
		if row[idx] != row2[idx] {
			return false
		}
	}
	return true
}

func (row RowSV) ToInterfaces() []interface{} {
	out := make([]interface{}, len(row))
	for idx := range row {
		out[idx] = row[idx].ToInterface()
	}
	return out
}
