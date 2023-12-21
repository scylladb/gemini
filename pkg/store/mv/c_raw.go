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

package mv

import (
	"fmt"
	"unsafe"

	"github.com/gocql/gocql"
)

// ColumnRaw for most cases.
type ColumnRaw string

func (col ColumnRaw) ToString(colInfo gocql.TypeInfo) string {
	if len(col) == 0 {
		return ""
	}
	tmpVal := colInfo.New()
	if err := gocql.Unmarshal(colInfo, unsafe.Slice(unsafe.StringData((string)(col)), len(col)), tmpVal); err != nil {
		panic(err)
	}
	out := fmt.Sprintf("%v", dereference(tmpVal))
	// Add name of the type for complex and collections types
	switch colInfo.Type() {
	case gocql.TypeList:
		out = fmt.Sprintf("list<%s>", out)
	case gocql.TypeSet:
		out = fmt.Sprintf("set<%s>", out)
	case gocql.TypeMap:
		out = fmt.Sprintf("map<%s>", out)
	case gocql.TypeTuple:
		out = fmt.Sprintf("tuple<%s>", out)
	case gocql.TypeUDT:
		out = fmt.Sprintf("udt<%s>", out)
	}
	return out
}

func (col ColumnRaw) ToStringRaw() string {
	return fmt.Sprint(unsafe.Slice(unsafe.StringData((string)(col)), len(col)))
}

func (col ColumnRaw) EqualColumn(colT interface{}) bool {
	col2, ok := colT.(ColumnRaw)
	if !ok {
		return false
	}
	return col == col2
}

func (col ColumnRaw) EqualElem(colT interface{}) bool {
	col2, ok := colT.(*ColumnRaw)
	if !ok {
		// Columns len are different - means columns are unequal
		return false
	}
	return col == *col2
}

func (col ColumnRaw) NewSameColumn() Column {
	return ColumnRaw("")
}

func (col ColumnRaw) ToUnmarshal() interface{} {
	return &col
}

func (col ColumnRaw) NewSameElem() Elem {
	tmp := ColumnRaw("")
	return &tmp
}

func (col *ColumnRaw) UnmarshalCQL(_ gocql.TypeInfo, data []byte) error {
	if len(data) > 0 {
		// Puts data without copying
		*col = (ColumnRaw)(unsafe.String(&data[0], len(data)))
	}
	return nil
}
