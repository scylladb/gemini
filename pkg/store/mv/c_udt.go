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
	"reflect"

	"github.com/gocql/gocql"
)

type UDT struct {
	Names  []string
	Values []Elem
}

func (u UDT) ToString(colInfo gocql.TypeInfo) string {
	out := "udt<"
	if len(u.Names) == 0 {
		return out + ">"
	}
	udt := colInfo.(gocql.UDTTypeInfo)

	for idx := range u.Values {
		out += u.pairToString(idx, udt.Elements[idx].Type)
	}
	out = out[:len(out)-1]
	return out + ">"
}

func (u UDT) pairToString(idx int, colType gocql.TypeInfo) string {
	return fmt.Sprintf("%s:%s;", u.Names[idx], u.Values[idx].ToString(colType))
}

func (u UDT) ToStringRaw() string {
	out := "udt<"
	if len(u.Names) == 0 {
		return out + ">"
	}
	for idx := range u.Values {
		out += u.pairToStringRaw(idx)
	}
	out = out[:len(out)-1]
	return out + ">"
}

func (u UDT) pairToStringRaw(idx int) string {
	return fmt.Sprintf("%s:%s;", u.Names[idx], u.Values[idx].ToStringRaw())
}

func (u UDT) EqualColumn(colT interface{}) bool {
	u2, ok := colT.(UDT)
	if len(u.Values) != len(u2.Values) || len(u.Names) != len(u2.Names) || !ok {
		return false
	}
	if len(u.Values) == 0 && len(u.Names) == 0 {
		return true
	}
	for idx := range u.Values {
		if !u.Values[idx].EqualElem(u2.Values[idx]) {
			return false
		}
	}
	for idx := range u.Names {
		if u.Names[idx] != u2.Names[idx] {
			return false
		}
	}
	return true
}

func (u UDT) EqualElem(colT interface{}) bool {
	u2, ok := colT.(*UDT)
	if len(u.Values) != len(u2.Values) || len(u.Names) != len(u2.Names) || !ok {
		return false
	}
	if len(u.Values) == 0 && len(u.Names) == 0 {
		return true
	}
	for idx := range u.Values {
		if !u.Values[idx].EqualElem(u2.Values[idx]) {
			return false
		}
	}
	for idx := range u.Names {
		if u.Names[idx] != u2.Names[idx] {
			return false
		}
	}
	return true
}

func (u *UDT) UnmarshalCQL(colInfo gocql.TypeInfo, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	udt, ok := colInfo.(gocql.UDTTypeInfo)
	if !ok {
		return fmt.Errorf("%+v is unsupported type to unmarshal in udt", reflect.TypeOf(colInfo))
	}
	for idx := range u.Values {
		switch len(data) {
		case 0:
			return nil
		case 1, 2, 3:
			return ErrorUnmarshalEOF
		default:
			elemLen := readLen(data) + 4
			if int32(len(data)) < elemLen {
				return ErrorUnmarshalEOF
			}
			err := u.Values[idx].UnmarshalCQL(udt.Elements[idx].Type, data[4:elemLen])
			if err != nil {
				return err
			}
			data = data[elemLen:]
		}
	}
	return nil
}

func (u UDT) NewSameColumn() Column {
	out := UDT{Names: u.Names, Values: make([]Elem, len(u.Values))}
	for idx := range u.Values {
		out.Values[idx] = u.Values[idx].NewSameElem()
	}
	return out
}

func (u UDT) NewSameElem() Elem {
	out := UDT{Names: u.Names, Values: make([]Elem, len(u.Values))}
	for idx := range u.Values {
		out.Values[idx] = u.Values[idx].NewSameElem()
	}
	return &out
}

func (u UDT) ToUnmarshal() interface{} {
	return &u
}
