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
	"reflect"

	"github.com/gocql/gocql"
)

type Tuple []Elem

func (t Tuple) ToString(colInfo gocql.TypeInfo) string {
	out := "tuple<"
	if len(t) == 0 {
		return out + ">"
	}
	tuple := colInfo.(gocql.TupleTypeInfo)
	for i, elem := range tuple.Elems {
		out += fmt.Sprintf("%d:%s;", i, t[i].ToString(elem))
	}
	return out[:len(out)-1] + ">"
}

func (t Tuple) ToStringRaw() string {
	out := "tuple<"
	if len(t) == 0 {
		return out + ">"
	}
	for i := range t {
		out += fmt.Sprintf("%d:%s;", i, t[i].ToStringRaw())
	}
	return out[:len(out)-1] + ">"
}

func (t Tuple) EqualColumn(colT interface{}) bool {
	t2, ok := colT.(Tuple)
	if len(t) != len(t2) || !ok {
		return false
	}
	if len(t) == 0 {
		return true
	}
	for idx := range t {
		if !t[idx].EqualElem(t2[idx]) {
			return false
		}
	}
	return true
}

func (t Tuple) EqualElem(colT interface{}) bool {
	t2, ok := colT.(*Tuple)
	if len(t) != len(*t2) || !ok {
		return false
	}
	if len(t) == 0 {
		return true
	}
	for idx := range t {
		if !t[idx].EqualElem((*t2)[idx]) {
			return false
		}
	}
	return true
}

func (t *Tuple) UnmarshalCQL(colInfo gocql.TypeInfo, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	tuple, ok := colInfo.(gocql.TupleTypeInfo)
	if !ok {
		return fmt.Errorf("%+v is unsupported type to unmarshal in tuple", reflect.TypeOf(colInfo))
	}
	for idx := range *t {
		if len(data) < 4 {
			return ErrorUnmarshalEOF
		}
		elemLen := readLen(data) + 4
		if int32(len(data)) < elemLen {
			return ErrorUnmarshalEOF
		}
		err := (*t)[idx].UnmarshalCQL(tuple.Elems[idx], data[4:elemLen])
		if err != nil {
			return err
		}
		data = data[elemLen:]
	}
	return nil
}

func (t Tuple) NewSameColumn() Column {
	out := make(Tuple, len(t))
	for idx := range t {
		out[idx] = t[idx].NewSameElem()
	}
	return out
}

func (t Tuple) NewSameElem() Elem {
	out := make(Tuple, len(t))
	for idx := range t {
		out[idx] = t[idx].NewSameElem()
	}
	return &out
}

func (t Tuple) ToUnmarshal() interface{} {
	return &t
}
