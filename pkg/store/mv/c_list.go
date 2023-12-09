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

type List []Elem

func (l List) ToString(colInfo gocql.TypeInfo) string {
	out := "set<"
	if colInfo.Type() == gocql.TypeList {
		out = "list<"
	}
	if len(l) == 0 {
		return out + ">"
	}
	listInfo := colInfo.(gocql.CollectionType)
	for idx := range l {
		out += fmt.Sprintf("%d:%s;", idx, l[idx].ToString(listInfo.Elem))
	}
	out = out[:len(out)-1]
	return out + ">"
}

func (l List) ToStringRaw() string {
	out := "set||list<"
	if len(l) == 0 {
		return out + ">"
	}
	for idx := range l {
		out += fmt.Sprintf("%d:%s;", idx, l[idx].ToStringRaw())
	}
	out = out[:len(out)-1]
	return out + ">"
}

func (l List) EqualColumn(colT interface{}) bool {
	l2, ok := colT.(List)
	if len(l) != len(l2) || !ok {
		return false
	}
	if len(l) == 0 {
		return true
	}
	for idx := range l {
		if !l[idx].EqualElem(l2[idx]) {
			return false
		}
	}
	return true
}

func (l List) EqualElem(colT interface{}) bool {
	l2, ok := colT.(*List)
	if len(l) != len(*l2) || !ok {
		return false
	}
	if len(l) == 0 {
		return true
	}
	for idx := range l {
		if !l[idx].EqualElem((*l2)[idx]) {
			return false
		}
	}
	return true
}

func (l *List) UnmarshalCQL(colInfo gocql.TypeInfo, data []byte) error {
	if len(data) < 1 {
		return nil
	}
	col, ok := colInfo.(gocql.CollectionType)
	if !ok {
		return fmt.Errorf("%+v is unsupported type to unmarshal in list/set", reflect.TypeOf(colInfo))
	}
	if colInfo.Version() <= protoVersion2 {
		if len(data) < 2 {
			return ErrorUnmarshalEOF
		}
		return l.unmarshalOld(col, data)
	}
	if len(data) < 4 {
		return ErrorUnmarshalEOF
	}
	return l.unmarshalNew(col, data)
}

func (l *List) unmarshalNew(colInfo gocql.CollectionType, data []byte) error {
	var err error
	listLen := readLen(data)
	data = data[4:]
	switch listLen {
	case 0:
	case 1:
		var elemLen int32
		if len(data) < 4 {
			return ErrorUnmarshalEOF
		}
		elemLen = readLen(data) + 4
		if len(data) < int(elemLen) {
			return ErrorUnmarshalEOF
		}
		if err = (*l)[0].UnmarshalCQL(colInfo.Elem, data[4:elemLen]); err != nil {
			return err
		}
	default:
		var elemLen int32
		*l = append(*l, make(List, listLen-1)...)
		for idx := range *l {
			if idx > 0 {
				(*l)[idx] = (*l)[0].NewSameElem()
			}
			if len(data) < 4 {
				return ErrorUnmarshalEOF
			}
			elemLen = readLen(data) + 4
			if len(data) < int(elemLen) {
				return ErrorUnmarshalEOF
			}
			if err = (*l)[idx].UnmarshalCQL(colInfo.Elem, data[4:elemLen]); err != nil {
				return err
			}
			data = data[elemLen:]
		}
	}
	return nil
}

func (l *List) unmarshalOld(colInfo gocql.CollectionType, data []byte) error {
	var err error
	listLen := readLenOld(data)
	data = data[2:]
	switch listLen {
	case 0:
	case 1:
		var elemLen int16
		if len(data) < 2 {
			return ErrorUnmarshalEOF
		}
		elemLen = readLenOld(data) + 2
		if len(data) < int(elemLen) {
			return ErrorUnmarshalEOF
		}
		if err = (*l)[0].UnmarshalCQL(colInfo.Elem, data[2:elemLen]); err != nil {
			return err
		}
	default:
		var elemLen int16
		*l = append(*l, make(List, listLen-1)...)
		for idx := range *l {
			if idx > 0 {
				(*l)[idx] = (*l)[0].NewSameElem()
			}
			if len(data) < 2 {
				return ErrorUnmarshalEOF
			}
			elemLen = readLenOld(data) + 2
			if len(data) < int(elemLen) {
				return ErrorUnmarshalEOF
			}
			if err = (*l)[idx].UnmarshalCQL(colInfo.Elem, data[2:elemLen]); err != nil {
				return err
			}
			data = data[elemLen:]
		}
	}
	return nil
}

func (l List) NewSameColumn() Column {
	return List{l[0].NewSameElem()}
}

func (l List) ToUnmarshal() interface{} {
	return &l
}

func (l List) NewSameElem() Elem {
	return &List{l[0].NewSameElem()}
}
