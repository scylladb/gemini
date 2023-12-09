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

type Map struct {
	Keys   []Elem
	Values []Elem
}

func (m Map) ToString(colInfo gocql.TypeInfo) string {
	out := "map("
	if len(m.Keys) == 0 {
		return out + ">"
	}
	mapInfo := colInfo.(gocql.CollectionType)
	for idx := range m.Keys {
		out += fmt.Sprintf("%s:%s;", m.Keys[idx].ToString(mapInfo.Key), m.Values[idx].ToString(mapInfo.Elem))
	}
	out = out[:len(out)-1]
	return out + ")"
}

func (m Map) ToStringRaw() string {
	out := "map<"
	if len(m.Keys) == 0 {
		return out + ">"
	}
	for idx := range m.Keys {
		out += fmt.Sprintf("key%d%s:value%s;", idx, m.Keys[idx].ToStringRaw(), m.Values[idx].ToStringRaw())
	}
	out = out[:len(out)-1]
	return out + ")"
}

func (m Map) EqualColumn(colT interface{}) bool {
	m2, ok := colT.(Map)
	if len(m.Keys) != len(m2.Keys) || len(m.Values) != len(m2.Values) || !ok {
		return false
	}
	if len(m.Keys) == 0 {
		return true
	}
	for idx := range m.Keys {
		if !m.Keys[idx].EqualElem(m2.Keys[idx]) {
			return false
		}
		if !m.Values[idx].EqualElem(m2.Values[idx]) {
			return false
		}
	}
	return true
}

func (m Map) EqualElem(colT interface{}) bool {
	m2, ok := colT.(*Map)
	if len(m.Keys) != len(m2.Keys) || len(m.Values) != len(m2.Values) || !ok {
		return false
	}
	if len(m.Keys) == 0 {
		return true
	}
	for idx := range m.Keys {
		if !m.Keys[idx].EqualElem(m2.Keys[idx]) {
			return false
		}
		if !m.Values[idx].EqualElem(m2.Values[idx]) {
			return false
		}
	}
	return true
}

func (m *Map) UnmarshalCQL(colInfo gocql.TypeInfo, data []byte) error {
	col, ok := colInfo.(gocql.CollectionType)
	if !ok {
		return fmt.Errorf("%+v is unsupported type to unmarshal in map", reflect.TypeOf(colInfo))
	}
	if colInfo.Version() <= protoVersion2 {
		if len(data) < 2 {
			return ErrorUnmarshalEOF
		}
		return m.oldUnmarshalCQL(col, data)
	}
	if len(data) < 4 {
		return ErrorUnmarshalEOF
	}
	return m.newUnmarshalCQL(col, data)
}

func (m *Map) oldUnmarshalCQL(mapInfo gocql.CollectionType, data []byte) error {
	var err error
	mapLen := readLenOld(data)
	data = data[2:]
	switch mapLen {
	case 0:
	case 1:
		var keyLen, valLen int16
		if len(data) < 4 {
			return ErrorUnmarshalEOF
		}
		keyLen = readLenOld(data) + 2
		if len(data) < int(keyLen) {
			return ErrorUnmarshalEOF
		}
		if err = m.Keys[0].UnmarshalCQL(mapInfo.Key, data[2:keyLen]); err != nil {
			return err
		}
		data = data[keyLen:]

		valLen = readLenOld(data) + 2
		if len(data) < int(valLen) {
			return ErrorUnmarshalEOF
		}
		if err = m.Values[0].UnmarshalCQL(mapInfo.Elem, data[2:valLen]); err != nil {
			return err
		}
	default:
		var keyLen, valLen int16
		m.Keys = append(m.Keys, make([]Elem, mapLen-1)...)
		m.Values = append(m.Values, make([]Elem, mapLen-1)...)
		for idx := range m.Keys {
			if idx > 0 {
				m.Keys[idx] = m.Keys[0].NewSameElem()
				m.Values[idx] = m.Values[0].NewSameElem()
			}
			if len(data) < 4 {
				return ErrorUnmarshalEOF
			}
			keyLen = readLenOld(data) + 2
			if len(data) < int(keyLen) {
				return ErrorUnmarshalEOF
			}
			if err = m.Keys[idx].UnmarshalCQL(mapInfo.Key, data[2:keyLen]); err != nil {
				return err
			}
			data = data[keyLen:]

			valLen = readLenOld(data) + 2
			if len(data) < int(valLen) {
				return ErrorUnmarshalEOF
			}
			if err = m.Values[idx].UnmarshalCQL(mapInfo.Elem, data[2:valLen]); err != nil {
				return err
			}
			data = data[valLen:]
		}
	}
	return nil
}

func (m *Map) newUnmarshalCQL(mapInfo gocql.CollectionType, data []byte) error {
	var err error
	mapLen := readLen(data)
	data = data[4:]
	switch mapLen {
	case 0:
	case 1:
		var keyLen, valLen int32
		if len(data) < 8 {
			return ErrorUnmarshalEOF
		}
		keyLen = readLen(data) + 4
		if len(data) < int(keyLen) {
			return ErrorUnmarshalEOF
		}
		if err = m.Keys[0].UnmarshalCQL(mapInfo.Key, data[4:keyLen]); err != nil {
			return err
		}
		data = data[keyLen:]

		valLen = readLen(data) + 4
		if len(data) < int(valLen) {
			return ErrorUnmarshalEOF
		}
		if err = m.Values[0].UnmarshalCQL(mapInfo.Elem, data[4:valLen]); err != nil {
			return err
		}
	default:
		var keyLen, valLen int32
		m.Keys = append(m.Keys, make([]Elem, mapLen-1)...)
		m.Values = append(m.Values, make([]Elem, mapLen-1)...)
		for idx := range m.Keys {
			if idx > 0 {
				m.Keys[idx] = m.Keys[0].NewSameElem()
				m.Values[idx] = m.Values[0].NewSameElem()
			}
			if len(data) < 8 {
				return ErrorUnmarshalEOF
			}
			keyLen = readLen(data) + 4
			if len(data) < int(keyLen) {
				return ErrorUnmarshalEOF
			}
			if err = m.Keys[idx].UnmarshalCQL(mapInfo.Key, data[4:keyLen]); err != nil {
				return err
			}
			data = data[keyLen:]

			valLen = readLen(data) + 4
			if len(data) < int(valLen) {
				return ErrorUnmarshalEOF
			}
			if err = m.Values[idx].UnmarshalCQL(mapInfo.Elem, data[4:valLen]); err != nil {
				return err
			}
			data = data[valLen:]
		}
	}
	return nil
}

func (m Map) NewSameColumn() Column {
	return Map{
		Keys:   []Elem{m.Keys[0].NewSameElem()},
		Values: []Elem{m.Values[0].NewSameElem()},
	}
}

func (m Map) ToUnmarshal() interface{} {
	return &m
}

func (m *Map) NewSameElem() Elem {
	return &Map{
		Keys:   []Elem{m.Keys[0].NewSameElem()},
		Values: []Elem{m.Values[0].NewSameElem()},
	}
}
