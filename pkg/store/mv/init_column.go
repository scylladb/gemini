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

import "github.com/gocql/gocql"

// initColumn returns Column implementation for specified 'typeInfo'.
// CQL protocols <=2 and >2 have different len of the <elems count value> for collection types (List, Set and Map).
// The simplest type representation - ColumnRaw{}, but it can be used only for not collection types and for Tuple, UDT types without collection types.
// All collection types and Tuple, UDT types with collection types have own representations.
// So initColumn checks all these cases and returns right Column implementation.
func initColumn(colType gocql.TypeInfo) Column {
	if !haveCollection(colType) {
		return ColumnRaw("")
	}

	switch colType.Type() {
	case gocql.TypeList, gocql.TypeSet:
		listType := colType.(gocql.CollectionType)
		return List{
			initElem(listType.Elem),
		}
	case gocql.TypeMap:
		mapType := colType.(gocql.CollectionType)
		return Map{
			Keys:   []Elem{initElem(mapType.Key)},
			Values: []Elem{initElem(mapType.Elem)},
		}
	case gocql.TypeTuple:
		elems := colType.(gocql.TupleTypeInfo).Elems
		tuple := make(Tuple, len(elems))
		for i, elem := range elems {
			tuple[i] = initElem(elem)
		}
		return tuple
	case gocql.TypeUDT:
		elems := colType.(gocql.UDTTypeInfo).Elements
		udt := UDT{
			Names:  make([]string, len(elems)),
			Values: make([]Elem, len(elems)),
		}
		for i := range elems {
			udt.Names[i] = elems[i].Name
			udt.Values[i] = initElem(elems[i].Type)
		}
		return udt
	default:
		return ColumnRaw("")
	}
}

// haveCollection returns true if the collection type is present in the specified type.
func haveCollection(typeInfo gocql.TypeInfo) bool {
	switch typeInfo.Type() {
	case gocql.TypeList, gocql.TypeSet, gocql.TypeMap:
		return true
	case gocql.TypeUDT:
		udt := typeInfo.(gocql.UDTTypeInfo)
		for idx := range udt.Elements {
			if haveCollection(udt.Elements[idx].Type) {
				return true
			}
		}
	case gocql.TypeTuple:
		tuple := typeInfo.(gocql.TupleTypeInfo)
		for idx := range tuple.Elems {
			if haveCollection(tuple.Elems[idx]) {
				return true
			}
		}
	}
	return false
}

// initElem returns Elem implementation for the specified type.
func initElem(elemType gocql.TypeInfo) Elem {
	if !haveCollection(elemType) {
		tmp := ColumnRaw("")
		return &tmp
	}

	switch elemType.Type() {
	case gocql.TypeList, gocql.TypeSet:
		listType := elemType.(gocql.CollectionType)
		return &List{
			initElem(listType.Elem),
		}
	case gocql.TypeMap:
		mapType := elemType.(gocql.CollectionType)
		return &Map{
			Keys:   []Elem{initElem(mapType.Key)},
			Values: []Elem{initElem(mapType.Elem)},
		}
	case gocql.TypeTuple:
		elems := elemType.(gocql.TupleTypeInfo).Elems
		tuple := make(Tuple, len(elems))
		for i, elem := range elems {
			tuple[i] = initElem(elem)
		}
		return &tuple
	case gocql.TypeUDT:
		elems := elemType.(gocql.UDTTypeInfo).Elements
		udt := UDT{
			Names:  make([]string, len(elems)),
			Values: make([]Elem, len(elems)),
		}
		for i := range elems {
			udt.Names[i] = elems[i].Name
			udt.Values[i] = initElem(elems[i].Type)
		}
		return &udt
	default:
		tmp := ColumnRaw("")
		return &tmp
	}
}
