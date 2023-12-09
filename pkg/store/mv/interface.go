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
	"github.com/gocql/gocql"
)

// Column is an interface that describes the necessary methods for the processing with columns.
// To unmarshall by the iter.Scan function need to put references of Column's implementations in interface{}.
// So we can put Column's implementations (not references) in the RowMV ([]Column) and send Column's references to the iter.Scan function by interface{}.
type Column interface {
	// NewSameColumn creates the same but empty column.
	NewSameColumn() Column
	EqualColumn(interface{}) bool
	// ToString makes string from Column's with interpretations into GO types.
	ToString(gocql.TypeInfo) string
	// ToStringRaw makes string from Column's without interpretations into GO types.
	ToStringRaw() string
	// ToUnmarshal makes references Column's implementations in the interface{}. Necessary to unmarshall by the iter.Scan function.
	ToUnmarshal() interface{}
}

// Elem is an interface that describes the necessary methods for the processing with elements of the collection, Tuple, UDT types.
type Elem interface {
	Column
	NewSameElem() Elem
	EqualElem(interface{}) bool
	UnmarshalCQL(colInfo gocql.TypeInfo, data []byte) error
}
