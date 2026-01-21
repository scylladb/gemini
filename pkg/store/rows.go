// Copyright 2025 ScyllaDB
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

package store

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
)

type (
	// Row represents a database row with column values stored as a slice
	// and a mapping from column names to indices for efficient access
	Row struct {
		columns map[string]int
		values  []any
	}

	Rows []Row
)

// NewRow creates a new Row with the given column names and values
func NewRow(columnNames []string, values []any) Row {
	columns := make(map[string]int, len(columnNames))
	for i, name := range columnNames {
		columns[name] = i
	}
	return Row{
		values:  values,
		columns: columns,
	}
}

// Get returns the value for the given column name
func (r Row) Get(columnName string) any {
	if idx, ok := r.columns[columnName]; ok {
		return r.values[idx]
	}
	return nil
}

// Set sets the value for the given column name
func (r Row) Set(columnName string, value any) {
	if idx, ok := r.columns[columnName]; ok {
		r.values[idx] = value
	}
}

func (r Rows) Len() int {
	return len(r)
}

func (r Rows) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r Rows) Less(i, j int) bool {
	return rowsCmp(r[i], r[j]) < 0
}

// MarshalJSON provides a readable JSON representation of a Row.
//
// The Row type stores values internally as a slice with a map of column
// names to indices. Since the internal fields are unexported, the default
// JSON encoder would serialize a Row as an empty object ({}).
//
// This custom marshaler converts the internal representation into a
// map[columnName]value so logs and errors include meaningful row content.
func (r Row) MarshalJSON() ([]byte, error) {
	// Build a temporary map to serialize column values by name.
	// Pre-size the map for efficiency.
	out := make(map[string]any, len(r.columns))
	for name, idx := range r.columns {
		// Guard against out-of-range indexes in case of malformed rows
		if idx >= 0 && idx < len(r.values) {
			out[name] = r.values[idx]
		} else {
			out[name] = nil
		}
	}
	return json.Marshal(out)
}

// rowsCmp compares two rows column-by-column using their value order.
// It supports a wide range of scalar types and falls back to string
// comparison for differing concrete types.
func rowsCmp(i, j Row) int {
	maximum := len(i.values)
	if len(j.values) < maximum {
		maximum = len(j.values)
	}
	for idx := 0; idx < maximum; idx++ {
		if c := compareValues(i.values[idx], j.values[idx]); c != 0 {
			return c
		}
	}
	// All common columns equal; shorter row sorts first
	switch {
	case len(i.values) < len(j.values):
		return -1
	case len(i.values) > len(j.values):
		return 1
	default:
		return 0
	}
}

func comparePtr[T cmp.Ordered](a, b *T) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	return cmp.Compare(*a, *b)
}

func compareBool(a, b bool) int {
	if a == b {
		return 0
	}
	if !a {
		return -1
	}
	return 1
}

func compareBoolPtr(a, b *bool) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	return compareBool(*a, *b)
}

//nolint:gocyclo
func compareValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	switch av := a.(type) {
	case []byte:
		if bv, ok := b.([]byte); ok {
			return bytes.Compare(av, bv)
		}
	case *[]byte:
		if bv, ok := b.(*[]byte); ok {
			if av == nil && bv == nil {
				return 0
			}
			if av == nil {
				return -1
			}
			if bv == nil {
				return 1
			}
			return bytes.Compare(*av, *bv)
		}
	case string:
		if bv, ok := b.(string); ok {
			return strings.Compare(av, bv)
		}
	case *string:
		if bv, ok := b.(*string); ok {
			return comparePtr(av, bv)
		}
	case bool:
		if bv, ok := b.(bool); ok {
			return compareBool(av, bv)
		}
	case *bool:
		if bv, ok := b.(*bool); ok {
			return compareBoolPtr(av, bv)
		}
	case float32:
		if bv, ok := b.(float32); ok {
			return cmp.Compare(av, bv)
		}
	case *float32:
		if bv, ok := b.(*float32); ok {
			return comparePtr(av, bv)
		}
	case float64:
		if bv, ok := b.(float64); ok {
			return cmp.Compare(av, bv)
		}
	case *float64:
		if bv, ok := b.(*float64); ok {
			return comparePtr(av, bv)
		}
	case int:
		if bv, ok := b.(int); ok {
			return cmp.Compare(av, bv)
		}
	case *int:
		if bv, ok := b.(*int); ok {
			return comparePtr(av, bv)
		}
	case int8:
		if bv, ok := b.(int8); ok {
			return cmp.Compare(av, bv)
		}
	case *int8:
		if bv, ok := b.(*int8); ok {
			return comparePtr(av, bv)
		}
	case int16:
		if bv, ok := b.(int16); ok {
			return cmp.Compare(av, bv)
		}
	case *int16:
		if bv, ok := b.(*int16); ok {
			return comparePtr(av, bv)
		}
	case int32:
		if bv, ok := b.(int32); ok {
			return cmp.Compare(av, bv)
		}
	case *int32:
		if bv, ok := b.(*int32); ok {
			return comparePtr(av, bv)
		}
	case int64:
		if bv, ok := b.(int64); ok {
			return cmp.Compare(av, bv)
		}
	case *int64:
		if bv, ok := b.(*int64); ok {
			return comparePtr(av, bv)
		}
	case uint:
		if bv, ok := b.(uint); ok {
			return cmp.Compare(av, bv)
		}
	case *uint:
		if bv, ok := b.(*uint); ok {
			return comparePtr(av, bv)
		}
	case uint8:
		if bv, ok := b.(uint8); ok {
			return cmp.Compare(av, bv)
		}
	case *uint8:
		if bv, ok := b.(*uint8); ok {
			return comparePtr(av, bv)
		}
	case uint16:
		if bv, ok := b.(uint16); ok {
			return cmp.Compare(av, bv)
		}
	case *uint16:
		if bv, ok := b.(*uint16); ok {
			return comparePtr(av, bv)
		}
	case uint32:
		if bv, ok := b.(uint32); ok {
			return cmp.Compare(av, bv)
		}
	case *uint32:
		if bv, ok := b.(*uint32); ok {
			return comparePtr(av, bv)
		}
	case uint64:
		if bv, ok := b.(uint64); ok {
			return cmp.Compare(av, bv)
		}
	case *uint64:
		if bv, ok := b.(*uint64); ok {
			return comparePtr(av, bv)
		}
	case gocql.UUID:
		if bv, ok := b.(gocql.UUID); ok {
			return bytes.Compare(av[:], bv[:])
		}
	case *gocql.UUID:
		if bv, ok := b.(*gocql.UUID); ok {
			if av == nil && bv == nil {
				return 0
			}
			if av == nil {
				return -1
			}
			if bv == nil {
				return 1
			}
			return bytes.Compare(av[:], bv[:])
		}
	case time.Time:
		if bv, ok := b.(time.Time); ok {
			return av.Compare(bv)
		}
	case *time.Time:
		if bv, ok := b.(*time.Time); ok {
			if av == nil && bv == nil {
				return 0
			}
			if av == nil {
				return -1
			}
			if bv == nil {
				return 1
			}
			return av.Compare(*bv)
		}
	case time.Duration:
		if bv, ok := b.(time.Duration); ok {
			return cmp.Compare(av, bv)
		}
	case *time.Duration:
		if bv, ok := b.(*time.Duration); ok {
			return comparePtr(av, bv)
		}
	case *big.Int:
		if bv, ok := b.(*big.Int); ok {
			if av == nil && bv == nil {
				return 0
			}
			if av == nil {
				return -1
			}
			if bv == nil {
				return 1
			}
			return av.Cmp(bv)
		}
	case *inf.Dec:
		if bv, ok := b.(*inf.Dec); ok {
			if av == nil && bv == nil {
				return 0
			}
			if av == nil {
				return -1
			}
			if bv == nil {
				return 1
			}
			return av.Cmp(bv)
		}
	}
	// Fallback to string representation when types differ or are not handled
	return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
}
