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

//nolint:gocyclo
func rowsCmp(i, j Row) int {
	switch mis := i.Get("pk0").(type) {
	case []byte:
		mjs, _ := j.Get("pk0").([]byte)
		return bytes.Compare(mis, mjs)
	case *[]byte:
		mjs, _ := j.Get("pk0").(*[]byte)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		return bytes.Compare(*mis, *mjs)
	case string:
		mjs, _ := j.Get("pk0").(string)
		return strings.Compare(mis, mjs)
	case *string:
		mjs, _ := j.Get("pk0").(*string)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		return strings.Compare(*mis, *mjs)
	case bool:
		mjs, _ := j.Get("pk0").(bool)
		if mis == mjs {
			return 0
		}
		if !mis && mjs {
			return -1
		}
		return 1
	case *bool:
		mjs, _ := j.Get("pk0").(*bool)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		if *mis == *mjs {
			return 0
		}
		if !*mis && *mjs {
			return -1
		}
		return 1
	case float32:
		mjs, _ := j.Get("pk0").(float32)
		return cmp.Compare(mis, mjs)
	case *float32:
		mjs, _ := j.Get("pk0").(*float32)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		return cmp.Compare(*mis, *mjs)
	case float64:
		mjs, _ := j.Get("pk0").(float64)
		return cmp.Compare(mis, mjs)
	case *float64:
		mjs, _ := j.Get("pk0").(*float64)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		return cmp.Compare(*mis, *mjs)
	case int:
		mjs, _ := j.Get("pk0").(int)
		return cmp.Compare(mis, mjs)
	case *int:
		mjs, _ := j.Get("pk0").(*int)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		return cmp.Compare(*mis, *mjs)
	case int8:
		mjs, _ := j.Get("pk0").(int8)
		return cmp.Compare(mis, mjs)
	case *int8:
		mjs, _ := j.Get("pk0").(*int8)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		return cmp.Compare(*mis, *mjs)
	case int16:
		mjs, _ := j.Get("pk0").(int16)
		return cmp.Compare(mis, mjs)
	case *int16:
		mjs, _ := j.Get("pk0").(*int16)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		return cmp.Compare(*mis, *mjs)
	case int32:
		mjs, _ := j.Get("pk0").(int32)
		return cmp.Compare(mis, mjs)
	case *int32:
		mjs, _ := j.Get("pk0").(*int32)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		return cmp.Compare(*mis, *mjs)
	case int64:
		mjs, _ := j.Get("pk0").(int64)
		return cmp.Compare(mis, mjs)
	case *int64:
		mjs, _ := j.Get("pk0").(*int64)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		return cmp.Compare(*mis, *mjs)
	case uint:
		mjs, _ := j.Get("pk0").(uint)
		return cmp.Compare(mis, mjs)
	case *uint:
		mjs, _ := j.Get("pk0").(*uint)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		return cmp.Compare(*mis, *mjs)
	case uint8:
		mjs, _ := j.Get("pk0").(uint8)
		return cmp.Compare(mis, mjs)
	case *uint8:
		mjs, _ := j.Get("pk0").(*uint8)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		return cmp.Compare(*mis, *mjs)
	case uint16:
		mjs, _ := j.Get("pk0").(uint16)
		return cmp.Compare(mis, mjs)
	case *uint16:
		mjs, _ := j.Get("pk0").(*uint16)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		return cmp.Compare(*mis, *mjs)
	case uint32:
		mjs, _ := j.Get("pk0").(uint32)
		return cmp.Compare(mis, mjs)
	case *uint32:
		mjs, _ := j.Get("pk0").(*uint32)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		return cmp.Compare(*mis, *mjs)
	case uint64:
		mjs, _ := j.Get("pk0").(uint64)
		return cmp.Compare(mis, mjs)
	case *uint64:
		mjs, _ := j.Get("pk0").(*uint64)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		return cmp.Compare(*mis, *mjs)
	case gocql.UUID:
		mjs, _ := j.Get("pk0").(gocql.UUID)
		return bytes.Compare(mis[:], mjs[:])
	case *gocql.UUID:
		mjs, _ := j.Get("pk0").(*gocql.UUID)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		return bytes.Compare(mis[:], mjs[:])
	case time.Time:
		mjs, _ := j.Get("pk0").(time.Time)
		return mis.Compare(mjs)
	case *time.Time:
		mjs, _ := j.Get("pk0").(*time.Time)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		return mis.Compare(*mjs)
	case time.Duration:
		mjs, _ := j.Get("pk0").(time.Duration)
		return cmp.Compare(mis, mjs)
	case *time.Duration:
		mjs, _ := j.Get("pk0").(*time.Duration)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}
		return cmp.Compare(*mis, *mjs)
	case *big.Int:
		mjs, _ := j.Get("pk0").(*big.Int)
		return mis.Cmp(mjs)
	case **big.Int:
		mjs, _ := j.Get("pk0").(**big.Int)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}

		return (**mis).Cmp(*mjs)
	case *inf.Dec:
		mjs, _ := j.Get("pk0").(*inf.Dec)
		return mis.Cmp(mjs)
	case **inf.Dec:
		mjs, _ := j.Get("pk0").(**inf.Dec)
		if mis == nil && mjs == nil {
			return 0
		}
		if mis == nil {
			return -1
		}
		if mjs == nil {
			return 1
		}

		return (*mis).Cmp(*mjs)
	case nil:
		return 0
	default:
		panic(fmt.Sprintf("unhandled type [%T][%v]\n", mis, mis))
	}
}
