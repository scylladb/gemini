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

package store

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/go-set/strset"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/typedef"
)

//nolint:gocyclo
func formatRows(sb *strings.Builder, key string, value any) string {
	sb.WriteString(key)
	sb.WriteByte('=')

	// Handle nil values
	if value == nil {
		sb.WriteString("null")
		return sb.String()
	}

	// Handle all types including pointers using type switch
	// For pointer types, check for nil and dereference
	switch v := value.(type) {
	// Pointer types - check for nil and dereference
	case *float64:
		if v != nil {
			sb.WriteString(strconv.FormatFloat(*v, 'G', -1, 64))
		} else {
			sb.WriteString("null")
		}
	case *float32:
		if v != nil {
			sb.WriteString(strconv.FormatFloat(float64(*v), 'G', -1, 32))
		} else {
			sb.WriteString("null")
		}
	case *int64:
		if v != nil {
			sb.WriteString(strconv.FormatInt(*v, 10))
		} else {
			sb.WriteString("null")
		}
	case *int32:
		if v != nil {
			sb.WriteString(strconv.FormatInt(int64(*v), 10))
		} else {
			sb.WriteString("null")
		}
	case *int16:
		if v != nil {
			sb.WriteString(strconv.FormatInt(int64(*v), 10))
		} else {
			sb.WriteString("null")
		}
	case *int8:
		if v != nil {
			sb.WriteString(strconv.FormatInt(int64(*v), 10))
		} else {
			sb.WriteString("null")
		}
	case *int:
		if v != nil {
			sb.WriteString(strconv.FormatInt(int64(*v), 10))
		} else {
			sb.WriteString("null")
		}
	case *uint64:
		if v != nil {
			sb.WriteString(strconv.FormatUint(*v, 10))
		} else {
			sb.WriteString("null")
		}
	case *uint32:
		if v != nil {
			sb.WriteString(strconv.FormatUint(uint64(*v), 10))
		} else {
			sb.WriteString("null")
		}
	case *uint16:
		if v != nil {
			sb.WriteString(strconv.FormatUint(uint64(*v), 10))
		} else {
			sb.WriteString("null")
		}
	case *uint8:
		if v != nil {
			sb.WriteString(strconv.FormatUint(uint64(*v), 10))
		} else {
			sb.WriteString("null")
		}
	case *uint:
		if v != nil {
			sb.WriteString(strconv.FormatUint(uint64(*v), 10))
		} else {
			sb.WriteString("null")
		}
	case *string:
		if v != nil {
			sb.WriteString(*v)
		} else {
			sb.WriteString("null")
		}
	case *bool:
		if v != nil {
			sb.WriteString(strconv.FormatBool(*v))
		} else {
			sb.WriteString("null")
		}
	case *time.Time:
		if v != nil {
			sb.WriteString(v.Format(time.DateTime))
		} else {
			sb.WriteString("null")
		}
	case *gocql.UUID:
		if v != nil {
			sb.WriteString(v.String())
		} else {
			sb.WriteString("null")
		}
	case *time.Duration:
		if v != nil {
			_, _ = fmt.Fprintf(sb, "%v", *v)
		} else {
			sb.WriteString("null")
		}
	case *big.Int:
		if v != nil {
			sb.WriteString(v.String())
		} else {
			sb.WriteString("null")
		}
	case *inf.Dec:
		if v != nil {
			sb.WriteString(v.String())
		} else {
			sb.WriteString("null")
		}
	case *[]byte:
		if v != nil {
			sb.Write(*v)
		} else {
			sb.WriteString("null")
		}
	// Non-pointer types
	case float32:
		sb.WriteString(strconv.FormatFloat(float64(v), 'G', -1, 32))
	case float64:
		sb.WriteString(strconv.FormatFloat(v, 'G', -1, 64))
	case int8:
		sb.WriteString(strconv.FormatInt(int64(v), 10))
	case int16:
		sb.WriteString(strconv.FormatInt(int64(v), 10))
	case int:
		sb.WriteString(strconv.FormatInt(int64(v), 10))
	case int32:
		sb.WriteString(strconv.FormatInt(int64(v), 10))
	case int64:
		sb.WriteString(strconv.FormatInt(v, 10))
	case uint:
		sb.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint8:
		sb.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint16:
		sb.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint32:
		sb.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint64:
		sb.WriteString(strconv.FormatUint(v, 10))
	case gocql.UUID:
		sb.WriteString(v.String())
	case time.Time:
		sb.WriteString(v.Format(time.DateTime))
	case time.Duration:
		_, _ = fmt.Fprintf(sb, "%v", v)
	case []byte:
		sb.Write(v)
	case string:
		sb.WriteString(v)
	case bool:
		sb.WriteString(strconv.FormatBool(v))
	default:
		// Fall back to %v (not %#v) to avoid printing pointer addresses
		_, _ = fmt.Fprintf(sb, "%v", v)
	}

	return sb.String()
}

func pks(t *typedef.Table, rows Rows) *strset.Set {
	if len(rows) == 0 {
		return strset.NewWithSize(0)
	}

	keySet := strset.NewWithSize(len(rows))

	var sb strings.Builder
	sb.Grow(64)

	for _, row := range rows {
		// Reset string builder for each row to build composite key
		sb.Reset()

		// Build composite key from all partition and clustering keys
		for _, pk := range t.PartitionKeys {
			formatRows(&sb, pk.Name, row.Get(pk.Name))
			sb.WriteByte(',')
		}

		for _, ck := range t.ClusteringKeys {
			formatRows(&sb, ck.Name, row.Get(ck.Name))
			sb.WriteByte(',')
		}

		// Add the composite key (trimming trailing comma)
		compositeKey := strings.TrimRight(sb.String(), ",")
		keySet.Add(compositeKey)
	}

	return keySet
}
