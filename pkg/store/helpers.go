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

func formatRows(sb *strings.Builder, key string, value any) string {
	sb.Reset()
	sb.WriteString(key)
	sb.WriteByte('=')

	switch v := value.(type) {
	case float32:
		sb.WriteString(strconv.FormatFloat(float64(v), 'G', -1, 32))
	case float64:
		sb.WriteString(strconv.FormatFloat(v, 'G', -1, 64))
	case int8:
		sb.WriteString(strconv.FormatInt(int64(v), 10))
	case int16:
		sb.WriteString(strconv.FormatInt(int64(v), 10))
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
	case *big.Int:
		sb.WriteString(v.String())
	case *inf.Dec:
		sb.WriteString(v.String())
	case nil:
	case []byte:
		sb.Write(v)
	case string:
		sb.WriteString(v)
	case int:
		sb.WriteString(strconv.FormatInt(int64(v), 10))
	case int32:
		sb.WriteString(strconv.FormatInt(int64(v), 10))
	case int64:
		sb.WriteString(strconv.FormatInt(v, 10))
	default:
		_, _ = fmt.Fprintf(sb, "%#v", v)
	}

	return sb.String()
}

func pks(t *typedef.Table, rows Rows) *strset.Set {
	if len(rows) == 0 {
		return strset.NewWithSize(0)
	}

	keySet := strset.NewWithSize(len(rows) * (len(t.PartitionKeys) + len(t.ClusteringKeys)))

	var sb strings.Builder
	sb.Grow(64)

	for _, row := range rows {
		for _, pk := range t.PartitionKeys {
			keySet.Add(formatRows(&sb, pk.Name, row[pk.Name]))
		}

		for _, ck := range t.ClusteringKeys {
			keySet.Add(formatRows(&sb, ck.Name, row[ck.Name]))
		}
	}

	return keySet
}
