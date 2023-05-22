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
	"strings"
	"time"

	"github.com/scylladb/gemini/pkg/testschema"

	"github.com/gocql/gocql"
)

func pks(t *testschema.Table, rows []map[string]interface{}) []string {
	var keySet []string
	for _, row := range rows {
		keys := make([]string, 0, len(t.PartitionKeys)+len(t.ClusteringKeys))
		keys = extractRowValues(keys, t.PartitionKeys, row)
		keys = extractRowValues(keys, t.ClusteringKeys, row)
		keySet = append(keySet, strings.Join(keys, ", 	"))
	}
	return keySet
}

func extractRowValues(values []string, columns testschema.Columns, row map[string]interface{}) []string {
	for _, pk := range columns {
		values = append(values, fmt.Sprintf(pk.Name+"=%v", row[pk.Name]))
	}
	return values
}

func lt(mi, mj map[string]interface{}) bool {
	switch mis := mi["pk0"].(type) {
	case []byte:
		mjs, _ := mj["pk0"].([]byte)
		return string(mis) < string(mjs)
	case string:
		mjs, _ := mj["pk0"].(string)
		return mis < mjs
	case int:
		mjs, _ := mj["pk0"].(int)
		return mis < mjs
	case int8:
		mjs, _ := mj["pk0"].(int8)
		return mis < mjs
	case int16:
		mjs, _ := mj["pk0"].(int16)
		return mis < mjs
	case int32:
		mjs, _ := mj["pk0"].(int32)
		return mis < mjs
	case int64:
		mjs, _ := mj["pk0"].(int64)
		return mis < mjs
	case gocql.UUID:
		mjs, _ := mj["pk0"].(gocql.UUID)
		return mis.String() < mjs.String()
	case time.Time:
		mjs, _ := mj["pk0"].(time.Time)
		return mis.UnixNano() < mjs.UnixNano()
	case *big.Int:
		mjs, _ := mj["pk0"].(*big.Int)
		return mis.Cmp(mjs) < 0
	default:
		msg := fmt.Sprintf("unhandled type %T\n", mis)
		time.Sleep(time.Second)
		panic(msg)
	}
}

func loadSet(iter *gocql.Iter) []map[string]interface{} {
	var rows []map[string]interface{}
	for {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}
		rows = append(rows, row)
	}
	return rows
}
