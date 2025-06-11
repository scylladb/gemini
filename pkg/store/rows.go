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
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
)

type (
	Row map[string]any

	Rows []Row
)

func (r Rows) Len() int {
	return len(r)
}

func (r Rows) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r Rows) Less(i, j int) bool {
	return rowsCmp(r[i], r[j]) < 0
}

func rowsCmp(i, j Row) int {
	switch mis := i["pk0"].(type) {
	case []byte:
		mjs, _ := j["pk0"].([]byte)
		return bytes.Compare(mis, mjs)
	case string:
		mjs, _ := j["pk0"].(string)
		return strings.Compare(mis, mjs)
	case float32:
		mjs, _ := j["pk0"].(float32)
		return cmp.Compare(mis, mjs)
	case float64:
		mjs, _ := j["pk0"].(float64)
		return cmp.Compare(mis, mjs)
	case int:
		mjs, _ := j["pk0"].(int)
		return cmp.Compare(mis, mjs)
	case int8:
		mjs, _ := j["pk0"].(int8)
		return cmp.Compare(mis, mjs)
	case int16:
		mjs, _ := j["pk0"].(int16)
		return cmp.Compare(mis, mjs)
	case int32:
		mjs, _ := j["pk0"].(int32)
		return cmp.Compare(mis, mjs)
	case int64:
		mjs, _ := j["pk0"].(int64)
		return cmp.Compare(mis, mjs)
	case uint:
		mjs, _ := j["pk0"].(uint)
		return cmp.Compare(mis, mjs)
	case uint8:
		mjs, _ := j["pk0"].(uint8)
		return cmp.Compare(mis, mjs)
	case uint16:
		mjs, _ := j["pk0"].(uint16)
		return cmp.Compare(mis, mjs)
	case uint32:
		mjs, _ := j["pk0"].(uint32)
		return cmp.Compare(mis, mjs)
	case uint64:
		mjs, _ := j["pk0"].(uint64)
		return cmp.Compare(mis, mjs)
	case gocql.UUID:
		mjs, _ := j["pk0"].(gocql.UUID)
		return bytes.Compare(mis[:], mjs[:])
	case time.Time:
		mjs, _ := j["pk0"].(time.Time)
		return mis.Compare(mjs)
	case *big.Int:
		mjs, _ := j["pk0"].(*big.Int)
		return mis.Cmp(mjs)
	case *inf.Dec:
		mjs, _ := j["pk0"].(*inf.Dec)
		return mis.Cmp(mjs)
	case nil:
		return 0
	default:
		panic(fmt.Sprintf("unhandled type [%T][%v]\n", mis, mis))
	}
}
