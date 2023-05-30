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

package coltypes

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"net"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"golang.org/x/exp/rand"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

type SimpleTypes []SimpleType

func (l SimpleTypes) Contains(colType typedef.Type) bool {
	t, ok := colType.(SimpleType)
	if !ok {
		return false
	}
	for _, typ := range l {
		if t == typ {
			return true
		}
	}
	return false
}

func (l SimpleTypes) Random() SimpleType {
	return l[rand.Intn(len(l))]
}

type SimpleType string

func (st SimpleType) Name() string {
	return string(st)
}

func (st SimpleType) CQLDef() string {
	return string(st)
}

func (st SimpleType) CQLHolder() string {
	return "?"
}

func (st SimpleType) CQLPretty(query string, value []interface{}) (string, int) {
	if len(value) == 0 {
		return query, 0
	}
	var replacement string
	switch st {
	case TYPE_ASCII, TYPE_TEXT, TYPE_VARCHAR, TYPE_INET, TYPE_DATE:
		replacement = fmt.Sprintf("'%s'", value[0])
	case TYPE_BLOB:
		if v, ok := value[0].(string); ok {
			if len(v) > 100 {
				v = v[:100]
			}
			replacement = "textasblob('" + v + "')"
		}
	case TYPE_BIGINT, TYPE_INT, TYPE_SMALLINT, TYPE_TINYINT:
		replacement = fmt.Sprintf("%d", value[0])
	case TYPE_DECIMAL, TYPE_DOUBLE, TYPE_FLOAT:
		replacement = fmt.Sprintf("%.2f", value[0])
	case TYPE_BOOLEAN:
		if v, ok := value[0].(bool); ok {
			replacement = fmt.Sprintf("%t", v)
		}
	case TYPE_TIME, TYPE_TIMESTAMP:
		if v, ok := value[0].(time.Time); ok {
			replacement = "'" + v.Format(time.RFC3339) + "'"
		}
	case TYPE_DURATION, TYPE_TIMEUUID, TYPE_UUID:
		replacement = fmt.Sprintf("%s", value[0])
	case TYPE_VARINT:
		if s, ok := value[0].(*big.Int); ok {
			replacement = fmt.Sprintf("%d", s.Int64())
		}
	default:
		panic(fmt.Sprintf("cql pretty: not supported type %s", st))
	}
	return strings.Replace(query, "?", replacement, 1), 1
}

func (st SimpleType) CQLType() gocql.TypeInfo {
	switch st {
	case TYPE_ASCII:
		return goCQLTypeMap[gocql.TypeAscii]
	case TYPE_TEXT:
		return goCQLTypeMap[gocql.TypeText]
	case TYPE_VARCHAR:
		return goCQLTypeMap[gocql.TypeVarchar]
	case TYPE_BLOB:
		return goCQLTypeMap[gocql.TypeBlob]
	case TYPE_BIGINT:
		return goCQLTypeMap[gocql.TypeBigInt]
	case TYPE_BOOLEAN:
		return goCQLTypeMap[gocql.TypeBoolean]
	case TYPE_DATE:
		return goCQLTypeMap[gocql.TypeDate]
	case TYPE_TIME:
		return goCQLTypeMap[gocql.TypeTime]
	case TYPE_TIMESTAMP:
		return goCQLTypeMap[gocql.TypeTimestamp]
	case TYPE_DECIMAL:
		return goCQLTypeMap[gocql.TypeDecimal]
	case TYPE_DOUBLE:
		return goCQLTypeMap[gocql.TypeDouble]
	case TYPE_DURATION:
		return goCQLTypeMap[gocql.TypeDuration]
	case TYPE_FLOAT:
		return goCQLTypeMap[gocql.TypeFloat]
	case TYPE_INET:
		return goCQLTypeMap[gocql.TypeInet]
	case TYPE_INT:
		return goCQLTypeMap[gocql.TypeInt]
	case TYPE_SMALLINT:
		return goCQLTypeMap[gocql.TypeSmallInt]
	case TYPE_TIMEUUID:
		return goCQLTypeMap[gocql.TypeTimeUUID]
	case TYPE_UUID:
		return goCQLTypeMap[gocql.TypeUUID]
	case TYPE_TINYINT:
		return goCQLTypeMap[gocql.TypeTinyInt]
	case TYPE_VARINT:
		return goCQLTypeMap[gocql.TypeVarint]
	default:
		panic(fmt.Sprintf("gocql type not supported %s", st))
	}
}

func (st SimpleType) Indexable() bool {
	return st != TYPE_DURATION
}

func (st SimpleType) GenValue(r *rand.Rand, p *typedef.PartitionRangeConfig) []interface{} {
	var val interface{}
	switch st {
	case TYPE_ASCII, TYPE_TEXT, TYPE_VARCHAR:
		ln := r.Intn(p.MaxStringLength) + p.MinStringLength
		val = utils.RandString(r, ln)
	case TYPE_BLOB:
		ln := r.Intn(p.MaxBlobLength) + p.MinBlobLength
		val = hex.EncodeToString([]byte(utils.RandString(r, ln)))
	case TYPE_BIGINT:
		val = r.Int63()
	case TYPE_BOOLEAN:
		val = r.Int()%2 == 0
	case TYPE_DATE:
		val = utils.RandDate(r)
	case TYPE_TIME, TYPE_TIMESTAMP:
		val = utils.RandTime(r)
	case TYPE_DECIMAL:
		val = inf.NewDec(r.Int63(), 3)
	case TYPE_DOUBLE:
		val = r.Float64()
	case TYPE_DURATION:
		val = (time.Minute * time.Duration(r.Intn(100))).String()
	case TYPE_FLOAT:
		val = r.Float32()
	case TYPE_INET:
		val = net.ParseIP(utils.RandIpV4Address(r, r.Intn(255), 2)).String()
	case TYPE_INT:
		val = r.Int31()
	case TYPE_SMALLINT:
		val = int16(r.Int31())
	case TYPE_TIMEUUID, TYPE_UUID:
		r := gocql.UUIDFromTime(utils.RandTime(r))
		val = r.String()
	case TYPE_TINYINT:
		val = int8(r.Int31())
	case TYPE_VARINT:
		val = big.NewInt(r.Int63())
	default:
		panic(fmt.Sprintf("generate value: not supported type %s", st))
	}
	return []interface{}{
		val,
	}
}
