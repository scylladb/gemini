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

package typedef

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"math/rand/v2"
	"net"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/utils"
)

type SimpleTypes []SimpleType

func (l SimpleTypes) Contains(colType Type) bool {
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

func (l SimpleTypes) Random(r *rand.Rand) SimpleType {
	return l[r.IntN(len(l))]
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

func (st SimpleType) LenValue() int {
	return 1
}

func (st SimpleType) CQLPretty(builder *bytes.Buffer, value any) error {
	switch st {
	case TYPE_INET:
		builder.WriteRune('\'')
		defer builder.WriteRune('\'')

		switch v := value.(type) {
		case net.IP:
			builder.WriteString(v.String())
		case net.IPMask:
			builder.WriteString(v.String())
		case string:
			builder.WriteString(v)
		default:
			return errors.Errorf("unexpected inet value [%T]%+v", value, value)
		}

		return nil
	case TYPE_ASCII, TYPE_TEXT, TYPE_VARCHAR, TYPE_DATE:
		if v, ok := value.(string); ok {
			builder.WriteRune('\'')
			builder.WriteString(v)
			builder.WriteRune('\'')
			return nil
		}

		return errors.Errorf("unexpected string value [%T]%+v", value, value)
	case TYPE_BLOB:
		if v, ok := value.(string); ok {
			if len(v) > 100 {
				v = v[:100]
			}
			builder.WriteString("textasblob('")
			builder.WriteString(v)
			builder.WriteString("')")
			return nil
		}

		return errors.Errorf("unexpected blob value [%T]%+v", value, value)
	case TYPE_BIGINT, TYPE_INT, TYPE_SMALLINT, TYPE_TINYINT:
		var i int64
		switch v := value.(type) {
		case int8:
			i = int64(v)
		case int16:
			i = int64(v)
		case int32:
			i = int64(v)
		case int:
			i = int64(v)
		case int64:
			i = v
		case *big.Int:
			builder.WriteString(v.Text(10))

			return nil
		default:
			return errors.Errorf("unexpected int value [%T]%+v", value, value)
		}

		builder.WriteString(strconv.FormatInt(i, 10))

		return nil
	case TYPE_DECIMAL, TYPE_DOUBLE, TYPE_FLOAT:
		var f float64
		switch v := value.(type) {
		case float32:
			f = float64(v)
		case float64:
			f = v
		case *inf.Dec:
			builder.WriteString(v.String())

			return nil
		default:
			return errors.Errorf("unexpected float value [%T]%+v", value, value)
		}

		builder.WriteString(strconv.FormatFloat(f, 'f', 2, 64))
		return nil
	case TYPE_BOOLEAN:
		if v, ok := value.(bool); ok {
			builder.WriteString(strconv.FormatBool(v))

			return nil
		}

		return errors.Errorf("unexpected boolean value [%T]%+v", value, value)
	case TYPE_TIME:
		if v, ok := value.(int64); ok {
			builder.WriteRune('\'')
			// CQL supports only 3 digits microseconds:
			// '10:10:55.83275+0000': marshaling error: Milliseconds length exceeds expected (5)"
			builder.WriteString(time.Time{}.Add(time.Duration(v)).Format("15:04:05.999"))
			builder.WriteRune('\'')

			return nil
		}

		return errors.Errorf("unexpected time value [%T]%+v", value, value)
	case TYPE_TIMESTAMP:
		if v, ok := value.(int64); ok {
			// CQL supports only 3 digits milliseconds:
			// '1976-03-25T10:10:55.83275+0000': marshaling error: Milliseconds length exceeds expected (5)"
			builder.WriteString(time.UnixMilli(v).UTC().Format("'2006-01-02T15:04:05.999-0700'"))
			return nil
		}

		return errors.Errorf("unexpected timestamp value [%T]%+v", value, value)
	case TYPE_DURATION, TYPE_TIMEUUID, TYPE_UUID:
		switch v := value.(type) {
		case string:
			builder.WriteString(v)
		case time.Duration:
			builder.WriteString(v.String())
		case gocql.UUID:
			builder.WriteString(v.String())
		default:
			return errors.Errorf("unexpected (duration|timeuuid|uuid) value [%T]%+v", value, value)
		}
		return nil
	case TYPE_VARINT:
		if s, ok := value.(*big.Int); ok {
			builder.WriteString(s.Text(10))
			return nil
		}

		return errors.Errorf("unexpected varint value [%T]%+v", value, value)
	default:
		return errors.Errorf("cql pretty: not supported type %s [%T]%+v", st, value, value)
	}
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

func (st SimpleType) GenJSONValue(r *rand.Rand, p *PartitionRangeConfig) any {
	switch st {
	case TYPE_BLOB:
		ln := r.IntN(p.MaxBlobLength) + p.MinBlobLength
		return "0x" + hex.EncodeToString([]byte(utils.RandString(r, ln)))
	case TYPE_TIME:
		return time.Unix(0, utils.RandTime(r)).UTC().Format("15:04:05.000000000")
	}
	return st.genValue(r, p)
}

func (st SimpleType) GenValue(r *rand.Rand, p *PartitionRangeConfig) []any {
	return []any{st.genValue(r, p)}
}

func (st SimpleType) genValue(r *rand.Rand, p *PartitionRangeConfig) any {
	switch st {
	case TYPE_ASCII, TYPE_TEXT, TYPE_VARCHAR:
		ln := r.IntN(p.MaxStringLength) + p.MinStringLength
		return utils.RandString(r, ln)
	case TYPE_BLOB:
		ln := r.IntN(p.MaxBlobLength) + p.MinBlobLength
		return hex.EncodeToString([]byte(utils.RandString(r, ln)))
	case TYPE_BIGINT:
		return r.Int64()
	case TYPE_BOOLEAN:
		return r.Int()%2 == 0
	case TYPE_DATE:
		return utils.RandDateStr(r)
	case TYPE_TIME:
		return utils.RandTime(r)
	case TYPE_TIMESTAMP:
		return utils.RandTimestamp(r)
	case TYPE_DECIMAL:
		return inf.NewDec(r.Int64(), 3)
	case TYPE_DOUBLE:
		return r.Float64()
	case TYPE_DURATION:
		return (time.Minute * time.Duration(r.IntN(100))).String()
	case TYPE_FLOAT:
		return r.Float32()
	case TYPE_INET:
		return net.ParseIP(utils.RandIPV4Address(r, r.IntN(255), 2)).String()
	case TYPE_INT:
		return r.Int32()
	case TYPE_SMALLINT:
		return int16(r.Uint64N(65536))
	case TYPE_TIMEUUID, TYPE_UUID:
		return utils.UUIDFromTime(r)
	case TYPE_TINYINT:
		return int8(r.Uint64N(256))
	case TYPE_VARINT:
		return big.NewInt(r.Int64())
	default:
		panic(fmt.Sprintf("generate value: not supported type %s", st))
	}
}

// ValueVariationsNumber returns number of bytes generated value holds
func (st SimpleType) ValueVariationsNumber(p *PartitionRangeConfig) float64 {
	switch st {
	case TYPE_ASCII, TYPE_TEXT, TYPE_VARCHAR:
		return math.Pow(2, float64(p.MaxStringLength))
	case TYPE_BLOB:
		return math.Pow(2, float64(p.MaxBlobLength))
	case TYPE_BIGINT:
		return 2 ^ 64
	case TYPE_BOOLEAN:
		return 2
	case TYPE_DATE:
		return 10000*365 + 2000*4
	case TYPE_TIME:
		return 86400000000000
	case TYPE_TIMESTAMP:
		return 2 ^ 64
	case TYPE_DECIMAL:
		return 2 ^ 64
	case TYPE_DOUBLE:
		return 2 ^ 64
	case TYPE_DURATION:
		return 2 ^ 64
	case TYPE_FLOAT:
		return 2 ^ 64
	case TYPE_INET:
		return 2 ^ 32
	case TYPE_INT:
		return 2 ^ 32
	case TYPE_SMALLINT:
		return 2 ^ 16
	case TYPE_TIMEUUID, TYPE_UUID:
		return 2 ^ 64
	case TYPE_TINYINT:
		return 2 ^ 8
	case TYPE_VARINT:
		return 2 ^ 64
	default:
		panic(fmt.Sprintf("generate value: not supported type %s", st))
	}
}
