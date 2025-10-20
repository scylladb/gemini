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
	"time"

	"github.com/gocql/gocql"
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

//nolint:gocyclo
func (st SimpleType) CQLType() gocql.TypeInfo {
	switch st {
	case TypeAscii:
		return goCQLTypeMap[gocql.TypeAscii]
	case TypeText:
		return goCQLTypeMap[gocql.TypeText]
	case TypeVarchar:
		return goCQLTypeMap[gocql.TypeVarchar]
	case TypeBlob:
		return goCQLTypeMap[gocql.TypeBlob]
	case TypeBigint:
		return goCQLTypeMap[gocql.TypeBigInt]
	case TypeBoolean:
		return goCQLTypeMap[gocql.TypeBoolean]
	case TypeDate:
		return goCQLTypeMap[gocql.TypeDate]
	case TypeTime:
		return goCQLTypeMap[gocql.TypeTime]
	case TypeTimestamp:
		return goCQLTypeMap[gocql.TypeTimestamp]
	case TypeDecimal:
		return goCQLTypeMap[gocql.TypeDecimal]
	case TypeDouble:
		return goCQLTypeMap[gocql.TypeDouble]
	case TypeDuration:
		return goCQLTypeMap[gocql.TypeDuration]
	case TypeFloat:
		return goCQLTypeMap[gocql.TypeFloat]
	case TypeInet:
		return goCQLTypeMap[gocql.TypeInet]
	case TypeInt:
		return goCQLTypeMap[gocql.TypeInt]
	case TypeSmallint:
		return goCQLTypeMap[gocql.TypeSmallInt]
	case TypeTimeuuid:
		return goCQLTypeMap[gocql.TypeTimeUUID]
	case TypeUuid:
		return goCQLTypeMap[gocql.TypeUUID]
	case TypeTinyint:
		return goCQLTypeMap[gocql.TypeTinyInt]
	case TypeVarint:
		return goCQLTypeMap[gocql.TypeVarint]
	default:
		panic(fmt.Sprintf("gocql type not supported %s", st))
	}
}

func (st SimpleType) Indexable() bool {
	return st != TypeDuration
}

func (st SimpleType) GenJSONValue(r utils.Random, p *PartitionRangeConfig) any {
	switch st {
	case TypeBlob:
		ln := r.IntN(p.MaxBlobLength) + p.MinBlobLength
		buffer := bytes.NewBuffer(nil)
		buffer.Grow(ln*2 + 2)
		buffer.WriteString("0x")
		encoder := hex.NewEncoder(buffer)
		_, _ = encoder.Write(utils.RandomBytes(r, ln))
		return utils.UnsafeString(buffer.Bytes())
	case TypeDecimal:
		return inf.NewDec(r.Int64N(math.MaxInt64), 3).String()
	case TypeUuid, TypeTimeuuid:
		return utils.UUIDFromTime(r).String()
	case TypeVarint:
		return big.NewInt(r.Int64N(math.MaxInt64)).String()
	case TypeDate:
		return utils.RandDate(r).Format(time.DateOnly)
	case TypeDuration:
		return utils.TimeDurationToScyllaDuration(utils.RandDuration(r))
	case TypeTime:
		return time.
			Unix(0, utils.RandTime(r)).
			UTC().
			Format("15:04:05.000000000")
	}

	return st.genValue(r, p)
}

func (st SimpleType) GenValue(r utils.Random, p *PartitionRangeConfig) []any {
	return []any{st.genValue(r, p)}
}

func (st SimpleType) GenValueOut(out []any, r utils.Random, p *PartitionRangeConfig) []any {
	out = append(out, st.genValue(r, p))
	return out
}

func (st SimpleType) genValue(r utils.Random, p *PartitionRangeConfig) any {
	switch st {
	case TypeBlob:
		return utils.RandomBytes(r, r.IntN(p.MaxStringLength)+p.MinStringLength)
	case TypeAscii, TypeText, TypeVarchar:
		return utils.RandString(r, r.IntN(p.MaxStringLength)+p.MinStringLength, false)
	case TypeBigint:
		return r.Int64()
	case TypeBoolean:
		return r.IntN(2) == 0
	case TypeDate:
		return utils.RandDate(r)
	case TypeTime:
		return utils.RandTime(r)
	case TypeTimestamp:
		return utils.RandTimestamp(r)
	case TypeDecimal:
		return inf.NewDec(r.Int64(), 3)
	case TypeDouble:
		return float64(r.Uint64()<<11>>11) / (1 << 53)
	case TypeDuration:
		return utils.RandDuration(r)
	case TypeFloat:
		return float32(r.Uint32()<<8>>8) / (1 << 24)
	case TypeInet:
		return utils.RandIPV4Address(r, r.IntN(math.MaxUint8), 1+r.IntN(3))
	case TypeInt:
		return int32(r.Int64N(math.MaxInt32))
	case TypeSmallint:
		return int16(r.Uint64N(math.MaxUint16))
	case TypeUuid:
		uuid, _ := gocql.RandomUUID()
		return uuid
	case TypeTimeuuid:
		return utils.UUIDFromTime(r)
	case TypeTinyint:
		return int8(r.Uint64N(math.MaxUint8))
	case TypeVarint:
		return big.NewInt(r.Int64())
	default:
		panic(fmt.Sprintf("generate value: not supported type %s", st))
	}
}

// ValueVariationsNumber returns the number of bytes generated value holds
func (st SimpleType) ValueVariationsNumber(p *PartitionRangeConfig) float64 {
	switch st {
	case TypeAscii, TypeText, TypeVarchar:
		return math.Pow(2, float64(p.MaxStringLength))
	case TypeBlob:
		return math.Pow(2, float64(p.MaxBlobLength))
	case TypeBoolean:
		return 2
	case TypeDate:
		return 10000*365 + 2000*4
	case TypeTime:
		return 86400000000000
	case TypeVarint, TypeTimeuuid, TypeUuid, TypeBigint, TypeTimestamp, TypeDecimal,
		TypeDouble, TypeDuration:
		return 2 ^ 64
	case TypeInet, TypeInt, TypeFloat:
		return 2 ^ 32
	case TypeSmallint:
		return 2 ^ 16
	case TypeTinyint:
		return 2 ^ 8
	default:
		panic(fmt.Sprintf("generate value: not supported type %s", st))
	}
}
