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
	"reflect"
	"slices"
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
	return slices.Contains(l, t)
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
	case TypeASCII:
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
	case TypeUUID:
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

func (st SimpleType) GenJSONValue(r utils.Random, p RangeConfig) any {
	switch st {
	case TypeBlob:
		ln := randLength(r, p.GetMinBlobLength(), p.GetMaxBlobLength())
		buffer := bytes.NewBuffer(nil)
		buffer.Grow(ln*2 + 2)
		buffer.WriteString("0x")
		encoder := hex.NewEncoder(buffer)
		_, _ = encoder.Write(utils.RandomBytes(r, ln))
		return utils.UnsafeString(buffer.Bytes())
	case TypeDecimal:
		return randDecimal(r).String()
	case TypeUUID, TypeTimeuuid:
		return utils.UUIDFromTime(r).String()
	case TypeVarint:
		return big.NewInt(randSigned(r)).String()
	case TypeDate:
		return utils.RandDate(r).Format(time.DateOnly)
	case TypeDuration:
		return utils.TimeDurationToScyllaDuration(utils.RandDuration(r))
	case TypeTime:
		return time.
			Unix(0, utils.RandTime(r)).
			UTC().
			Format("15:04:05.000000000")
	default:
		return st.genValue(r, p)
	}
}

func (st SimpleType) GenValue(r utils.Random, p RangeConfig) []any {
	return []any{st.genValue(r, p)}
}

func (st SimpleType) GenValueOut(out []any, r utils.Random, p RangeConfig) []any {
	out = append(out, st.genValue(r, p))
	return out
}

// randLength returns a length in [minLen, maxLen]. A max below the min is
// taken as the min, so a misconfigured range never reaches rand.IntN with a
// non-positive bound.
func randLength(r utils.Random, minLen, maxLen int) int {
	minLen = max(minLen, 0)
	maxLen = max(maxLen, minLen)

	return minLen + r.IntN(maxLen-minLen+1)
}

// randSigned returns an int64 over its whole range, negative values included.
func randSigned(r utils.Random) int64 {
	return int64(r.Uint64())
}

// randDecimal returns a decimal with a random sign and a scale in [0, 9]. CQL
// keeps the scale of a decimal, so a cluster that changes it changes the value.
func randDecimal(r utils.Random) *inf.Dec {
	return inf.NewDec(randSigned(r), inf.Scale(r.IntN(10)))
}

// randFloat64 returns a finite double. One draw in four is a random bit
// pattern: it covers -0, subnormal numbers and the largest magnitudes. The
// others are values of everyday magnitude with a random sign. NaN and ±Inf
// are left out, because a double can be a key column.
func randFloat64(r utils.Random) float64 {
	if r.IntN(4) == 0 {
		f := math.Float64frombits(r.Uint64())
		if math.IsNaN(f) || math.IsInf(f, 0) {
			// Clearing the top exponent bit leaves a finite number.
			f = math.Float64frombits(math.Float64bits(f) &^ (1 << 62))
		}

		return f
	}

	f := float64(r.Uint64()<<11>>11) / (1 << 53) * math.Pow10(r.IntN(10))
	if r.IntN(2) == 0 {
		f = -f
	}

	return f
}

// randFloat32 returns a finite float, as randFloat64 does for a double.
func randFloat32(r utils.Random) float32 {
	if r.IntN(4) == 0 {
		f := math.Float32frombits(r.Uint32())
		if f != f || math.IsInf(float64(f), 0) {
			// Clearing the top exponent bit leaves a finite number.
			f = math.Float32frombits(math.Float32bits(f) &^ (1 << 30))
		}

		return f
	}

	f := float32(r.Uint32()<<8>>8) / (1 << 24) * float32(math.Pow10(r.IntN(6)))
	if r.IntN(2) == 0 {
		f = -f
	}

	return f
}

func (st SimpleType) genValue(r utils.Random, p RangeConfig) any {
	switch st {
	case TypeBlob:
		return utils.RandomBytes(r, randLength(r, p.GetMinBlobLength(), p.GetMaxBlobLength()))
	case TypeASCII, TypeText, TypeVarchar:
		return utils.RandString(r, randLength(r, p.GetMinStringLength(), p.GetMaxStringLength()), false)
	case TypeBigint:
		return randSigned(r)
	case TypeBoolean:
		return r.IntN(2) == 0
	case TypeDate:
		return utils.RandDate(r)
	case TypeTime:
		return time.Duration(utils.RandTime(r))
	case TypeTimestamp:
		return utils.RandTimestamp(r)
	case TypeDecimal:
		return randDecimal(r)
	case TypeDouble:
		return randFloat64(r)
	case TypeDuration:
		return utils.RandDuration(r)
	case TypeFloat:
		return randFloat32(r)
	case TypeInet:
		return utils.RandIPV4Address(r, r.IntN(math.MaxUint8), 1+r.IntN(3))
	case TypeInt:
		return int32(r.Uint32())
	case TypeSmallint:
		return int16(r.Uint32())
	case TypeUUID:
		return utils.RandomUUID(r)
	case TypeTimeuuid:
		return utils.UUIDFromTime(r)
	case TypeTinyint:
		return int8(r.Uint32())
	case TypeVarint:
		return big.NewInt(randSigned(r))
	default:
		panic(fmt.Sprintf("generate value: not supported type %s", st))
	}
}

// goType returns the Go type of the values that genValue makes for st.
func (st SimpleType) goType() reflect.Type {
	switch st {
	case TypeBlob:
		return reflect.TypeFor[[]byte]()
	case TypeASCII, TypeText, TypeVarchar, TypeInet:
		return reflect.TypeFor[string]()
	case TypeBigint, TypeTimestamp:
		return reflect.TypeFor[int64]()
	case TypeBoolean:
		return reflect.TypeFor[bool]()
	case TypeDate:
		return reflect.TypeFor[time.Time]()
	case TypeTime, TypeDuration:
		return reflect.TypeFor[time.Duration]()
	case TypeDecimal:
		return reflect.TypeFor[*inf.Dec]()
	case TypeDouble:
		return reflect.TypeFor[float64]()
	case TypeFloat:
		return reflect.TypeFor[float32]()
	case TypeInt:
		return reflect.TypeFor[int32]()
	case TypeSmallint:
		return reflect.TypeFor[int16]()
	case TypeTinyint:
		return reflect.TypeFor[int8]()
	case TypeUUID, TypeTimeuuid:
		return reflect.TypeFor[gocql.UUID]()
	case TypeVarint:
		return reflect.TypeFor[*big.Int]()
	default:
		panic(fmt.Sprintf("generate value: not supported type %s", st))
	}
}

// jsonGoType returns the Go type of the values that GenJSONValue makes for st.
func (st SimpleType) jsonGoType() reflect.Type {
	switch st {
	case TypeBlob, TypeDecimal, TypeUUID, TypeTimeuuid, TypeVarint, TypeDate, TypeDuration, TypeTime:
		return reflect.TypeFor[string]()
	default:
		return st.goType()
	}
}

// ValueVariationsNumber returns the number of bytes generated value holds
func (st SimpleType) ValueVariationsNumber(p RangeConfig) float64 {
	switch st {
	case TypeASCII, TypeText, TypeVarchar:
		return math.Pow(2, float64(p.GetMaxStringLength()))
	case TypeBlob:
		return math.Pow(2, float64(p.GetMaxBlobLength()))
	case TypeBoolean:
		return 2
	case TypeDate:
		return 10000*365 + 2000*4
	case TypeTime:
		return 86400000000000
	case TypeVarint, TypeTimeuuid, TypeUUID, TypeBigint, TypeTimestamp, TypeDecimal,
		TypeDouble, TypeDuration:
		return math.MaxUint64
	case TypeInet, TypeInt, TypeFloat:
		return math.MaxUint32
	case TypeSmallint:
		return math.MaxUint16
	case TypeTinyint:
		return math.MaxUint8
	default:
		panic(fmt.Sprintf("generate value: not supported type %s", st))
	}
}
