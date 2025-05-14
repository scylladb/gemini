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
	"math"
	"math/rand/v2"
	"reflect"
	"strconv"
	"sync/atomic"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"

	"github.com/scylladb/gemini/pkg/utils"
)

// nolint:revive
const (
	TypeUdt   = "udt"
	TypeMap   = "map"
	TypeList  = "list"
	TypeSet   = "set"
	TypeTuple = "tuple"
)

// nolint:revive
const (
	TypeAscii     = SimpleType("ascii")
	TypeBigint    = SimpleType("bigint")
	TypeBlob      = SimpleType("blob")
	TypeBoolean   = SimpleType("boolean")
	TypeDate      = SimpleType("date")
	TypeDecimal   = SimpleType("decimal")
	TypeDouble    = SimpleType("double")
	TypeDuration  = SimpleType("duration")
	TypeFloat     = SimpleType("float")
	TypeInet      = SimpleType("inet")
	TypeInt       = SimpleType("int")
	TypeSmallint  = SimpleType("smallint")
	TypeText      = SimpleType("text")
	TypeTime      = SimpleType("time")
	TypeTimestamp = SimpleType("timestamp")
	TypeTimeuuid  = SimpleType("timeuuid")
	TypeTinyint   = SimpleType("tinyint")
	TypeUuid      = SimpleType("uuid")
	TypeVarchar   = SimpleType("varchar")
	TypeVarint    = SimpleType("varint")
)

const (
	maxMapSize = 10
	maxBagSize = 10
)

var (
	TypesMapKeyBlacklist = map[SimpleType]struct{}{
		TypeBlob:     {},
		TypeDuration: {},
	}
	TypesForIndex = SimpleTypes{
		TypeDecimal,
		TypeDouble,
		TypeFloat,
		TypeInt,
		TypeSmallint,
		TypeTinyint,
		TypeVarint,
	}
	PartitionKeyTypes = SimpleTypes{
		TypeAscii, TypeBigint, TypeDate, TypeDecimal, TypeDouble,
		TypeFloat, TypeInet, TypeInt, TypeSmallint, TypeText, TypeTime, TypeTimestamp, TypeTimeuuid,
		TypeTinyint, TypeUuid, TypeVarchar, TypeVarint, TypeBoolean,
	}

	PkTypes = SimpleTypes{
		TypeAscii, TypeBigint, TypeBlob, TypeDate, TypeDecimal, TypeDouble,
		TypeFloat, TypeInet, TypeInt, TypeSmallint, TypeText, TypeTime, TypeTimestamp, TypeTimeuuid,
		TypeTinyint, TypeUuid, TypeVarchar, TypeVarint,
	}
	AllTypes = append(append(SimpleTypes{}, PkTypes...), TypeBoolean, TypeDuration)
)

var goCQLTypeMap = map[gocql.Type]gocql.TypeInfo{
	gocql.TypeAscii:     gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeAscii, ""),
	gocql.TypeBigInt:    gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeBigInt, ""),
	gocql.TypeBlob:      gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeBlob, ""),
	gocql.TypeBoolean:   gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeBoolean, ""),
	gocql.TypeDate:      gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeDate, ""),
	gocql.TypeDecimal:   gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeDecimal, ""),
	gocql.TypeDouble:    gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeDouble, ""),
	gocql.TypeDuration:  gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeDuration, ""),
	gocql.TypeFloat:     gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeFloat, ""),
	gocql.TypeInet:      gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeInet, ""),
	gocql.TypeInt:       gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeInt, ""),
	gocql.TypeSmallInt:  gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeSmallInt, ""),
	gocql.TypeText:      gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeText, ""),
	gocql.TypeTime:      gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeTime, ""),
	gocql.TypeTimestamp: gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeTimestamp, ""),
	gocql.TypeTimeUUID:  gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeTimeUUID, ""),
	gocql.TypeTinyInt:   gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeTinyInt, ""),
	gocql.TypeUUID:      gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeUUID, ""),
	gocql.TypeVarchar:   gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeVarchar, ""),
	gocql.TypeVarint:    gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeVarint, ""),

	// Complex col-types
	gocql.TypeList:  gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeList, ""),
	gocql.TypeMap:   gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeMap, ""),
	gocql.TypeSet:   gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeSet, ""),
	gocql.TypeTuple: gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeTuple, ""),
	gocql.TypeUDT:   gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeUDT, ""),

	// Special col-types
	gocql.TypeCounter: gocql.NewNativeType(GoCQLProtoVersion4, gocql.TypeCounter, ""),
}

type MapType struct {
	ComplexType string     `json:"complex_type"`
	KeyType     SimpleType `json:"key_type"`
	ValueType   SimpleType `json:"value_type"`
	Frozen      bool       `json:"frozen"`
}

func (mt *MapType) CQLType() gocql.TypeInfo {
	return goCQLTypeMap[gocql.TypeMap]
}

func (mt *MapType) Name() string {
	if mt.Frozen {
		return "frozen<map<" + mt.KeyType.Name() + "," + mt.ValueType.Name() + ">>"
	}
	return "map<" + mt.KeyType.Name() + "," + mt.ValueType.Name() + ">"
}

func (mt *MapType) CQLHolder() string {
	return "?"
}

func (mt *MapType) CQLPretty(builder *bytes.Buffer, value any) error {
	if reflect.TypeOf(value).Kind() != reflect.Map {
		return errors.Errorf("expected map, got [%T]%v", value, value)
	}

	builder.WriteRune('{')
	defer builder.WriteRune('}')

	vof := reflect.ValueOf(value)
	s := vof.MapRange()
	length := vof.Len()

	for id := 0; s.Next(); id++ {
		if err := mt.KeyType.CQLPretty(builder, s.Key().Interface()); err != nil {
			return err
		}
		builder.WriteRune(':')

		if err := mt.ValueType.CQLPretty(builder, s.Value().Interface()); err != nil {
			return err
		}

		if id < length-1 {
			builder.WriteRune(',')
		}
	}

	return nil
}

func (mt *MapType) GenJSONValue(r *rand.Rand, p *PartitionRangeConfig) any {
	count := r.IntN(9) + 1
	vals := reflect.MakeMap(
		reflect.MapOf(
			reflect.TypeOf(mt.KeyType.GenJSONValue(r, p)),
			reflect.TypeOf(mt.ValueType.GenJSONValue(r, p)),
		),
	)
	for i := 0; i < count; i++ {
		vals.SetMapIndex(
			reflect.ValueOf(mt.KeyType.GenJSONValue(r, p)),
			reflect.ValueOf(mt.ValueType.GenJSONValue(r, p)),
		)
	}
	return vals.Interface()
}

func (mt *MapType) GenValue(r *rand.Rand, p *PartitionRangeConfig) []any {
	count := utils.RandInt2(r, 1, maxMapSize+1)
	vals := reflect.MakeMap(
		reflect.MapOf(
			reflect.TypeOf(mt.KeyType.GenValue(r, p)[0]),
			reflect.TypeOf(mt.ValueType.GenValue(r, p)[0]),
		),
	)
	for i := 0; i < count; i++ {
		vals.SetMapIndex(
			reflect.ValueOf(mt.KeyType.GenValue(r, p)[0]),
			reflect.ValueOf(mt.ValueType.GenValue(r, p)[0]),
		)
	}
	return []any{vals.Interface()}
}

func (mt *MapType) LenValue() int {
	return 1
}

func (mt *MapType) CQLDef() string {
	if mt.Frozen {
		return "frozen<map<" + mt.KeyType.CQLDef() + "," + mt.ValueType.CQLDef() + ">>"
	}
	return "map<" + mt.KeyType.CQLDef() + "," + mt.ValueType.CQLDef() + ">"
}

func (mt *MapType) Indexable() bool {
	return false
}

// ValueVariationsNumber returns number of bytes generated value holds
func (mt *MapType) ValueVariationsNumber(p *PartitionRangeConfig) float64 {
	return math.Pow(
		mt.KeyType.ValueVariationsNumber(p)*mt.ValueType.ValueVariationsNumber(p),
		maxMapSize,
	)
}

type CounterType struct {
	Value int64
}

func (ct *CounterType) CQLType() gocql.TypeInfo {
	return goCQLTypeMap[gocql.TypeMap]
}

func (ct *CounterType) Name() string {
	return "counter"
}

func (ct *CounterType) CQLHolder() string {
	return "?"
}

func (ct *CounterType) CQLPretty(builder *bytes.Buffer, value any) error {
	switch v := value.(type) {
	case int64:
		builder.WriteString(strconv.FormatInt(v, 10))
	case int:
		builder.WriteString(strconv.FormatInt(int64(v), 10))
	case int32:
		builder.WriteString(strconv.FormatInt(int64(v), 10))
	case uint64:
		builder.WriteString(strconv.FormatUint(v, 10))
	case uint32:
		builder.WriteString(strconv.FormatUint(uint64(v), 10))
	case uint:
		builder.WriteString(strconv.FormatUint(uint64(v), 10))
	default:
		return errors.Errorf("counter cql pretty, unknown type [%T]%v", value, value)
	}

	return nil
}

func (ct *CounterType) GenJSONValue(r *rand.Rand, _ *PartitionRangeConfig) any {
	if utils.IsUnderTest() {
		return r.Int64()
	}
	return atomic.AddInt64(&ct.Value, 1)
}

func (ct *CounterType) GenValue(r *rand.Rand, _ *PartitionRangeConfig) []any {
	if utils.IsUnderTest() {
		return []any{r.Int64()}
	}
	return []any{atomic.AddInt64(&ct.Value, 1)}
}

func (ct *CounterType) LenValue() int {
	return 1
}

func (ct *CounterType) CQLDef() string {
	return "counter"
}

func (ct *CounterType) Indexable() bool {
	return false
}

// ValueVariationsNumber returns number of bytes generated value holds
func (ct *CounterType) ValueVariationsNumber(_ *PartitionRangeConfig) float64 {
	// As a type, counters are a 64-bit signed integer
	return 2 ^ 64
}
