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
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"

	"github.com/gocql/gocql"
	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

// nolint:revive
const (
	TYPE_ASCII     = SimpleType("ascii")
	TYPE_BIGINT    = SimpleType("bigint")
	TYPE_BLOB      = SimpleType("blob")
	TYPE_BOOLEAN   = SimpleType("boolean")
	TYPE_DATE      = SimpleType("date")
	TYPE_DECIMAL   = SimpleType("decimal")
	TYPE_DOUBLE    = SimpleType("double")
	TYPE_DURATION  = SimpleType("duration")
	TYPE_FLOAT     = SimpleType("float")
	TYPE_INET      = SimpleType("inet")
	TYPE_INT       = SimpleType("int")
	TYPE_SMALLINT  = SimpleType("smallint")
	TYPE_TEXT      = SimpleType("text")
	TYPE_TIME      = SimpleType("time")
	TYPE_TIMESTAMP = SimpleType("timestamp")
	TYPE_TIMEUUID  = SimpleType("timeuuid")
	TYPE_TINYINT   = SimpleType("tinyint")
	TYPE_UUID      = SimpleType("uuid")
	TYPE_VARCHAR   = SimpleType("varchar")
	TYPE_VARINT    = SimpleType("varint")
)

// TODO: Add support for time when gocql bug is fixed.
var (
	TypesMapKeyBlacklist = map[SimpleType]struct{}{
		TYPE_BLOB:     {},
		TYPE_DURATION: {},
	}
	TypesForIndex     = SimpleTypes{TYPE_DECIMAL, TYPE_DOUBLE, TYPE_FLOAT, TYPE_INT, TYPE_SMALLINT, TYPE_TINYINT, TYPE_VARINT}
	PartitionKeyTypes = SimpleTypes{TYPE_INT, TYPE_SMALLINT, TYPE_TINYINT, TYPE_VARINT}
	PkTypes           = SimpleTypes{
		TYPE_ASCII, TYPE_BIGINT, TYPE_BLOB, TYPE_DATE, TYPE_DECIMAL, TYPE_DOUBLE,
		TYPE_FLOAT, TYPE_INET, TYPE_INT, TYPE_SMALLINT, TYPE_TEXT /*TYPE_TIME,*/, TYPE_TIMESTAMP, TYPE_TIMEUUID,
		TYPE_TINYINT, TYPE_UUID, TYPE_VARCHAR, TYPE_VARINT,
	}
	AllTypes = append(append(SimpleTypes{}, PkTypes...), TYPE_BOOLEAN, TYPE_DURATION)
)

var goCQLTypeMap = map[gocql.Type]gocql.TypeInfo{
	gocql.TypeAscii:     gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeAscii, ""),
	gocql.TypeBigInt:    gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeBigInt, ""),
	gocql.TypeBlob:      gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeBlob, ""),
	gocql.TypeBoolean:   gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeBoolean, ""),
	gocql.TypeDate:      gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeDate, ""),
	gocql.TypeDecimal:   gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeDecimal, ""),
	gocql.TypeDouble:    gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeDouble, ""),
	gocql.TypeDuration:  gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeDuration, ""),
	gocql.TypeFloat:     gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeFloat, ""),
	gocql.TypeInet:      gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeInet, ""),
	gocql.TypeInt:       gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeInt, ""),
	gocql.TypeSmallInt:  gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeSmallInt, ""),
	gocql.TypeText:      gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeText, ""),
	gocql.TypeTime:      gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeTime, ""),
	gocql.TypeTimestamp: gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeTimestamp, ""),
	gocql.TypeTimeUUID:  gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeTimeUUID, ""),
	gocql.TypeTinyInt:   gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeTinyInt, ""),
	gocql.TypeUUID:      gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeUUID, ""),
	gocql.TypeVarchar:   gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeVarchar, ""),
	gocql.TypeVarint:    gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeVarint, ""),

	// Complex coltypes
	gocql.TypeList:  gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeList, ""),
	gocql.TypeMap:   gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeMap, ""),
	gocql.TypeSet:   gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeSet, ""),
	gocql.TypeTuple: gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeTuple, ""),
	gocql.TypeUDT:   gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeUDT, ""),

	// Special coltypes
	gocql.TypeCounter: gocql.NewNativeType(typedef.GoCQLProtoVersion4, gocql.TypeCounter, ""),
}

type MapType struct {
	KeyType   SimpleType `json:"key_type"`
	ValueType SimpleType `json:"value_type"`
	Frozen    bool       `json:"frozen"`
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

func (mt *MapType) CQLPretty(query string, value []interface{}) (string, int) {
	if reflect.TypeOf(value[0]).Kind() != reflect.Map {
		panic(fmt.Sprintf("map cql pretty, unknown type %v", mt))
	}
	s := reflect.ValueOf(value[0]).MapRange()
	vv := "{"
	for s.Next() {
		vv += fmt.Sprintf("%v:?,", s.Key().Interface())
		vv, _ = mt.ValueType.CQLPretty(vv, []interface{}{s.Value().Interface()})
	}
	vv = strings.TrimSuffix(vv, ",")
	vv += "}"
	return strings.Replace(query, "?", vv, 1), 1
}

func (mt *MapType) GenValue(r *rand.Rand, p *typedef.PartitionRangeConfig) []interface{} {
	count := r.Intn(9) + 1
	vals := make(map[interface{}]interface{})
	for i := 0; i < count; i++ {
		vals[mt.KeyType.GenValue(r, p)[0]] = mt.ValueType.GenValue(r, p)[0]
	}
	return []interface{}{vals}
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

func (ct *CounterType) CQLPretty(query string, value []interface{}) (string, int) {
	return strings.Replace(query, "?", fmt.Sprintf("%d", value[0]), 1), 1
}

func (ct *CounterType) GenValue(r *rand.Rand, _ *typedef.PartitionRangeConfig) []interface{} {
	if utils.UnderTest {
		return []interface{}{r.Int63()}
	}
	return []interface{}{atomic.AddInt64(&ct.Value, 1)}
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
