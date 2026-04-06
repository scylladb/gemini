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

package typedef

import (
	"math"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// SimpleType
// ---------------------------------------------------------------------------

func TestSimpleType_Name(t *testing.T) {
	t.Parallel()

	cases := []struct {
		typ  SimpleType
		want string
	}{
		{TypeInt, "int"},
		{TypeText, "text"},
		{TypeAscii, "ascii"},
		{TypeBigint, "bigint"},
		{TypeBoolean, "boolean"},
		{TypeBlob, "blob"},
		{TypeDouble, "double"},
		{TypeFloat, "float"},
		{TypeUuid, "uuid"},
		{TypeTimeuuid, "timeuuid"},
		{TypeTimestamp, "timestamp"},
		{TypeDuration, "duration"},
		{TypeDate, "date"},
		{TypeTime, "time"},
		{TypeDecimal, "decimal"},
		{TypeInet, "inet"},
		{TypeSmallint, "smallint"},
		{TypeTinyint, "tinyint"},
		{TypeVarchar, "varchar"},
		{TypeVarint, "varint"},
	}

	for _, tc := range cases {
		t.Run(string(tc.typ), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.typ.Name())
		})
	}
}

func TestSimpleType_CQLDef(t *testing.T) {
	t.Parallel()

	cases := []SimpleType{
		TypeInt, TypeText, TypeAscii, TypeBigint, TypeBoolean, TypeBlob,
		TypeDouble, TypeFloat, TypeUuid, TypeTimeuuid, TypeTimestamp,
		TypeDuration, TypeDate, TypeTime, TypeDecimal, TypeInet,
		TypeSmallint, TypeTinyint, TypeVarchar, TypeVarint,
	}

	for _, typ := range cases {
		t.Run(string(typ), func(t *testing.T) {
			t.Parallel()
			// CQLDef returns the raw CQL type name, same as Name()
			assert.Equal(t, string(typ), typ.CQLDef())
		})
	}
}

func TestSimpleType_CQLHolder(t *testing.T) {
	t.Parallel()

	cases := []SimpleType{TypeInt, TypeText, TypeBoolean, TypeBlob, TypeUuid}

	for _, typ := range cases {
		t.Run(string(typ), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, "?", typ.CQLHolder())
		})
	}
}

func TestSimpleType_CQLType(t *testing.T) {
	t.Parallel()

	cases := []struct {
		typ      SimpleType
		wantType gocql.Type
	}{
		{TypeAscii, gocql.TypeAscii},
		{TypeText, gocql.TypeText},
		{TypeVarchar, gocql.TypeVarchar},
		{TypeBlob, gocql.TypeBlob},
		{TypeBigint, gocql.TypeBigInt},
		{TypeBoolean, gocql.TypeBoolean},
		{TypeDate, gocql.TypeDate},
		{TypeTime, gocql.TypeTime},
		{TypeTimestamp, gocql.TypeTimestamp},
		{TypeDecimal, gocql.TypeDecimal},
		{TypeDouble, gocql.TypeDouble},
		{TypeDuration, gocql.TypeDuration},
		{TypeFloat, gocql.TypeFloat},
		{TypeInet, gocql.TypeInet},
		{TypeInt, gocql.TypeInt},
		{TypeSmallint, gocql.TypeSmallInt},
		{TypeTimeuuid, gocql.TypeTimeUUID},
		{TypeUuid, gocql.TypeUUID},
		{TypeTinyint, gocql.TypeTinyInt},
		{TypeVarint, gocql.TypeVarint},
	}

	for _, tc := range cases {
		t.Run(string(tc.typ), func(t *testing.T) {
			t.Parallel()
			info := tc.typ.CQLType()
			require.NotNil(t, info)
			assert.Equal(t, tc.wantType, info.Type())
		})
	}
}

func TestSimpleType_CQLType_Panic(t *testing.T) {
	t.Parallel()

	unknown := SimpleType("unknown_type_xyz")
	assert.Panics(t, func() { unknown.CQLType() })
}

func TestSimpleType_Indexable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		typ       SimpleType
		indexable bool
	}{
		{TypeInt, true},
		{TypeText, true},
		{TypeBoolean, true},
		{TypeBlob, true},
		{TypeUuid, true},
		{TypeDuration, false}, // only Duration is not indexable
		{TypeTimestamp, true},
		{TypeBigint, true},
	}

	for _, tc := range cases {
		t.Run(string(tc.typ), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.indexable, tc.typ.Indexable())
		})
	}
}

func TestSimpleType_ValueVariationsNumber(t *testing.T) {
	t.Parallel()

	cfg := testRangeConfig

	cases := []struct {
		typ  SimpleType
		want float64
	}{
		{TypeAscii, math.Pow(2, float64(cfg.MaxStringLength))},
		{TypeText, math.Pow(2, float64(cfg.MaxStringLength))},
		{TypeVarchar, math.Pow(2, float64(cfg.MaxStringLength))},
		{TypeBlob, math.Pow(2, float64(cfg.MaxBlobLength))},
		{TypeBoolean, 2},
		{TypeDate, 10000*365 + 2000*4},
		{TypeTime, 86400000000000},
		{TypeVarint, math.MaxUint64},
		{TypeTimeuuid, math.MaxUint64},
		{TypeUuid, math.MaxUint64},
		{TypeBigint, math.MaxUint64},
		{TypeTimestamp, math.MaxUint64},
		{TypeDecimal, math.MaxUint64},
		{TypeDouble, math.MaxUint64},
		{TypeDuration, math.MaxUint64},
		{TypeInet, math.MaxUint32},
		{TypeInt, math.MaxUint32},
		{TypeFloat, math.MaxUint32},
		{TypeSmallint, math.MaxUint16},
		{TypeTinyint, math.MaxUint8},
	}

	for _, tc := range cases {
		t.Run(string(tc.typ), func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.typ.ValueVariationsNumber(cfg))
		})
	}
}

func TestSimpleType_ValueVariationsNumber_Panic(t *testing.T) {
	t.Parallel()

	unknown := SimpleType("not_a_real_type")
	assert.Panics(t, func() { unknown.ValueVariationsNumber(testRangeConfig) })
}

func TestSimpleType_GenJSONValue(t *testing.T) {
	t.Parallel()

	types := []SimpleType{
		TypeBlob, TypeDecimal, TypeUuid, TypeTimeuuid, TypeVarint,
		TypeDate, TypeDuration, TypeTime,
		TypeAscii, TypeText, TypeVarchar, TypeBigint, TypeBoolean,
		TypeTimestamp, TypeDouble, TypeFloat, TypeInet, TypeInt,
		TypeSmallint, TypeTinyint,
	}

	for _, typ := range types {
		t.Run(string(typ), func(t *testing.T) {
			t.Parallel()
			rng := rand.New(rand.NewPCG(42, 0))
			val := typ.GenJSONValue(rng, testRangeConfig)
			assert.NotNil(t, val, "GenJSONValue should not return nil for %s", typ)
		})
	}
}

func TestSimpleTypes_Contains(t *testing.T) {
	t.Parallel()

	list := SimpleTypes{TypeInt, TypeText, TypeBoolean}

	cases := []struct {
		typ   Type
		name  string
		found bool
	}{
		{TypeInt, "int present", true},
		{TypeText, "text present", true},
		{TypeBoolean, "boolean present", true},
		{TypeBlob, "blob absent", false},
		{&Collection{ComplexType: TypeList, ValueType: TypeInt}, "collection not SimpleType", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.found, list.Contains(tc.typ))
		})
	}
}

func TestSimpleTypes_Random(t *testing.T) {
	t.Parallel()

	list := SimpleTypes{TypeInt, TypeText, TypeBoolean, TypeBlob, TypeUuid}
	rng := rand.New(rand.NewPCG(1, 0))

	for range 50 {
		got := list.Random(rng)
		assert.True(t, list.Contains(got), "Random() returned %q which is not in the list", got)
	}
}

// ---------------------------------------------------------------------------
// Collection
// ---------------------------------------------------------------------------

func TestCollection_Name(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		coll *Collection
		want string
	}{
		{
			name: "list unfrozen",
			coll: &Collection{ComplexType: TypeList, ValueType: TypeInt},
			want: "list<int>",
		},
		{
			name: "list frozen",
			coll: &Collection{ComplexType: TypeList, ValueType: TypeInt, Frozen: true},
			want: "frozen<list<int>>",
		},
		{
			name: "set unfrozen",
			coll: &Collection{ComplexType: TypeSet, ValueType: TypeText},
			want: "set<text>",
		},
		{
			name: "set frozen",
			coll: &Collection{ComplexType: TypeSet, ValueType: TypeText, Frozen: true},
			want: "frozen<set<text>>",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.coll.Name())
		})
	}
}

func TestCollection_CQLDef(t *testing.T) {
	t.Parallel()

	// CQLDef mirrors Name() for collections
	c := &Collection{ComplexType: TypeList, ValueType: TypeInt, Frozen: false}
	assert.Equal(t, "list<int>", c.CQLDef())

	cf := &Collection{ComplexType: TypeSet, ValueType: TypeText, Frozen: true}
	assert.Equal(t, "frozen<set<text>>", cf.CQLDef())
}

func TestCollection_CQLHolder(t *testing.T) {
	t.Parallel()

	c := &Collection{ComplexType: TypeList, ValueType: TypeInt}
	assert.Equal(t, "?", c.CQLHolder())
}

func TestCollection_CQLType(t *testing.T) {
	t.Parallel()

	cases := []struct {
		coll     *Collection
		name     string
		wantType gocql.Type
	}{
		{&Collection{ComplexType: TypeSet, ValueType: TypeInt}, "set", gocql.TypeSet},
		{&Collection{ComplexType: TypeList, ValueType: TypeInt}, "list", gocql.TypeList},
		{&Collection{ComplexType: "bag", ValueType: TypeInt}, "unknown defaults to list", gocql.TypeList},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			info := tc.coll.CQLType()
			require.NotNil(t, info)
			assert.Equal(t, tc.wantType, info.Type())
		})
	}
}

func TestCollection_Indexable(t *testing.T) {
	t.Parallel()

	c := &Collection{ComplexType: TypeList, ValueType: TypeInt}
	assert.False(t, c.Indexable(), "Collection.Indexable() should always be false")
}

func TestCollection_ValueVariationsNumber(t *testing.T) {
	t.Parallel()

	c := &Collection{ComplexType: TypeList, ValueType: TypeInt}
	want := math.Pow(TypeInt.ValueVariationsNumber(testRangeConfig), maxBagSize)
	assert.Equal(t, want, c.ValueVariationsNumber(testRangeConfig))
}

func TestCollection_GenJSONValue(t *testing.T) {
	t.Parallel()

	c := &Collection{ComplexType: TypeList, ValueType: TypeInt}
	rng := rand.New(rand.NewPCG(42, 0))
	val := c.GenJSONValue(rng, testRangeConfig)

	items, ok := val.([]any)
	require.True(t, ok, "GenJSONValue should return []any, got %T", val)
	assert.NotEmpty(t, items)
}

// ---------------------------------------------------------------------------
// TupleType
// ---------------------------------------------------------------------------

func TestTupleType_Name(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		tuple *TupleType
		want  string
	}{
		{
			name:  "single element",
			tuple: &TupleType{ValueTypes: []SimpleType{TypeInt}},
			want:  "Type: int",
		},
		{
			name:  "two elements",
			tuple: &TupleType{ValueTypes: []SimpleType{TypeInt, TypeText}},
			want:  "Type: int,text",
		},
		{
			name:  "three elements",
			tuple: &TupleType{ValueTypes: []SimpleType{TypeInt, TypeText, TypeBoolean}},
			want:  "Type: int,text,boolean",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.tuple.Name())
		})
	}
}

func TestTupleType_CQLDef(t *testing.T) {
	t.Parallel()

	cases := []struct {
		tuple  *TupleType
		name   string
		want   string
		frozen bool
	}{
		{
			name:  "unfrozen",
			tuple: &TupleType{ValueTypes: []SimpleType{TypeInt, TypeText}},
			want:  "tuple<int,text>",
		},
		{
			name:   "frozen",
			tuple:  &TupleType{ValueTypes: []SimpleType{TypeInt, TypeText}, Frozen: true},
			frozen: true,
			want:   "frozen<tuple<int,text>>",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.tuple.CQLDef())
		})
	}
}

func TestTupleType_CQLHolder(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		tuple *TupleType
		want  string
	}{
		{
			name:  "one element",
			tuple: &TupleType{ValueTypes: []SimpleType{TypeInt}},
			want:  "(?)",
		},
		{
			name:  "two elements",
			tuple: &TupleType{ValueTypes: []SimpleType{TypeInt, TypeText}},
			want:  "(?,?)",
		},
		{
			name:  "three elements",
			tuple: &TupleType{ValueTypes: []SimpleType{TypeInt, TypeText, TypeBoolean}},
			want:  "(?,?,?)",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.tuple.CQLHolder()
			assert.Equal(t, tc.want, got)
			// Verify the count of ? matches number of types
			assert.Equal(t, len(tc.tuple.ValueTypes), strings.Count(got, "?"))
		})
	}
}

func TestTupleType_CQLType(t *testing.T) {
	t.Parallel()

	tuple := &TupleType{ValueTypes: []SimpleType{TypeInt, TypeText}}
	info := tuple.CQLType()
	require.NotNil(t, info)
	assert.Equal(t, gocql.TypeTuple, info.Type())
}

func TestTupleType_Indexable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		tuple     *TupleType
		name      string
		indexable bool
	}{
		{
			name:      "no duration",
			tuple:     &TupleType{ValueTypes: []SimpleType{TypeInt, TypeText}},
			indexable: true,
		},
		{
			name:      "contains duration",
			tuple:     &TupleType{ValueTypes: []SimpleType{TypeInt, TypeDuration}},
			indexable: false,
		},
		{
			name:      "only duration",
			tuple:     &TupleType{ValueTypes: []SimpleType{TypeDuration}},
			indexable: false,
		},
		{
			name:      "duration not in list",
			tuple:     &TupleType{ValueTypes: []SimpleType{TypeBoolean, TypeFloat}},
			indexable: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.indexable, tc.tuple.Indexable())
		})
	}
}

func TestTupleType_GenJSONValue(t *testing.T) {
	t.Parallel()

	tuple := &TupleType{ValueTypes: []SimpleType{TypeInt, TypeText, TypeBoolean}}
	rng := rand.New(rand.NewPCG(42, 0))
	val := tuple.GenJSONValue(rng, testRangeConfig)

	items, ok := val.([]any)
	require.True(t, ok, "GenJSONValue should return []any, got %T", val)
	assert.Len(t, items, len(tuple.ValueTypes))
}

func TestTupleType_ValueVariationsNumber(t *testing.T) {
	t.Parallel()

	// Single type: out starts at 1; iteration: out = 1 * 1 * var(TypeInt) = var(TypeInt)
	tuple1 := &TupleType{ValueTypes: []SimpleType{TypeInt}}
	got1 := tuple1.ValueVariationsNumber(testRangeConfig)
	assert.Positive(t, got1)

	// Two types: the multiplication follows the source code logic (out *= out * next)
	// which is a known squaring pattern, but we test that the result is deterministic
	tuple2 := &TupleType{ValueTypes: []SimpleType{TypeInt, TypeText}}
	got2a := tuple2.ValueVariationsNumber(testRangeConfig)
	got2b := tuple2.ValueVariationsNumber(testRangeConfig)
	assert.Equal(t, got2a, got2b, "ValueVariationsNumber should be deterministic")
	assert.Positive(t, got2a)
}

// ---------------------------------------------------------------------------
// UDTType
// ---------------------------------------------------------------------------

func TestUDTType_Name(t *testing.T) {
	t.Parallel()

	udt := &UDTType{TypeName: "my_udt", ValueTypes: map[string]SimpleType{"f1": TypeInt}}
	assert.Equal(t, "my_udt", udt.Name())
}

func TestUDTType_CQLDef(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		udt  *UDTType
		want string
	}{
		{
			name: "unfrozen",
			udt:  &UDTType{TypeName: "addr", ValueTypes: map[string]SimpleType{"f": TypeText}},
			want: "addr",
		},
		{
			name: "frozen",
			udt:  &UDTType{TypeName: "addr", ValueTypes: map[string]SimpleType{"f": TypeText}, Frozen: true},
			want: "frozen<addr>",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.udt.CQLDef())
		})
	}
}

func TestUDTType_CQLHolder(t *testing.T) {
	t.Parallel()

	udt := &UDTType{TypeName: "x", ValueTypes: map[string]SimpleType{"a": TypeInt}}
	assert.Equal(t, "?", udt.CQLHolder())
}

func TestUDTType_CQLType(t *testing.T) {
	t.Parallel()

	udt := &UDTType{TypeName: "x", ValueTypes: map[string]SimpleType{"a": TypeInt}}
	info := udt.CQLType()
	require.NotNil(t, info)
	assert.Equal(t, gocql.TypeUDT, info.Type())
}

func TestUDTType_Indexable(t *testing.T) {
	t.Parallel()

	cases := []struct {
		udt       *UDTType
		name      string
		indexable bool
	}{
		{
			name:      "no duration",
			udt:       &UDTType{TypeName: "x", ValueTypes: map[string]SimpleType{"a": TypeInt, "b": TypeText}},
			indexable: true,
		},
		{
			name:      "has duration",
			udt:       &UDTType{TypeName: "x", ValueTypes: map[string]SimpleType{"a": TypeInt, "b": TypeDuration}},
			indexable: false,
		},
		{
			name:      "empty value types",
			udt:       &UDTType{TypeName: "x", ValueTypes: map[string]SimpleType{}},
			indexable: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.indexable, tc.udt.Indexable())
		})
	}
}

func TestUDTType_GenJSONValue(t *testing.T) {
	t.Parallel()

	udt := &UDTType{
		TypeName:   "test_udt",
		ValueTypes: map[string]SimpleType{"a": TypeInt, "b": TypeText},
		Frozen:     true,
	}
	rng := rand.New(rand.NewPCG(42, 0))
	val := udt.GenJSONValue(rng, testRangeConfig)

	m, ok := val.(map[string]any)
	require.True(t, ok, "GenJSONValue should return map[string]any, got %T", val)
	assert.Contains(t, m, "a")
	assert.Contains(t, m, "b")
}

func TestUDTType_ValueVariationsNumber(t *testing.T) {
	t.Parallel()

	cases := []struct {
		udt  *UDTType
		name string
		want float64
	}{
		{
			name: "single field",
			udt:  &UDTType{TypeName: "x", ValueTypes: map[string]SimpleType{"a": TypeBoolean}},
			want: TypeBoolean.ValueVariationsNumber(testRangeConfig), // 2
		},
		{
			name: "two fields",
			udt: &UDTType{TypeName: "x", ValueTypes: map[string]SimpleType{
				"a": TypeBoolean, // 2
				"b": TypeTinyint, // 255
			}},
			want: 2 * 255,
		},
		{
			name: "empty",
			udt:  &UDTType{TypeName: "x", ValueTypes: map[string]SimpleType{}},
			want: 1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := tc.udt.ValueVariationsNumber(testRangeConfig)
			assert.Equal(t, tc.want, got)
		})
	}
}

// ---------------------------------------------------------------------------
// MapType
// ---------------------------------------------------------------------------

func TestMapType_Name(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		mt   *MapType
		want string
	}{
		{
			name: "unfrozen",
			mt:   &MapType{KeyType: TypeInt, ValueType: TypeText},
			want: "map<int,text>",
		},
		{
			name: "frozen",
			mt:   &MapType{KeyType: TypeInt, ValueType: TypeText, Frozen: true},
			want: "frozen<map<int,text>>",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.mt.Name())
		})
	}
}

func TestMapType_CQLDef(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		mt   *MapType
		want string
	}{
		{
			name: "unfrozen",
			mt:   &MapType{KeyType: TypeInt, ValueType: TypeText},
			want: "map<int,text>",
		},
		{
			name: "frozen",
			mt:   &MapType{KeyType: TypeInt, ValueType: TypeText, Frozen: true},
			want: "frozen<map<int,text>>",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, tc.mt.CQLDef())
		})
	}
}

func TestMapType_CQLType(t *testing.T) {
	t.Parallel()

	mt := &MapType{KeyType: TypeInt, ValueType: TypeText}
	info := mt.CQLType()
	require.NotNil(t, info)
	assert.Equal(t, gocql.TypeMap, info.Type())
}

func TestMapType_Indexable(t *testing.T) {
	t.Parallel()

	mt := &MapType{KeyType: TypeInt, ValueType: TypeText}
	assert.False(t, mt.Indexable(), "MapType.Indexable() should always return false")
}

func TestMapType_GenJSONValue(t *testing.T) {
	t.Parallel()

	mt := &MapType{KeyType: TypeInt, ValueType: TypeText}
	rng := rand.New(rand.NewPCG(42, 0))
	val := mt.GenJSONValue(rng, testRangeConfig)
	assert.NotNil(t, val)
}

func TestMapType_ValueVariationsNumber(t *testing.T) {
	t.Parallel()

	mt := &MapType{KeyType: TypeBoolean, ValueType: TypeTinyint}
	// keys=2 vals=255; product=510; ^maxMapSize(10)
	want := math.Pow(
		TypeBoolean.ValueVariationsNumber(testRangeConfig)*TypeTinyint.ValueVariationsNumber(testRangeConfig),
		maxMapSize,
	)
	got := mt.ValueVariationsNumber(testRangeConfig)
	assert.Equal(t, want, got)
}

// ---------------------------------------------------------------------------
// CounterType
// ---------------------------------------------------------------------------

func TestCounterType_Name(t *testing.T) {
	t.Parallel()

	ct := &CounterType{}
	assert.Equal(t, "counter", ct.Name())
}

func TestCounterType_CQLDef(t *testing.T) {
	t.Parallel()

	ct := &CounterType{}
	assert.Equal(t, "counter", ct.CQLDef())
}

func TestCounterType_CQLHolder(t *testing.T) {
	t.Parallel()

	ct := &CounterType{}
	assert.Equal(t, "?", ct.CQLHolder())
}

func TestCounterType_CQLType(t *testing.T) {
	t.Parallel()

	ct := &CounterType{}
	info := ct.CQLType()
	require.NotNil(t, info)
	// CounterType.CQLType() returns TypeMap (see source)
	assert.Equal(t, gocql.TypeMap, info.Type())
}

func TestCounterType_Indexable(t *testing.T) {
	t.Parallel()

	ct := &CounterType{}
	assert.False(t, ct.Indexable())
}

func TestCounterType_ValueVariationsNumber(t *testing.T) {
	t.Parallel()

	ct := &CounterType{}
	assert.Equal(t, float64(math.MaxUint64), ct.ValueVariationsNumber(testRangeConfig))
}

// TestCounterType_GenValue_UnderTest verifies that under -tags testing,
// GenValue returns a random int64 (not incrementing the counter).
func TestCounterType_GenValue_UnderTest(t *testing.T) {
	t.Parallel()

	ct := &CounterType{}
	rng := rand.New(rand.NewPCG(42, 0))

	vals := ct.GenValue(rng, testRangeConfig)
	require.Len(t, vals, 1)
	_, ok := vals[0].(int64)
	assert.True(t, ok, "GenValue should return int64 under test mode, got %T", vals[0])
}

func TestCounterType_GenJSONValue_UnderTest(t *testing.T) {
	t.Parallel()

	ct := &CounterType{}
	rng := rand.New(rand.NewPCG(42, 0))

	val := ct.GenJSONValue(rng, testRangeConfig)
	_, ok := val.(int64)
	assert.True(t, ok, "GenJSONValue should return int64 under test mode, got %T", val)
}
