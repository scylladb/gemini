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
	"math/rand/v2"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testRangeConfig = ValueRangeConfig{
	MaxBlobLength:   10,
	MinBlobLength:   1,
	MaxStringLength: 10,
	MinStringLength: 1,
}

// TestGenValueOut_BindCount_MatchesGenValue verifies that GenValueOut produces
// exactly the same number of bind values as GenValue for every Type.
// This is critical: CQL queries have a fixed number of ? placeholders, and
// if GenValueOut appends more or fewer values, gocql returns
// "expected N values send got M".
func TestGenValueOut_BindCount_MatchesGenValue(t *testing.T) {
	t.Parallel()

	types := []struct {
		typ  Type
		name string
	}{
		{TypeInt, "SimpleType_Int"},
		{TypeBigint, "SimpleType_Bigint"},
		{TypeText, "SimpleType_Text"},
		{TypeBoolean, "SimpleType_Boolean"},
		{TypeFloat, "SimpleType_Float"},
		{TypeDouble, "SimpleType_Double"},
		{TypeBlob, "SimpleType_Blob"},
		{TypeUuid, "SimpleType_Uuid"},
		{TypeTimestamp, "SimpleType_Timestamp"},
		{TypeTinyint, "SimpleType_Tinyint"},
		{TypeSmallint, "SimpleType_Smallint"},
		{&Collection{ComplexType: TypeList, ValueType: TypeInt}, "List_Int"},
		{&Collection{ComplexType: TypeList, ValueType: TypeText}, "List_Text"},
		{&Collection{ComplexType: TypeList, ValueType: TypeUuid}, "List_Uuid"},
		{&Collection{ComplexType: TypeSet, ValueType: TypeInt}, "Set_Int"},
		{&Collection{ComplexType: TypeSet, ValueType: TypeText}, "Set_Text"},
		{&TupleType{ValueTypes: []SimpleType{TypeInt, TypeText}}, "Tuple_Int_Text"},
		{&TupleType{ValueTypes: []SimpleType{TypeInt}}, "Tuple_Single"},
		{&TupleType{ValueTypes: []SimpleType{TypeInt, TypeText, TypeBoolean}}, "Tuple_Triple"},
		{&UDTType{ValueTypes: map[string]SimpleType{"a": TypeInt, "b": TypeText}, TypeName: "test_udt", Frozen: true}, "UDT"},
		{&MapType{KeyType: TypeInt, ValueType: TypeText, Frozen: true}, "Map_Int_Text"},
	}

	for _, tt := range types {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Run multiple times since collection sizes are random
			for i := range 20 {
				// Use same seed for both calls so random sizes match
				rng1 := rand.New(rand.NewPCG(uint64(i+1), 0))
				rng2 := rand.New(rand.NewPCG(uint64(i+1), 0))

				genValue := tt.typ.GenValue(rng1, testRangeConfig)
				genValueLen := len(genValue)

				existing := []any{"existing_sentinel"}
				result := tt.typ.GenValueOut(existing, rng2, testRangeConfig)

				// GenValueOut should have appended exactly genValueLen elements
				added := len(result) - 1 // subtract the existing sentinel
				assert.Equalf(t, genValueLen, added,
					"iteration %d: GenValue produced %d values but GenValueOut appended %d (total %d, had 1 existing)",
					i, genValueLen, added, len(result))

				// First element should be the sentinel
				assert.Equal(t, "existing_sentinel", result[0],
					"GenValueOut must not modify existing elements")
			}
		})
	}
}

// TestGenValueOut_Collection_SingleBindValue verifies that Collection types
// (list, set) produce exactly ONE bind value — the collection as a whole,
// not one per element.
func TestGenValueOut_Collection_SingleBindValue(t *testing.T) {
	t.Parallel()

	types := []struct {
		coll *Collection
		name string
	}{
		{&Collection{ComplexType: TypeList, ValueType: TypeInt}, "List_Int"},
		{&Collection{ComplexType: TypeList, ValueType: TypeText}, "List_Text"},
		{&Collection{ComplexType: TypeSet, ValueType: TypeInt}, "Set_Int"},
		{&Collection{ComplexType: TypeSet, ValueType: TypeBigint}, "Set_Bigint"},
	}

	for _, tt := range types {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			for i := range 20 {
				rng := rand.New(rand.NewPCG(uint64(i+1), 0))

				var out []any
				out = tt.coll.GenValueOut(out, rng, testRangeConfig)

				require.Len(t, out, 1,
					"Collection.GenValueOut must produce exactly 1 bind value, got %d", len(out))

				// The single value should be a []any (the collection items)
				items, ok := out[0].([]any)
				require.True(t, ok,
					"Collection bind value must be []any, got %T", out[0])
				assert.NotEmpty(t, items,
					"Collection must have at least 1 element")
			}
		})
	}
}

// TestGenValueOut_Collection_MatchesGenValue verifies that the actual
// collection content from GenValueOut matches GenValue when using the
// same random seed.
func TestGenValueOut_Collection_MatchesGenValue(t *testing.T) {
	t.Parallel()

	coll := &Collection{ComplexType: TypeList, ValueType: TypeInt}

	for i := range 20 {
		rng1 := rand.New(rand.NewPCG(uint64(i+1), 0))
		rng2 := rand.New(rand.NewPCG(uint64(i+1), 0))

		genValue := coll.GenValue(rng1, testRangeConfig)
		require.Len(t, genValue, 1)

		var out []any
		out = coll.GenValueOut(out, rng2, testRangeConfig)
		require.Len(t, out, 1)

		expected := genValue[0].([]any)
		actual := out[0].([]any)

		assert.Equal(t, len(expected), len(actual),
			"iteration %d: GenValue collection has %d items, GenValueOut has %d",
			i, len(expected), len(actual))

		for j := range expected {
			assert.Equal(t, expected[j], actual[j],
				"iteration %d: item %d differs", i, j)
		}
	}
}

// TestGenValueOut_UDT_SingleBindValue verifies UDT produces exactly one bind value.
func TestGenValueOut_UDT_SingleBindValue(t *testing.T) {
	t.Parallel()

	udt := &UDTType{
		ValueTypes: map[string]SimpleType{"f1": TypeInt, "f2": TypeText},
		TypeName:   "test_udt",
		Frozen:     true,
	}

	rng := rand.New(rand.NewPCG(1, 0))
	var out []any
	out = udt.GenValueOut(out, rng, testRangeConfig)

	require.Len(t, out, 1, "UDT.GenValueOut must produce exactly 1 bind value")

	m, ok := out[0].(map[string]any)
	require.True(t, ok, "UDT bind value must be map[string]any, got %T", out[0])
	assert.Contains(t, m, "f1")
	assert.Contains(t, m, "f2")
}

// TestGenValueOut_Map_SingleBindValue verifies Map produces exactly one bind value.
func TestGenValueOut_Map_SingleBindValue(t *testing.T) {
	t.Parallel()

	mt := &MapType{KeyType: TypeInt, ValueType: TypeText, Frozen: true}

	rng := rand.New(rand.NewPCG(1, 0))
	var out []any
	out = mt.GenValueOut(out, rng, testRangeConfig)

	require.Len(t, out, 1, "MapType.GenValueOut must produce exactly 1 bind value")
}

// TestGenValueOut_Tuple_MultipleBindValues verifies Tuple produces one bind
// value per tuple field (matching TupleColumn placeholder count).
func TestGenValueOut_Tuple_MultipleBindValues(t *testing.T) {
	t.Parallel()

	tuple := &TupleType{ValueTypes: []SimpleType{TypeInt, TypeText, TypeBoolean}}

	rng := rand.New(rand.NewPCG(1, 0))
	var out []any
	out = tuple.GenValueOut(out, rng, testRangeConfig)

	assert.Len(t, out, 3, "Tuple with 3 fields must produce 3 bind values")
}

// TestGenValueOut_LenValue_Consistency verifies that LenValue() matches
// the actual number of values produced by GenValueOut for every Type.
func TestGenValueOut_LenValue_Consistency(t *testing.T) {
	t.Parallel()

	types := []struct {
		typ  Type
		name string
	}{
		{TypeInt, "SimpleType_Int"},
		{TypeText, "SimpleType_Text"},
		{&Collection{ComplexType: TypeList, ValueType: TypeInt}, "List_Int"},
		{&Collection{ComplexType: TypeSet, ValueType: TypeText}, "Set_Text"},
		{&TupleType{ValueTypes: []SimpleType{TypeInt, TypeText}}, "Tuple_2"},
		{&TupleType{ValueTypes: []SimpleType{TypeInt, TypeText, TypeBoolean, TypeFloat}}, "Tuple_4"},
		{&UDTType{ValueTypes: map[string]SimpleType{"a": TypeInt}, TypeName: "t", Frozen: true}, "UDT"},
		{&MapType{KeyType: TypeInt, ValueType: TypeText, Frozen: true}, "Map"},
	}

	for _, tt := range types {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rng := rand.New(rand.NewPCG(42, 0))

			var out []any
			out = tt.typ.GenValueOut(out, rng, testRangeConfig)

			assert.Equal(t, tt.typ.LenValue(), len(out),
				"LenValue() = %d but GenValueOut produced %d values",
				tt.typ.LenValue(), len(out))
		})
	}
}

// TestGenValueOut_SimulateInsert simulates what Insert() does: builds a full
// values slice from partition keys, clustering keys, and columns, then
// verifies the total matches TotalLenValues.
func TestGenValueOut_SimulateInsert(t *testing.T) {
	t.Parallel()

	table := &Table{
		PartitionKeys: Columns{
			{Name: "pk0", Type: TypeInt},
		},
		ClusteringKeys: Columns{
			{Name: "ck0", Type: TypeBigint},
			{Name: "ck1", Type: TypeText},
		},
		Columns: Columns{
			{Name: "v0", Type: TypeText},
			{Name: "v1", Type: TypeInt},
			{Name: "v2", Type: &Collection{ComplexType: TypeList, ValueType: TypeInt}},
			{Name: "v3", Type: &TupleType{ValueTypes: []SimpleType{TypeInt, TypeText}}},
		},
	}
	table.Init(nil)

	rng := rand.New(rand.NewPCG(42, 0))

	values := make([]any, 0, table.TotalLenValues)

	// Simulate partition keys (normally from partition generator)
	values = append(values, int32(1)) // pk0

	// Simulate clustering keys
	for _, ck := range table.ClusteringKeys {
		values = ck.Type.GenValueOut(values, rng, testRangeConfig)
	}

	// Simulate columns
	for _, col := range table.Columns {
		values = col.Type.GenValueOut(values, rng, testRangeConfig)
	}

	assert.Equal(t, table.TotalLenValues, len(values),
		"TotalLenValues=%d but actual values produced=%d (would cause gocql bind mismatch)",
		table.TotalLenValues, len(values))
}
