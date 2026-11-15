// Copyright 2026 ScyllaDB
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
	"reflect"
	"testing"
)

var genTestRange = &ValueRangeConfig{
	MaxBlobLength:   16,
	MinBlobLength:   1,
	MaxStringLength: 16,
	MinStringLength: 1,
}

// TestSimpleTypeGoTypeMatchesGeneratedValue: MapType builds its map from
// goType and jsonGoType. A type that differs from the generated value makes
// reflect panic in SetMapIndex.
func TestSimpleTypeGoTypeMatchesGeneratedValue(t *testing.T) {
	t.Parallel()

	r := rand.New(rand.NewPCG(1, 2))
	for _, st := range AllTypes {
		if got := reflect.TypeOf(st.genValue(r, genTestRange)); got != st.goType() {
			t.Errorf("%s: genValue makes %v, goType is %v", st, got, st.goType())
		}
		if got := reflect.TypeOf(st.GenJSONValue(r, genTestRange)); got != st.jsonGoType() {
			t.Errorf("%s: GenJSONValue makes %v, jsonGoType is %v", st, got, st.jsonGoType())
		}
	}
}

// TestMapGenValueEveryKeyAndValueType generates a map for every allowed key
// and value type pair, through both value paths.
func TestMapGenValueEveryKeyAndValueType(t *testing.T) {
	t.Parallel()

	r := rand.New(rand.NewPCG(3, 4))
	for _, key := range AllTypes {
		if _, banned := TypesMapKeyBlacklist[key]; banned {
			continue
		}
		for _, value := range AllTypes {
			mt := &MapType{ComplexType: TypeMap, KeyType: key, ValueType: value}

			got := reflect.ValueOf(mt.GenValue(r, genTestRange)[0])
			if got.Type() != reflect.MapOf(key.goType(), value.goType()) {
				t.Fatalf("map<%s,%s>: GenValue made %v", key, value, got.Type())
			}
			if got.Len() < 1 || got.Len() > maxMapSize {
				t.Fatalf("map<%s,%s>: GenValue made %d entries", key, value, got.Len())
			}

			out := mt.GenValueOut(nil, r, genTestRange)
			if len(out) != 1 || reflect.TypeOf(out[0]) != got.Type() {
				t.Fatalf("map<%s,%s>: GenValueOut made %#v", key, value, out)
			}

			// A JSON object key is a string, see MapType.GenJSONValue.
			if js := reflect.TypeOf(mt.GenJSONValue(r, genTestRange)); js != reflect.TypeFor[map[string]any]() {
				t.Fatalf("map<%s,%s>: GenJSONValue made %v", key, value, js)
			}
		}
	}
}

func newWideUDT() *UDTType {
	fields := make(map[string]SimpleType, len(AllTypes))
	for i, st := range AllTypes {
		fields[string(rune('a'+i))+"_field"] = st
	}

	return &UDTType{ComplexType: TypeUdt, ValueTypes: fields, TypeName: "udt_1", Frozen: true}
}

// TestUDTSameSeedGivesSameValues: the UDT fields are generated in sorted
// order, so one seed gives the same values on a new UDTType too. Go map
// iteration order changes between ranges, so a new instance per draw finds
// an order dependence.
func TestUDTSameSeedGivesSameValues(t *testing.T) {
	t.Parallel()

	for seed := range uint64(10) {
		want := newWideUDT().GenValue(rand.New(rand.NewPCG(seed, 5)), genTestRange)
		wantJSON := newWideUDT().GenJSONValue(rand.New(rand.NewPCG(seed, 5)), genTestRange)
		for range 10 {
			if got := newWideUDT().GenValue(rand.New(rand.NewPCG(seed, 5)), genTestRange); !reflect.DeepEqual(want, got) {
				t.Fatalf("seed %d: GenValue gave %#v, then %#v", seed, want, got)
			}
			out := newWideUDT().GenValueOut(nil, rand.New(rand.NewPCG(seed, 5)), genTestRange)
			if !reflect.DeepEqual(want, out) {
				t.Fatalf("seed %d: GenValueOut gave %#v, GenValue gave %#v", seed, out, want)
			}
			if got := newWideUDT().GenJSONValue(rand.New(rand.NewPCG(seed, 5)), genTestRange); !reflect.DeepEqual(wantJSON, got) {
				t.Fatalf("seed %d: GenJSONValue gave %#v, then %#v", seed, wantJSON, got)
			}
		}
	}
}

func TestUDTFieldNamesAreSortedAfterJSONRead(t *testing.T) {
	t.Parallel()

	col, err := GetUDTTypeColumn(map[string]any{
		"name": "u",
		"type": map[string]any{
			"complex_type": TypeUdt,
			"type_name":    "udt_9",
			"frozen":       true,
			"value_types": map[string]any{
				"zeta": "int", "alpha": "text", "mid": "bigint", "beta": "uuid",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	udt, ok := col.Type.(*UDTType)
	if !ok {
		t.Fatalf("column type is %T, want *UDTType", col.Type)
	}

	want := []string{"alpha", "beta", "mid", "zeta"}
	for range 10 {
		if got := udt.FieldNames(); !reflect.DeepEqual(want, got) {
			t.Fatalf("FieldNames() = %v, want %v", got, want)
		}
	}
}

func TestTupleValueVariationsNumberIsProduct(t *testing.T) {
	t.Parallel()

	tuple := &TupleType{
		ComplexType: TypeTuple,
		ValueTypes:  []SimpleType{TypeBoolean, TypeBoolean, TypeTinyint},
	}

	want := TypeBoolean.ValueVariationsNumber(genTestRange) *
		TypeBoolean.ValueVariationsNumber(genTestRange) *
		TypeTinyint.ValueVariationsNumber(genTestRange)
	if got := tuple.ValueVariationsNumber(genTestRange); got != want {
		t.Fatalf("ValueVariationsNumber() = %v, want %v", got, want)
	}
}

// TestNewPartitionValuesSharesNames: every Values built from one column
// layout shares one name slice, and a Merge into one Values leaves the others
// unchanged.
func TestNewPartitionValuesSharesNames(t *testing.T) {
	t.Parallel()

	cols := Columns{{Name: "pk1", Type: TypeInt}, {Name: "pk2", Type: TypeText}}
	first := NewPartitionValues(cols, []any{int32(1), "a"})
	second := NewPartitionValues(cols, []any{int32(2), "b"})

	if &first.names[0] != &second.names[0] {
		t.Fatal("two Values of one layout have separate name slices")
	}

	first.Merge(NewValuesFromMap(map[string][]any{"extra": {1}}))
	if second.Len() != 2 || second.Get("extra") != nil {
		t.Fatalf("Merge into one Values changed another: %v", second.ToMap())
	}
	if got := second.Get("pk2"); !reflect.DeepEqual(got, []any{"b"}) {
		t.Fatalf("second.Get(pk2) = %v", got)
	}
}

func TestNewPartitionValuesFollowsChangedColumns(t *testing.T) {
	t.Parallel()

	cols := Columns{{Name: "pk1", Type: TypeInt}, {Name: "pk2", Type: TypeInt}}
	before := NewPartitionValues(cols, []any{int32(1), int32(2)})

	cols[1].Name = "renamed"
	after := NewPartitionValues(cols, []any{int32(1), int32(2)})

	if after.Get("renamed") == nil || after.Get("pk2") != nil {
		t.Fatalf("Values after a column change = %v", after.ToMap())
	}
	if before.Get("pk2") == nil {
		t.Fatalf("Values before a column change = %v", before.ToMap())
	}
}

func TestCounterColumnJSONRoundTrip(t *testing.T) {
	t.Parallel()

	var col ColumnDef
	if err := col.UnmarshalJSON([]byte(`{"name":"c0","type":"counter"}`)); err != nil {
		t.Fatal(err)
	}
	if _, ok := col.Type.(*CounterType); !ok {
		t.Fatalf("column type is %T, want *CounterType", col.Type)
	}

	data, err := col.MarshalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != `{"type":"counter","name":"c0"}` {
		t.Fatalf("MarshalJSON() = %s", data)
	}

	var again ColumnDef
	if err = again.UnmarshalJSON(data); err != nil {
		t.Fatal(err)
	}
	if _, ok := again.Type.(*CounterType); !ok {
		t.Fatalf("column type after a round trip is %T, want *CounterType", again.Type)
	}
}

func TestHasTupleColumns(t *testing.T) {
	t.Parallel()

	tuple := &TupleType{ComplexType: TypeTuple, ValueTypes: []SimpleType{TypeInt, TypeInt}}
	plain := Columns{{Name: "a", Type: TypeInt}}
	withTuple := Columns{{Name: "a", Type: TypeInt}, {Name: "t", Type: tuple}}

	cases := map[string]struct {
		table *Table
		want  bool
	}{
		"none":            {table: &Table{PartitionKeys: plain, ClusteringKeys: plain, Columns: plain}},
		"partition key":   {table: &Table{PartitionKeys: withTuple}, want: true},
		"clustering key":  {table: &Table{PartitionKeys: plain, ClusteringKeys: withTuple}, want: true},
		"regular columns": {table: &Table{PartitionKeys: plain, Columns: withTuple}, want: true},
	}
	for name, tc := range cases {
		if got := tc.table.HasTupleColumns(); got != tc.want {
			t.Errorf("%s: HasTupleColumns() = %v, want %v", name, got, tc.want)
		}
	}
}

func BenchmarkMapGenValue(b *testing.B) {
	mt := &MapType{ComplexType: TypeMap, KeyType: TypeInt, ValueType: TypeText}
	r := rand.New(rand.NewPCG(1, 1))

	b.ReportAllocs()
	for b.Loop() {
		_ = mt.GenValue(r, genTestRange)
	}
}

func BenchmarkMapGenValueVarint(b *testing.B) {
	mt := &MapType{ComplexType: TypeMap, KeyType: TypeBigint, ValueType: TypeVarint}
	r := rand.New(rand.NewPCG(1, 1))

	b.ReportAllocs()
	for b.Loop() {
		_ = mt.GenValue(r, genTestRange)
	}
}

func BenchmarkMapGenJSONValue(b *testing.B) {
	mt := &MapType{ComplexType: TypeMap, KeyType: TypeInt, ValueType: TypeDecimal}
	r := rand.New(rand.NewPCG(1, 1))

	b.ReportAllocs()
	for b.Loop() {
		_ = mt.GenJSONValue(r, genTestRange)
	}
}

func BenchmarkTupleGenValueOut(b *testing.B) {
	tuple := &TupleType{ComplexType: TypeTuple, ValueTypes: []SimpleType{TypeInt, TypeBigint, TypeBoolean, TypeDouble}}
	r := rand.New(rand.NewPCG(1, 1))
	out := make([]any, 0, 4)

	b.ReportAllocs()
	for b.Loop() {
		out = tuple.GenValueOut(out[:0], r, genTestRange)
	}
}

func BenchmarkCollectionGenValue(b *testing.B) {
	list := &Collection{ComplexType: TypeList, ValueType: TypeInt}
	r := rand.New(rand.NewPCG(1, 1))

	b.ReportAllocs()
	for b.Loop() {
		_ = list.GenValue(r, genTestRange)
	}
}

func BenchmarkUDTGenValue(b *testing.B) {
	udt := &UDTType{
		ComplexType: TypeUdt,
		TypeName:    "udt_1",
		ValueTypes:  map[string]SimpleType{"a": TypeInt, "b": TypeBigint, "c": TypeBoolean, "d": TypeDouble},
	}
	r := rand.New(rand.NewPCG(1, 1))

	b.ReportAllocs()
	for b.Loop() {
		_ = udt.GenValue(r, genTestRange)
	}
}

func BenchmarkNewPartitionValues(b *testing.B) {
	cols := Columns{{Name: "pk1", Type: TypeInt}, {Name: "pk2", Type: TypeText}, {Name: "pk3", Type: TypeBigint}}
	src := []any{int32(1), "a", int64(2)}

	b.ReportAllocs()
	for b.Loop() {
		_ = NewPartitionValues(cols, src)
	}
}
