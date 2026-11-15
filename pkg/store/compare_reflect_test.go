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

package store

import (
	"cmp"
	"math"
	"math/big"
	"math/rand/v2"
	"net"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
)

// referenceCompareStructured is the element-by-element comparator that boxes
// every element through compareValues. compareReflect must order every pair
// of values the same way.
func referenceCompareStructured(a, b reflect.Value) (int, bool) {
	if a.Type() != b.Type() {
		return 0, false
	}

	switch a.Kind() {
	case reflect.Pointer:
		if a.IsNil() || b.IsNil() {
			return comparePresence(a.IsNil(), b.IsNil()), true
		}
		return referenceCompareValues(a.Elem().Interface(), b.Elem().Interface()), true
	case reflect.Slice, reflect.Array:
		n := min(a.Len(), b.Len())
		for i := range n {
			if c := referenceCompareValues(a.Index(i).Interface(), b.Index(i).Interface()); c != 0 {
				return c, true
			}
		}
		return cmp.Compare(a.Len(), b.Len()), true
	case reflect.Map:
		if c := cmp.Compare(a.Len(), b.Len()); c != 0 {
			return c, true
		}
		keys := func(m reflect.Value) []reflect.Value {
			k := m.MapKeys()
			slices.SortFunc(k, func(x, y reflect.Value) int {
				return referenceCompareValues(x.Interface(), y.Interface())
			})
			return k
		}
		aKeys, bKeys := keys(a), keys(b)
		for i := range aKeys {
			if c := referenceCompareValues(aKeys[i].Interface(), bKeys[i].Interface()); c != 0 {
				return c, true
			}
		}
		for i := range aKeys {
			if c := referenceCompareValues(a.MapIndex(aKeys[i]).Interface(), b.MapIndex(bKeys[i]).Interface()); c != 0 {
				return c, true
			}
		}
		return 0, true
	default:
		return 0, false
	}
}

// referenceCompareValues is compareValues with referenceCompareStructured for
// the values that compareValues hands to compareStructured.
func referenceCompareValues(a, b any) int {
	if a != nil && b != nil {
		ra, rb := reflect.ValueOf(a), reflect.ValueOf(b)
		if ra.Type() == rb.Type() {
			switch ra.Kind() {
			case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
				if !isScalarCase(a) {
					c, _ := referenceCompareStructured(ra, rb)
					return c
				}
			default:
			}
		}
	}

	return compareValues(a, b)
}

// isScalarCase reports whether compareValues has a case of its own for v.
func isScalarCase(v any) bool {
	switch v.(type) {
	case []byte, *[]byte, *string, *bool, *float32, *float64, *int, *int8, *int16, *int32, *int64,
		*uint, *uint8, *uint16, *uint32, *uint64, gocql.UUID, *gocql.UUID, *time.Time, *time.Duration,
		*big.Int, *inf.Dec, net.IP, *net.IP, *gocql.Duration:
		return true
	default:
		return false
	}
}

// scalarGens produce values from small domains, so pairs are often equal or
// share a prefix.
var scalarGens = map[string]func(r *rand.Rand) any{
	"string":   func(r *rand.Rand) any { return string(rune('a' + r.IntN(3))) },
	"int":      func(r *rand.Rand) any { return r.IntN(3) - 1 },
	"int8":     func(r *rand.Rand) any { return int8(r.IntN(3) - 1) },
	"int16":    func(r *rand.Rand) any { return int16(r.IntN(3) - 1) },
	"int32":    func(r *rand.Rand) any { return int32(r.IntN(3) - 1) },
	"int64":    func(r *rand.Rand) any { return int64(r.IntN(3) - 1) },
	"bool":     func(r *rand.Rand) any { return r.IntN(2) == 0 },
	"float32":  func(r *rand.Rand) any { return []float32{-1, 0, 1, float32(math.NaN())}[r.IntN(4)] },
	"float64":  func(r *rand.Rand) any { return []float64{-1.5, 0, 1.5, math.NaN(), math.Inf(1)}[r.IntN(5)] },
	"duration": func(r *rand.Rand) any { return time.Duration(r.IntN(3)) * time.Second },
	"uuid": func(r *rand.Rand) any {
		var u gocql.UUID
		u[r.IntN(16)] = byte(r.IntN(2))
		return u
	},
	"time": func(r *rand.Rand) any {
		t := time.Unix(int64(r.IntN(3)), 0)
		if r.IntN(2) == 0 {
			// The same instant in another location: Compare calls them equal.
			return t.In(time.FixedZone("x", 3600))
		}
		return t.UTC()
	},
	"blob":   func(r *rand.Rand) any { return []byte{byte(r.IntN(2)), byte(r.IntN(2))}[:r.IntN(3)] },
	"bigint": func(r *rand.Rand) any { return big.NewInt(int64(r.IntN(3) - 1)) },
	"dec":    func(r *rand.Rand) any { return inf.NewDec(int64(r.IntN(3)), inf.Scale(r.IntN(2))) },
	"inet": func(r *rand.Rand) any {
		ip := net.IPv4(10, 0, 0, byte(r.IntN(2)))
		if r.IntN(2) == 0 {
			return ip.To4() // the 4-byte and the 16-byte form must compare equal
		}
		return ip
	},
	"cqlduration": func(r *rand.Rand) any { return gocql.Duration{Days: int32(r.IntN(2)), Nanoseconds: int64(r.IntN(2))} },
}

// shapes build scan-like values: pointers to typed slices and maps, as gocql
// allocates them for collection columns, plus []any and nested collections.
func shapes() map[string]func(r *rand.Rand) any {
	out := map[string]func(r *rand.Rand) any{}

	for name, gen := range scalarGens {
		elemType := reflect.TypeOf(gen(rand.New(rand.NewPCG(1, 1))))

		out["*[]"+name] = func(r *rand.Rand) any {
			n := r.IntN(4)
			s := reflect.MakeSlice(reflect.SliceOf(elemType), n, n)
			for i := range n {
				s.Index(i).Set(reflect.ValueOf(gen(r)))
			}
			p := reflect.New(s.Type())
			p.Elem().Set(s)
			return p.Interface()
		}

		if elemType.Comparable() && name != "float32" && name != "float64" {
			out["*map["+name+"]string"] = func(r *rand.Rand) any {
				m := reflect.MakeMap(reflect.MapOf(elemType, reflect.TypeFor[string]()))
				for range r.IntN(4) {
					key := gen(r)
					// A CQL map holds each key value once. Go map keys of
					// pointers or of time.Time can hold one value twice.
					duplicate := false
					for _, k := range m.MapKeys() {
						if compareValues(k.Interface(), key) == 0 {
							duplicate = true
						}
					}
					if !duplicate {
						m.SetMapIndex(reflect.ValueOf(key), reflect.ValueOf(scalarGens["string"](r)))
					}
				}
				p := reflect.New(m.Type())
				p.Elem().Set(m)
				return p.Interface()
			}
		}

		out["*map[string]"+name] = func(r *rand.Rand) any {
			m := reflect.MakeMap(reflect.MapOf(reflect.TypeFor[string](), elemType))
			for range r.IntN(4) {
				m.SetMapIndex(reflect.ValueOf(scalarGens["string"](r)), reflect.ValueOf(gen(r)))
			}
			p := reflect.New(m.Type())
			p.Elem().Set(m)
			return p.Interface()
		}
	}

	out["*[]any"] = func(r *rand.Rand) any {
		s := make([]any, r.IntN(4))
		for i := range s {
			if r.IntN(5) == 0 {
				continue // a null tuple element
			}
			s[i] = scalarGens["int64"](r)
		}
		return &s
	}

	out["*map[string][]int32"] = func(r *rand.Rand) any {
		m := map[string][]int32{}
		for range r.IntN(3) {
			m[string(rune('a'+r.IntN(3)))] = []int32{int32(r.IntN(2)), int32(r.IntN(2))}[:r.IntN(3)]
		}
		return &m
	}

	return out
}

func compareSign(c int) int {
	return cmp.Compare(c, 0)
}

// TestCompareReflectMatchesReference checks that compareReflect orders every
// pair of scan-shaped values the same way as the boxing comparator, so row
// equality and row sort order do not change.
func TestCompareReflectMatchesReference(t *testing.T) {
	t.Parallel()

	for name, gen := range shapes() {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			r := rand.New(rand.NewPCG(42, uint64(len(name))))
			for range 2000 {
				a, b := gen(r), gen(r)

				want := compareSign(referenceCompareValues(a, b))
				if got := compareSign(compareValues(a, b)); got != want {
					t.Fatalf("compareValues(%#v, %#v) = %d, reference = %d", a, b, got, want)
				}

				if got := compareSign(compareValues(a, a)); got != 0 {
					t.Fatalf("compareValues(%#v, itself) = %d, want 0", a, got)
				}
			}
		})
	}
}

func BenchmarkCompareCollections(b *testing.B) {
	list := []string{"alpha", "beta", "gamma", "delta", "epsilon"}
	m := map[string]int64{"a": 1, "b": 2, "c": 3, "d": 4}
	other := map[string]int64{"a": 1, "b": 2, "c": 3, "d": 4}

	b.Run("list", func(b *testing.B) {
		x, y := slices.Clone(list), slices.Clone(list)
		b.ReportAllocs()
		for b.Loop() {
			_ = compareValues(&x, &y)
		}
	})

	b.Run("map", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = compareValues(&m, &other)
		}
	})

	b.Run("reference list", func(b *testing.B) {
		x, y := slices.Clone(list), slices.Clone(list)
		b.ReportAllocs()
		for b.Loop() {
			_ = referenceCompareValues(&x, &y)
		}
	})

	b.Run("reference map", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			_ = referenceCompareValues(&m, &other)
		}
	})
}
