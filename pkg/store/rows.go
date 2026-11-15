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

package store

import (
	"bytes"
	"cmp"
	"encoding/json/v2"
	"fmt"
	"math"
	"math/big"
	"net"
	"reflect"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
)

type (
	// columnIndex maps column names to their position in a Row's value slice.
	// It is cached and shared *by pointer* across every row with the same column
	// layout, so all rows from one query reference the same *columnIndex. That
	// lets callers test layout identity with a pointer comparison (a == b)
	// instead of reflecting on the underlying map — see uniformKeyIndices.
	columnIndex struct {
		byName map[string]int
	}

	// Row represents a database row with column values stored as a slice
	// and a shared mapping from column names to indices for efficient access.
	Row struct {
		columns *columnIndex
		values  []any
	}

	Rows []Row
)

// columnIndexCache caches the column-name→index mapping for a given set
// of columns. Since gocql always returns the same column order for a
// given query, we build the layout once and share it (by pointer) across
// all rows from the same iterator.
var columnIndexCache sync.Map // key: joined column names, value: *columnIndex

func getColumnIndex(columnNames []string) *columnIndex {
	// Build a cache key from column names. Using a simple join since
	// column names don't contain null bytes.
	key := strings.Join(columnNames, "\x00")

	if cached, ok := columnIndexCache.Load(key); ok {
		return mustColumnIndex(cached)
	}

	byName := make(map[string]int, len(columnNames))
	for i, name := range columnNames {
		byName[name] = i
	}
	// LoadOrStore so concurrent first-builders converge on a single shared pointer.
	actual, _ := columnIndexCache.LoadOrStore(key, &columnIndex{byName: byName})
	return mustColumnIndex(actual)
}

// mustColumnIndex returns v as a *columnIndex. columnIndexCache stores only
// *columnIndex values, so any other type is a gemini bug.
func mustColumnIndex(v any) *columnIndex {
	ci, ok := v.(*columnIndex)
	if !ok {
		panic(fmt.Sprintf("columnIndexCache holds %T, want *columnIndex", v))
	}

	return ci
}

// NewRow creates a new Row with the given column names and values.
// The column-name→index layout is cached and shared across rows with the
// same column layout, eliminating per-row map allocation.
func NewRow(columnNames []string, values []any) Row {
	return newRowWithIndex(getColumnIndex(columnNames), values)
}

// newRowWithIndex creates a Row from a column index that the caller resolved
// once for a whole result set. The scan loops use it, so a row costs no key
// build and no cache lookup.
func newRowWithIndex(columns *columnIndex, values []any) Row {
	return Row{
		values:  values,
		columns: columns,
	}
}

// Get returns the value for the given column name
func (r Row) Get(columnName string) any {
	if r.columns == nil {
		return nil
	}
	if idx, ok := r.columns.byName[columnName]; ok {
		return r.values[idx]
	}
	return nil
}

// hasColumn reports whether the row has a column with the given name.
// Unlike Get, it distinguishes "column absent" from "column present with nil value".
func (r Row) hasColumn(columnName string) bool {
	if r.columns == nil {
		return false
	}
	_, ok := r.columns.byName[columnName]
	return ok
}

// Set sets the value for the given column name
func (r Row) Set(columnName string, value any) {
	if r.columns == nil {
		return
	}
	if idx, ok := r.columns.byName[columnName]; ok {
		r.values[idx] = value
	}
}

func (r Rows) Len() int {
	return len(r)
}

func (r Rows) Swap(i, j int) {
	r[i], r[j] = r[j], r[i]
}

func (r Rows) Less(i, j int) bool {
	return rowsCmp(r[i], r[j]) < 0
}

// MarshalJSON provides a readable JSON representation of a Row.
//
// The Row type stores values internally as a slice with a map of column
// names to indices. Since the internal fields are unexported, the default
// JSON encoder would serialize a Row as an empty object ({}).
//
// This custom marshaler converts the internal representation into a
// map[columnName]value so logs and errors include meaningful row content.
func (r Row) MarshalJSON() ([]byte, error) {
	if r.columns == nil {
		return json.Marshal(map[string]any{}, json.Deterministic(true))
	}
	// Build a temporary map to serialize column values by name.
	// Pre-size the map for efficiency.
	out := make(map[string]any, len(r.columns.byName))
	for name, idx := range r.columns.byName {
		// Guard against out-of-range indexes in case of malformed rows
		if idx >= 0 && idx < len(r.values) {
			out[name] = r.values[idx]
		} else {
			out[name] = nil
		}
	}
	return json.Marshal(out, json.Deterministic(true))
}

// ValuesEqual reports whether two values are semantically equal, handling
// cross-type pointer/value equivalence correctly.
//
// Unlike compareValues (which falls back to fmt.Sprint for cross-type pairs and
// therefore prints pointer addresses for pointer types), valuesEqual explicitly
// handles all cross-type deref cases: *T vs T and T vs *T for every gocql scalar.
//
// Rules:
//   - nil == nil (regardless of type)
//   - nil pointer != non-nil value
//   - *T(x) == T(x) iff *x == x
//   - T(x) == *T(x) iff x == *x
//   - For all same-type pairs: uses the same logic as compareValues
//
//nolint:gocyclo,cyclop
func ValuesEqual(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Fast path: same type — delegate to compareValues (zero-allocation for all
	// known concrete types).
	if compareValues(a, b) == 0 {
		return true
	}

	// Slow path: cross-type pointer/value pairs.
	// We handle every scalar type gocql can return.
	switch av := a.(type) {
	case string:
		if bv, ok := b.(*string); ok {
			return bv != nil && av == *bv
		}
	case *string:
		if bv, ok := b.(string); ok {
			return av != nil && *av == bv
		}
	case bool:
		if bv, ok := b.(*bool); ok {
			return bv != nil && av == *bv
		}
	case *bool:
		if bv, ok := b.(bool); ok {
			return av != nil && *av == bv
		}
	case int:
		if bv, ok := b.(*int); ok {
			return bv != nil && av == *bv
		}
	case *int:
		if bv, ok := b.(int); ok {
			return av != nil && *av == bv
		}
	case int8:
		if bv, ok := b.(*int8); ok {
			return bv != nil && av == *bv
		}
	case *int8:
		if bv, ok := b.(int8); ok {
			return av != nil && *av == bv
		}
	case int16:
		if bv, ok := b.(*int16); ok {
			return bv != nil && av == *bv
		}
	case *int16:
		if bv, ok := b.(int16); ok {
			return av != nil && *av == bv
		}
	case int32:
		if bv, ok := b.(*int32); ok {
			return bv != nil && av == *bv
		}
	case *int32:
		if bv, ok := b.(int32); ok {
			return av != nil && *av == bv
		}
	case int64:
		if bv, ok := b.(*int64); ok {
			return bv != nil && av == *bv
		}
	case *int64:
		if bv, ok := b.(int64); ok {
			return av != nil && *av == bv
		}
	case uint:
		if bv, ok := b.(*uint); ok {
			return bv != nil && av == *bv
		}
	case *uint:
		if bv, ok := b.(uint); ok {
			return av != nil && *av == bv
		}
	case uint8:
		if bv, ok := b.(*uint8); ok {
			return bv != nil && av == *bv
		}
	case *uint8:
		if bv, ok := b.(uint8); ok {
			return av != nil && *av == bv
		}
	case uint16:
		if bv, ok := b.(*uint16); ok {
			return bv != nil && av == *bv
		}
	case *uint16:
		if bv, ok := b.(uint16); ok {
			return av != nil && *av == bv
		}
	case uint32:
		if bv, ok := b.(*uint32); ok {
			return bv != nil && av == *bv
		}
	case *uint32:
		if bv, ok := b.(uint32); ok {
			return av != nil && *av == bv
		}
	case uint64:
		if bv, ok := b.(*uint64); ok {
			return bv != nil && av == *bv
		}
	case *uint64:
		if bv, ok := b.(uint64); ok {
			return av != nil && *av == bv
		}
	case float32:
		if bv, ok := b.(*float32); ok {
			return bv != nil && compareFloat(float64(av), float64(*bv)) == 0
		}
	case *float32:
		if bv, ok := b.(float32); ok {
			return av != nil && compareFloat(float64(*av), float64(bv)) == 0
		}
	case float64:
		if bv, ok := b.(*float64); ok {
			return bv != nil && compareFloat(av, *bv) == 0
		}
	case *float64:
		if bv, ok := b.(float64); ok {
			return av != nil && compareFloat(*av, bv) == 0
		}
	case time.Duration:
		if bv, ok := b.(*time.Duration); ok {
			return bv != nil && av == *bv
		}
	case *time.Duration:
		if bv, ok := b.(time.Duration); ok {
			return av != nil && *av == bv
		}
	case gocql.UUID:
		if bv, ok := b.(*gocql.UUID); ok {
			return bv != nil && av == *bv
		}
	case *gocql.UUID:
		if bv, ok := b.(gocql.UUID); ok {
			return av != nil && *av == bv
		}
	case time.Time:
		if bv, ok := b.(*time.Time); ok {
			return bv != nil && av.Equal(*bv)
		}
	case *time.Time:
		if bv, ok := b.(time.Time); ok {
			return av != nil && av.Equal(bv)
		}
	}

	// Last resort: canonicalValueString dereferences all pointer types and
	// formats with strconv — safe, no address leakage.
	return canonicalValueString(a) == canonicalValueString(b)
}

// rowsEqual reports whether two Rows slices are value-equal.
// It replaces reflect.DeepEqual(a, b) in the fast-path of CompareCollectedRows.
// reflect.DeepEqual allocates heavily when walking the map[string]int inside
// every Row and treats *string(x) != string(x).
//
// rowsEqual uses valuesEqual which handles cross-type pointer/value equivalence
// (e.g. *string vs string with the same content) without leaking pointer addresses.
func rowsEqual(a, b Rows) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		av := a[i].values
		bv := b[i].values
		if len(av) != len(bv) {
			return false
		}
		for j := range av {
			if !ValuesEqual(av[j], bv[j]) {
				return false
			}
		}
	}
	return true
}

// rowsCmp compares two rows column-by-column using their value order.
// It supports a wide range of scalar types and falls back to string
// comparison for differing concrete types.
func rowsCmp(i, j Row) int {
	maximum := min(len(j.values), len(i.values))
	for idx := range maximum {
		if c := compareValues(i.values[idx], j.values[idx]); c != 0 {
			return c
		}
	}
	// All common columns equal; shorter row sorts first
	switch {
	case len(i.values) < len(j.values):
		return -1
	case len(i.values) > len(j.values):
		return 1
	default:
		return 0
	}
}

func comparePtr[T cmp.Ordered](a, b *T) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	return cmp.Compare(*a, *b)
}

func compareBool(a, b bool) int {
	if a == b {
		return 0
	}
	if !a {
		return -1
	}
	return 1
}

func compareBoolPtr(a, b *bool) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}
	return compareBool(*a, *b)
}

//nolint:gocyclo
func compareValues(a, b any) int {
	if a == nil && b == nil {
		return 0
	}
	if a == nil {
		return -1
	}
	if b == nil {
		return 1
	}

	switch av := a.(type) {
	case []byte:
		if bv, ok := b.([]byte); ok {
			return bytes.Compare(av, bv)
		}
	case *[]byte:
		if bv, ok := b.(*[]byte); ok {
			if av == nil && bv == nil {
				return 0
			}
			if av == nil {
				return -1
			}
			if bv == nil {
				return 1
			}
			return bytes.Compare(*av, *bv)
		}
	case string:
		if bv, ok := b.(string); ok {
			return strings.Compare(av, bv)
		}
	case *string:
		if bv, ok := b.(*string); ok {
			return comparePtr(av, bv)
		}
	case bool:
		if bv, ok := b.(bool); ok {
			return compareBool(av, bv)
		}
	case *bool:
		if bv, ok := b.(*bool); ok {
			return compareBoolPtr(av, bv)
		}
	case float32:
		if bv, ok := b.(float32); ok {
			return compareFloat(float64(av), float64(bv))
		}
	case *float32:
		if bv, ok := b.(*float32); ok {
			if av == nil || bv == nil {
				return comparePresence(av == nil, bv == nil)
			}
			return compareFloat(float64(*av), float64(*bv))
		}
	case float64:
		if bv, ok := b.(float64); ok {
			return compareFloat(av, bv)
		}
	case *float64:
		if bv, ok := b.(*float64); ok {
			if av == nil || bv == nil {
				return comparePresence(av == nil, bv == nil)
			}
			return compareFloat(*av, *bv)
		}
	case int:
		if bv, ok := b.(int); ok {
			return cmp.Compare(av, bv)
		}
	case *int:
		if bv, ok := b.(*int); ok {
			return comparePtr(av, bv)
		}
	case int8:
		if bv, ok := b.(int8); ok {
			return cmp.Compare(av, bv)
		}
	case *int8:
		if bv, ok := b.(*int8); ok {
			return comparePtr(av, bv)
		}
	case int16:
		if bv, ok := b.(int16); ok {
			return cmp.Compare(av, bv)
		}
	case *int16:
		if bv, ok := b.(*int16); ok {
			return comparePtr(av, bv)
		}
	case int32:
		if bv, ok := b.(int32); ok {
			return cmp.Compare(av, bv)
		}
	case *int32:
		if bv, ok := b.(*int32); ok {
			return comparePtr(av, bv)
		}
	case int64:
		if bv, ok := b.(int64); ok {
			return cmp.Compare(av, bv)
		}
	case *int64:
		if bv, ok := b.(*int64); ok {
			return comparePtr(av, bv)
		}
	case uint:
		if bv, ok := b.(uint); ok {
			return cmp.Compare(av, bv)
		}
	case *uint:
		if bv, ok := b.(*uint); ok {
			return comparePtr(av, bv)
		}
	case uint8:
		if bv, ok := b.(uint8); ok {
			return cmp.Compare(av, bv)
		}
	case *uint8:
		if bv, ok := b.(*uint8); ok {
			return comparePtr(av, bv)
		}
	case uint16:
		if bv, ok := b.(uint16); ok {
			return cmp.Compare(av, bv)
		}
	case *uint16:
		if bv, ok := b.(*uint16); ok {
			return comparePtr(av, bv)
		}
	case uint32:
		if bv, ok := b.(uint32); ok {
			return cmp.Compare(av, bv)
		}
	case *uint32:
		if bv, ok := b.(*uint32); ok {
			return comparePtr(av, bv)
		}
	case uint64:
		if bv, ok := b.(uint64); ok {
			return cmp.Compare(av, bv)
		}
	case *uint64:
		if bv, ok := b.(*uint64); ok {
			return comparePtr(av, bv)
		}
	case gocql.UUID:
		if bv, ok := b.(gocql.UUID); ok {
			return bytes.Compare(av[:], bv[:])
		}
	case *gocql.UUID:
		if bv, ok := b.(*gocql.UUID); ok {
			if av == nil && bv == nil {
				return 0
			}
			if av == nil {
				return -1
			}
			if bv == nil {
				return 1
			}
			return bytes.Compare(av[:], bv[:])
		}
	case time.Time:
		if bv, ok := b.(time.Time); ok {
			return av.Compare(bv)
		}
	case *time.Time:
		if bv, ok := b.(*time.Time); ok {
			if av == nil && bv == nil {
				return 0
			}
			if av == nil {
				return -1
			}
			if bv == nil {
				return 1
			}
			return av.Compare(*bv)
		}
	case time.Duration:
		if bv, ok := b.(time.Duration); ok {
			return cmp.Compare(av, bv)
		}
	case *time.Duration:
		if bv, ok := b.(*time.Duration); ok {
			return comparePtr(av, bv)
		}
	case *big.Int:
		if bv, ok := b.(*big.Int); ok {
			if av == nil && bv == nil {
				return 0
			}
			if av == nil {
				return -1
			}
			if bv == nil {
				return 1
			}
			return av.Cmp(bv)
		}
	case *inf.Dec:
		if bv, ok := b.(*inf.Dec); ok {
			if av == nil || bv == nil {
				return comparePresence(av == nil, bv == nil)
			}
			return compareDecimal(av, bv)
		}
	case net.IP:
		if bv, ok := b.(net.IP); ok {
			return compareIP(av, bv)
		}
	case *net.IP:
		if bv, ok := b.(*net.IP); ok {
			if av == nil || bv == nil {
				return comparePresence(av == nil, bv == nil)
			}
			return compareIP(*av, *bv)
		}
	case gocql.Duration:
		if bv, ok := b.(gocql.Duration); ok {
			return compareDuration(av, bv)
		}
	case *gocql.Duration:
		if bv, ok := b.(*gocql.Duration); ok {
			if av == nil || bv == nil {
				return comparePresence(av == nil, bv == nil)
			}
			return compareDuration(*av, *bv)
		}
	}

	// Collections and UDTs arrive as pointers to slices and maps.
	if c, ok := compareStructured(reflect.ValueOf(a), reflect.ValueOf(b)); ok {
		return c
	}

	// Values of different types never come from the two clusters for one
	// column. They keep an order by string representation.
	return strings.Compare(fmt.Sprint(a), fmt.Sprint(b))
}

// compareStructured compares two values of the same type that are pointers,
// slices, arrays or maps, element by element in the order of compareValues. It
// reports false when the types differ or the kind is not one of those.
//
// Slices and arrays compare element by element, then by length. Maps compare by
// length, then by their sorted keys, then by the values in key order. A nil
// pointer orders before a non-nil one.
func compareStructured(a, b reflect.Value) (int, bool) {
	if a.Type() != b.Type() {
		return 0, false
	}

	switch a.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array, reflect.Map:
		return compareReflect(a, b), true
	default:
		return 0, false
	}
}

var (
	bigIntPtrType = reflect.TypeFor[*big.Int]()
	decPtrType    = reflect.TypeFor[*inf.Dec]()
	timeType      = reflect.TypeFor[time.Time]()
	ipType        = reflect.TypeFor[net.IP]()
	durationType  = reflect.TypeFor[gocql.Duration]()
)

// compareReflect compares two values of one type with the order of
// compareValues. Collections from a scan hold their elements in place, so it
// reads basic kinds straight from the reflect.Value and boxes nothing into an
// interface on the common paths.
//
//nolint:gocyclo,cyclop // one case per reflect kind
func compareReflect(a, b reflect.Value) int {
	switch a.Type() {
	case bigIntPtrType, decPtrType, timeType, ipType, durationType:
		// Their order is not the order of their fields or bytes.
		return compareValues(a.Interface(), b.Interface())
	}

	switch a.Kind() {
	case reflect.String:
		return strings.Compare(a.String(), b.String())
	case reflect.Bool:
		return compareBool(a.Bool(), b.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return cmp.Compare(a.Int(), b.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return cmp.Compare(a.Uint(), b.Uint())
	case reflect.Float32, reflect.Float64:
		return compareFloat(a.Float(), b.Float())
	case reflect.Pointer:
		if a.IsNil() || b.IsNil() {
			return comparePresence(a.IsNil(), b.IsNil())
		}

		return compareReflect(a.Elem(), b.Elem())
	case reflect.Interface:
		if a.IsNil() || b.IsNil() {
			return comparePresence(a.IsNil(), b.IsNil())
		}

		return compareValues(a.Elem().Interface(), b.Elem().Interface())
	case reflect.Slice, reflect.Array:
		if a.Type().Elem().Kind() == reflect.Uint8 && a.Kind() == reflect.Slice {
			return bytes.Compare(a.Bytes(), b.Bytes())
		}

		n := min(a.Len(), b.Len())
		for i := range n {
			if c := compareReflect(a.Index(i), b.Index(i)); c != 0 {
				return c
			}
		}

		return cmp.Compare(a.Len(), b.Len())
	case reflect.Map:
		if c := cmp.Compare(a.Len(), b.Len()); c != 0 {
			return c
		}

		if mapsEqualByLookup(a, b) {
			return 0
		}

		aKeys, bKeys := sortedKeys(a), sortedKeys(b)
		for i := range aKeys {
			if c := compareReflect(aKeys[i], bKeys[i]); c != 0 {
				return c
			}
		}

		// The keys of both maps are equal pairwise here. Each map is read with
		// its own key: a pointer key (varint, decimal) of a is not a key of b.
		for i := range aKeys {
			if c := compareReflect(a.MapIndex(aKeys[i]), b.MapIndex(bKeys[i])); c != 0 {
				return c
			}
		}

		return 0
	default:
		return compareValues(a.Interface(), b.Interface())
	}
}

// mapsEqualByLookup reports whether two maps of one type with the same
// length hold equal entries. It looks up each key of a in b, which costs no
// sort and no key slice. Most compared maps are equal, so this is the common
// path. It reports false when the maps differ, and also when the key type has
// a Go equality that differs from compareReflect, so the caller then orders
// the maps by their sorted keys.
func mapsEqualByLookup(a, b reflect.Value) bool {
	if !lookupSafeKey(a.Type().Key()) {
		return false
	}

	key := reflect.New(a.Type().Key()).Elem()
	val := reflect.New(a.Type().Elem()).Elem()

	var it reflect.MapIter
	it.Reset(a)
	for it.Next() {
		key.SetIterKey(&it)
		val.SetIterValue(&it)

		other := b.MapIndex(key)
		if !other.IsValid() || compareReflect(val, other) != 0 {
			return false
		}
	}

	return true
}

// lookupSafeKey reports whether two map keys of type t are equal in Go
// exactly when compareReflect orders them equal. A float key is not: -0 and +0
// are one Go key but two CQL values. A pointer key is not: equal values have
// different addresses.
func lookupSafeKey(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.String, reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return true
	case reflect.Array:
		return t.Elem().Kind() == reflect.Uint8
	default:
		return false
	}
}

func sortedKeys(m reflect.Value) []reflect.Value {
	keys := m.MapKeys()
	slices.SortFunc(keys, compareReflect)

	return keys
}

// compareFloat orders floats numerically, with -0 before +0 and NaN before
// every number. CQL stores the sign of a zero, so a cluster that returns +0
// for a written -0 has changed the value. All NaNs compare equal: their
// payload is not part of what CQL defines.
func compareFloat(a, b float64) int {
	if c := cmp.Compare(a, b); c != 0 {
		return c
	}

	if a == 0 {
		return compareBool(!math.Signbit(a), !math.Signbit(b))
	}

	return 0
}

// compareDecimal orders decimals by value, then by scale. CQL keeps the scale
// of a decimal, so 1.0 and 1.00 are two different values.
func compareDecimal(a, b *inf.Dec) int {
	if c := a.Cmp(b); c != 0 {
		return c
	}

	return cmp.Compare(a.Scale(), b.Scale())
}

// comparePresence orders a nil pointer before a non-nil one. Both nil compare
// equal.
func comparePresence(aNil, bNil bool) int {
	switch {
	case aNil && bNil:
		return 0
	case aNil:
		return -1
	default:
		return 1
	}
}

// compareIP orders addresses by their 16-byte form, so the 4-byte and the
// 16-byte encoding of one IPv4 address compare equal.
func compareIP(a, b net.IP) int {
	return bytes.Compare(a.To16(), b.To16())
}

func compareDuration(a, b gocql.Duration) int {
	if c := cmp.Compare(a.Months, b.Months); c != 0 {
		return c
	}
	if c := cmp.Compare(a.Days, b.Days); c != 0 {
		return c
	}
	return cmp.Compare(a.Nanoseconds, b.Nanoseconds)
}
