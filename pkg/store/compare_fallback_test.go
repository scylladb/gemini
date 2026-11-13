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
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/gocql/gocql"
)

// TestCompareValues_FallbackTypesAgreeWithStrings checks that the comparison of
// the types that used to fall back to fmt.Sprint reports equality exactly when
// the string representations are equal, and orders every pair consistently.
func TestCompareValues_FallbackTypesAgreeWithStrings(t *testing.T) {
	t.Parallel()

	groups := map[string][]any{
		"list": {new([]int32{1, 2, 3}), new([]int32{1, 2, 3}), new([]int32{3, 2, 1}), new([]int32{}), new([]int32(nil))},
		"set":  {new([]string{"a", "b"}), new([]string{"a", "b"}), new([]string{"a"}), new([]string{})},
		"map": {
			new(map[string]int32{"a": 1, "b": 2}), new(map[string]int32{"b": 2, "a": 1}),
			new(map[string]int32{"a": 1}), new(map[string]int32{}),
		},
		"udt": {
			new(map[string]any{"f1": int32(1), "f2": "x"}), new(map[string]any{"f2": "x", "f1": int32(1)}),
			new(map[string]any{"f1": int32(2), "f2": "x"}),
		},
		"inet": {
			net.ParseIP("10.0.0.1").To4(), net.ParseIP("10.0.0.1"), net.ParseIP("10.0.0.2"),
			new(net.ParseIP("192.168.1.1")), new(net.ParseIP("192.168.1.1").To4()), new(net.ParseIP("::1")),
		},
		"duration": {
			gocql.Duration{Months: 1, Days: 2, Nanoseconds: 3},
			gocql.Duration{Months: 1, Days: 2, Nanoseconds: 3},
			gocql.Duration{Days: 40},
			new(gocql.Duration{Nanoseconds: 5}), new(gocql.Duration{Nanoseconds: 5}),
		},
		"float list": {new([]float64{1.5, 2.5}), new([]float64{1.5, 2.5}), new([]float64{2.5})},
		"list of timestamps": {
			new([]time.Time{time.Unix(10, 0).UTC(), time.Unix(20, 0).UTC()}),
			new([]time.Time{time.Unix(10, 0).UTC(), time.Unix(20, 0).UTC()}),
			new([]time.Time{time.Unix(10, 0).UTC()}),
		},
		"nested": {
			new(map[string][]int32{"a": {1, 2}}), new(map[string][]int32{"a": {1, 2}}),
			new(map[string][]int32{"a": {2}}), new(map[string][]int32{"b": {1, 2}}),
		},
		"nil pointers": {(*[]int32)(nil), (*[]int32)(nil), new([]int32{1})},
	}

	for name, values := range groups {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			for i, a := range values {
				for j, b := range values {
					got := compareValues(a, b)
					stringsEqual := fmt.Sprint(a) == fmt.Sprint(b)
					if (got == 0) != stringsEqual {
						t.Errorf("[%d] %v vs [%d] %v: compareValues = %d, string representations equal = %v",
							i, a, j, b, got, stringsEqual)
					}
					if back := compareValues(b, a); sign(back) != -sign(got) {
						t.Errorf("[%d] vs [%d]: compareValues is not antisymmetric: %d and %d", i, j, got, back)
					}
				}
			}
		})
	}
}

// TestCompareValues_StructuredOrderIsTransitive sorts unequal collections and
// checks that the order is a total order, which the row sort relies on.
func TestCompareValues_StructuredOrderIsTransitive(t *testing.T) {
	t.Parallel()

	values := []any{
		new([]int32{3}), new([]int32{1, 2}), new([]int32{1}), new([]int32{}), new([]int32{1, 3}),
		new([]int32{2, 0}), (*[]int32)(nil),
	}

	for _, a := range values {
		for _, b := range values {
			for _, c := range values {
				if compareValues(a, b) <= 0 && compareValues(b, c) <= 0 && compareValues(a, c) > 0 {
					t.Fatalf("order is not transitive: %v <= %v <= %v but %v > %v", a, b, c, a, c)
				}
			}
		}
	}
}

func sign(v int) int {
	switch {
	case v < 0:
		return -1
	case v > 0:
		return 1
	default:
		return 0
	}
}
