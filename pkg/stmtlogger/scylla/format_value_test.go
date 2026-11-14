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

package scylla

import (
	"fmt"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"
)

// TestFormatValueFast_MatchesGoSyntax pins the _logs values text: the direct
// cases, lists and sets among them, must print exactly what fmt.Sprintf("%#v")
// printed for these types.
func TestFormatValueFast_MatchesGoSyntax(t *testing.T) {
	t.Parallel()

	values := []any{
		1500 * time.Millisecond,
		time.Duration(-7),
		time.Date(2026, 11, 13, 10, 20, 30, 400, time.UTC),
		time.Unix(0, 0).UTC(),
		big.NewInt(42),
		big.NewInt(-9_000_000_000_000),
		(*big.Int)(nil),
		inf.NewDec(12345, 3),
		inf.NewDec(-1, 0),
		(*inf.Dec)(nil),
		[]any{"a\"b", int64(-2), int32(3), int16(4), int8(5), 6, true, nil},
		[]any{3 * time.Second, time.Date(2026, 1, 2, 3, 4, 5, 6, time.UTC)},
		[]any{[]byte{1, 2}, 1.5, float32(2.5), gocql.TimeUUID(), big.NewInt(7), inf.NewDec(1, 2)},
		[]any{[]any{"nested", nil}, map[string]any{"k": 1}},
		[]any{[]byte(nil), []byte{}, []byte{0, 0xff, 0x10}, net.IP{10, 0, 0, 1}},
		[]any{},
		[]any(nil),
	}

	for _, v := range values {
		if got, want := formatValueFast(v), fmt.Sprintf("%#v", v); got != want {
			t.Errorf("formatValueFast(%T) = %q, want %q", v, got, want)
		}
	}
}
