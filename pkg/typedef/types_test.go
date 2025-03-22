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
	"math/big"
	"net"
	"testing"
	"time"

	"gopkg.in/inf.v0"
)

var millennium = time.Date(1999, 12, 31, 23, 59, 59, 0, time.UTC)

var prettytests = []struct {
	typ      Type
	query    string
	expected string
	values   []any
}{
	{
		typ:      TypeAscii,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{"a"},
		expected: "SELECT * FROM tbl WHERE pk0='a'",
	},
	{
		typ:      TypeBigint,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{big.NewInt(10)},
		expected: "SELECT * FROM tbl WHERE pk0=10",
	},
	{
		typ:      TypeBlob,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{"a"},
		expected: "SELECT * FROM tbl WHERE pk0=textasblob('a')",
	},
	{
		typ:      TypeBoolean,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{true},
		expected: "SELECT * FROM tbl WHERE pk0=true",
	},
	{
		typ:      TypeDate,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{millennium.Format("2006-01-02")},
		expected: "SELECT * FROM tbl WHERE pk0='1999-12-31'",
	},
	{
		typ:      TypeDecimal,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{inf.NewDec(1000, 0)},
		expected: "SELECT * FROM tbl WHERE pk0=1000",
	},
	{
		typ:      TypeDouble,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{10.0},
		expected: "SELECT * FROM tbl WHERE pk0=10.00",
	},
	{
		typ:      TypeDuration,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{10 * time.Minute},
		expected: "SELECT * FROM tbl WHERE pk0=10m0s",
	},
	{
		typ:      TypeFloat,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{10.0},
		expected: "SELECT * FROM tbl WHERE pk0=10.00",
	},
	{
		typ:      TypeInet,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{net.ParseIP("192.168.0.1")},
		expected: "SELECT * FROM tbl WHERE pk0='192.168.0.1'",
	},
	{
		typ:      TypeInt,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{10},
		expected: "SELECT * FROM tbl WHERE pk0=10",
	},
	{
		typ:      TypeSmallint,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{2},
		expected: "SELECT * FROM tbl WHERE pk0=2",
	},
	{
		typ:      TypeText,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{"a"},
		expected: "SELECT * FROM tbl WHERE pk0='a'",
	},
	{
		typ:      TypeTime,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{millennium.UnixNano()},
		expected: "SELECT * FROM tbl WHERE pk0='" + millennium.Format("15:04:05.999") + "'",
	},
	{
		typ:      TypeTimestamp,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{millennium.UnixMilli()},
		expected: "SELECT * FROM tbl WHERE pk0='" + millennium.Format("2006-01-02T15:04:05.999-0700") + "'",
	},
	{
		typ:      TypeTimeuuid,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{"63176980-bfde-11d3-bc37-1c4d704231dc"},
		expected: "SELECT * FROM tbl WHERE pk0=63176980-bfde-11d3-bc37-1c4d704231dc",
	},
	{
		typ:      TypeTinyint,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{1},
		expected: "SELECT * FROM tbl WHERE pk0=1",
	},
	{
		typ:      TypeUuid,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{"63176980-bfde-11d3-bc37-1c4d704231dc"},
		expected: "SELECT * FROM tbl WHERE pk0=63176980-bfde-11d3-bc37-1c4d704231dc",
	},
	{
		typ:      TypeVarchar,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{"a"},
		expected: "SELECT * FROM tbl WHERE pk0='a'",
	},
	{
		typ:      TypeVarint,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{big.NewInt(1001)},
		expected: "SELECT * FROM tbl WHERE pk0=1001",
	},
	{
		typ: &BagType{
			ComplexType: TypeSet,
			ValueType:   TypeAscii,
			Frozen:      false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{[]string{"a", "b"}},
		expected: "SELECT * FROM tbl WHERE pk0={'a','b'}",
	},
	{
		typ: &BagType{
			ComplexType: TypeList,
			ValueType:   TypeAscii,
			Frozen:      false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{[]string{"a", "b"}},
		expected: "SELECT * FROM tbl WHERE pk0=['a','b']",
	},
	{
		typ: &MapType{
			KeyType:   TypeAscii,
			ValueType: TypeAscii,
			Frozen:    false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{map[string]string{"a": "b"}},
		expected: "SELECT * FROM tbl WHERE pk0={'a':'b'}",
	},
	{
		typ: &MapType{
			KeyType:   TypeAscii,
			ValueType: TypeBlob,
			Frozen:    false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []any{map[string]string{"a": "b"}},
		expected: "SELECT * FROM tbl WHERE pk0={'a':textasblob('b')}",
	},
	{
		typ: &TupleType{
			ValueTypes: []SimpleType{TypeAscii},
			Frozen:     false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=(?)",
		values:   []interface{}{"a"},
		expected: "SELECT * FROM tbl WHERE pk0=('a')",
	},
	{
		typ: &TupleType{
			ValueTypes: []SimpleType{TypeAscii, TypeAscii},
			Frozen:     false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=(?,?)",
		values:   []interface{}{"a", "b"},
		expected: "SELECT * FROM tbl WHERE pk0=('a','b')",
	},
}

func TestCQLPretty(t *testing.T) {
	t.Parallel()

	for id := range prettytests {
		test := prettytests[id]
		t.Run(test.typ.Name(), func(t *testing.T) {
			t.Parallel()
			builder := bytes.NewBuffer(nil)
			err := prettyCQL(builder, test.query, test.values, []Type{test.typ})
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			result := builder.String()
			if result != test.expected {
				t.Errorf("expected '%s', got '%s' for values %v and type '%v'", test.expected, result, test.values, test.typ)
			}
		})
	}
}
