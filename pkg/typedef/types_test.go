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

package typedef_test

import (
	"math/big"
	"net"
	"testing"
	"time"

	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/typedef"
)

var millennium = time.Date(1999, 12, 31, 23, 59, 59, 0, time.UTC)

var prettytests = []struct {
	typ      typedef.Type
	query    string
	expected string
	values   []interface{}
}{
	{
		typ:      typedef.TYPE_ASCII,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"a"},
		expected: "SELECT * FROM tbl WHERE pk0='a'",
	},
	{
		typ:      typedef.TYPE_BIGINT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{big.NewInt(10)},
		expected: "SELECT * FROM tbl WHERE pk0=10",
	},
	{
		typ:      typedef.TYPE_BLOB,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"a"},
		expected: "SELECT * FROM tbl WHERE pk0=textasblob('a')",
	},
	{
		typ:      typedef.TYPE_BOOLEAN,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{true},
		expected: "SELECT * FROM tbl WHERE pk0=true",
	},
	{
		typ:      typedef.TYPE_DATE,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{millennium.Format("2006-01-02")},
		expected: "SELECT * FROM tbl WHERE pk0='1999-12-31'",
	},
	{
		typ:      typedef.TYPE_DECIMAL,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{inf.NewDec(1000, 0)},
		expected: "SELECT * FROM tbl WHERE pk0=1000",
	},
	{
		typ:      typedef.TYPE_DOUBLE,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{10.0},
		expected: "SELECT * FROM tbl WHERE pk0=10.00",
	},
	{
		typ:      typedef.TYPE_DURATION,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{10 * time.Minute},
		expected: "SELECT * FROM tbl WHERE pk0=10m0s",
	},
	{
		typ:      typedef.TYPE_FLOAT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{10.0},
		expected: "SELECT * FROM tbl WHERE pk0=10.00",
	},
	{
		typ:      typedef.TYPE_INET,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{net.ParseIP("192.168.0.1")},
		expected: "SELECT * FROM tbl WHERE pk0='192.168.0.1'",
	},
	{
		typ:      typedef.TYPE_INT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{10},
		expected: "SELECT * FROM tbl WHERE pk0=10",
	},
	{
		typ:      typedef.TYPE_SMALLINT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{2},
		expected: "SELECT * FROM tbl WHERE pk0=2",
	},
	{
		typ:      typedef.TYPE_TEXT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"a"},
		expected: "SELECT * FROM tbl WHERE pk0='a'",
	},
	{
		typ:      typedef.TYPE_TIME,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{millennium},
		expected: "SELECT * FROM tbl WHERE pk0='" + millennium.Format(time.RFC3339) + "'",
	},
	{
		typ:      typedef.TYPE_TIMESTAMP,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{millennium},
		expected: "SELECT * FROM tbl WHERE pk0='" + millennium.Format(time.RFC3339) + "'",
	},
	{
		typ:      typedef.TYPE_TIMEUUID,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"63176980-bfde-11d3-bc37-1c4d704231dc"},
		expected: "SELECT * FROM tbl WHERE pk0=63176980-bfde-11d3-bc37-1c4d704231dc",
	},
	{
		typ:      typedef.TYPE_TINYINT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{1},
		expected: "SELECT * FROM tbl WHERE pk0=1",
	},
	{
		typ:      typedef.TYPE_UUID,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"63176980-bfde-11d3-bc37-1c4d704231dc"},
		expected: "SELECT * FROM tbl WHERE pk0=63176980-bfde-11d3-bc37-1c4d704231dc",
	},
	{
		typ:      typedef.TYPE_VARCHAR,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"a"},
		expected: "SELECT * FROM tbl WHERE pk0='a'",
	},
	{
		typ:      typedef.TYPE_VARINT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{big.NewInt(1001)},
		expected: "SELECT * FROM tbl WHERE pk0=1001",
	},
	{
		typ: &typedef.BagType{
			ComplexType: typedef.TYPE_SET,
			ValueType:   typedef.TYPE_ASCII,
			Frozen:      false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{[]string{"a", "b"}},
		expected: "SELECT * FROM tbl WHERE pk0={'a','b'}",
	},
	{
		typ: &typedef.BagType{
			ComplexType: typedef.TYPE_LIST,
			ValueType:   typedef.TYPE_ASCII,
			Frozen:      false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{[]string{"a", "b"}},
		expected: "SELECT * FROM tbl WHERE pk0=['a','b']",
	},
	{
		typ: &typedef.MapType{
			KeyType:   typedef.TYPE_ASCII,
			ValueType: typedef.TYPE_ASCII,
			Frozen:    false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{map[string]string{"a": "b"}},
		expected: "SELECT * FROM tbl WHERE pk0={a:'b'}",
	},
	{
		typ: &typedef.MapType{
			KeyType:   typedef.TYPE_ASCII,
			ValueType: typedef.TYPE_BLOB,
			Frozen:    false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{map[string]string{"a": "b"}},
		expected: "SELECT * FROM tbl WHERE pk0={a:textasblob('b')}",
	},
	{
		typ: &typedef.TupleType{
			ValueTypes: []typedef.SimpleType{typedef.TYPE_ASCII},
			Frozen:     false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"a"},
		expected: "SELECT * FROM tbl WHERE pk0='a'",
	},
	{
		typ: &typedef.TupleType{
			ValueTypes: []typedef.SimpleType{typedef.TYPE_ASCII, typedef.TYPE_ASCII},
			Frozen:     false,
		},
		query:    "SELECT * FROM tbl WHERE pk0={?,?}",
		values:   []interface{}{"a", "b"},
		expected: "SELECT * FROM tbl WHERE pk0={'a','b'}",
	},
}

func TestCQLPretty(t *testing.T) {
	t.Parallel()

	for _, p := range prettytests {
		result, _ := p.typ.CQLPretty(p.query, p.values)
		if result != p.expected {
			t.Fatalf("expected '%s', got '%s' for values %v and type '%v'", p.expected, result, p.values, p.typ)
		}
	}
}
