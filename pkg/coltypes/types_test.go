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

package coltypes_test

import (
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/scylladb/gemini/pkg/coltypes"
	"github.com/scylladb/gemini/pkg/typedef"

	"gopkg.in/inf.v0"
)

var millennium = time.Date(1999, 12, 31, 23, 59, 59, 0, time.UTC)

var prettytests = []struct {
	typ      typedef.Type
	query    string
	expected string
	values   []interface{}
}{
	{
		typ:      coltypes.TYPE_ASCII,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"a"},
		expected: "SELECT * FROM tbl WHERE pk0='a'",
	},
	{
		typ:      coltypes.TYPE_BIGINT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{big.NewInt(10)},
		expected: "SELECT * FROM tbl WHERE pk0=10",
	},
	{
		typ:      coltypes.TYPE_BLOB,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"a"},
		expected: "SELECT * FROM tbl WHERE pk0=textasblob('a')",
	},
	{
		typ:      coltypes.TYPE_BOOLEAN,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{true},
		expected: "SELECT * FROM tbl WHERE pk0=true",
	},
	{
		typ:      coltypes.TYPE_DATE,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{millennium.Format("2006-01-02")},
		expected: "SELECT * FROM tbl WHERE pk0='1999-12-31'",
	},
	{
		typ:      coltypes.TYPE_DECIMAL,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{inf.NewDec(1000, 0)},
		expected: "SELECT * FROM tbl WHERE pk0=1000",
	},
	{
		typ:      coltypes.TYPE_DOUBLE,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{10.0},
		expected: "SELECT * FROM tbl WHERE pk0=10.00",
	},
	{
		typ:      coltypes.TYPE_DURATION,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{10 * time.Minute},
		expected: "SELECT * FROM tbl WHERE pk0=10m0s",
	},
	{
		typ:      coltypes.TYPE_FLOAT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{10.0},
		expected: "SELECT * FROM tbl WHERE pk0=10.00",
	},
	{
		typ:      coltypes.TYPE_INET,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{net.ParseIP("192.168.0.1")},
		expected: "SELECT * FROM tbl WHERE pk0='192.168.0.1'",
	},
	{
		typ:      coltypes.TYPE_INT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{10},
		expected: "SELECT * FROM tbl WHERE pk0=10",
	},
	{
		typ:      coltypes.TYPE_SMALLINT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{2},
		expected: "SELECT * FROM tbl WHERE pk0=2",
	},
	{
		typ:      coltypes.TYPE_TEXT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"a"},
		expected: "SELECT * FROM tbl WHERE pk0='a'",
	},
	{
		typ:      coltypes.TYPE_TIME,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{millennium},
		expected: "SELECT * FROM tbl WHERE pk0='" + millennium.Format(time.RFC3339) + "'",
	},
	{
		typ:      coltypes.TYPE_TIMESTAMP,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{millennium},
		expected: "SELECT * FROM tbl WHERE pk0='" + millennium.Format(time.RFC3339) + "'",
	},
	{
		typ:      coltypes.TYPE_TIMEUUID,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"63176980-bfde-11d3-bc37-1c4d704231dc"},
		expected: "SELECT * FROM tbl WHERE pk0=63176980-bfde-11d3-bc37-1c4d704231dc",
	},
	{
		typ:      coltypes.TYPE_TINYINT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{1},
		expected: "SELECT * FROM tbl WHERE pk0=1",
	},
	{
		typ:      coltypes.TYPE_UUID,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"63176980-bfde-11d3-bc37-1c4d704231dc"},
		expected: "SELECT * FROM tbl WHERE pk0=63176980-bfde-11d3-bc37-1c4d704231dc",
	},
	{
		typ:      coltypes.TYPE_VARCHAR,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"a"},
		expected: "SELECT * FROM tbl WHERE pk0='a'",
	},
	{
		typ:      coltypes.TYPE_VARINT,
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{big.NewInt(1001)},
		expected: "SELECT * FROM tbl WHERE pk0=1001",
	},
	{
		typ: &coltypes.BagType{
			Kind:   "set",
			Type:   coltypes.TYPE_ASCII,
			Frozen: false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{[]string{"a", "b"}},
		expected: "SELECT * FROM tbl WHERE pk0={'a','b'}",
	},
	{
		typ: &coltypes.BagType{
			Kind:   "list",
			Type:   coltypes.TYPE_ASCII,
			Frozen: false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{[]string{"a", "b"}},
		expected: "SELECT * FROM tbl WHERE pk0=['a','b']",
	},
	{
		typ: &coltypes.MapType{
			KeyType:   coltypes.TYPE_ASCII,
			ValueType: coltypes.TYPE_ASCII,
			Frozen:    false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{map[string]string{"a": "b"}},
		expected: "SELECT * FROM tbl WHERE pk0={a:'b'}",
	},
	{
		typ: &coltypes.MapType{
			KeyType:   coltypes.TYPE_ASCII,
			ValueType: coltypes.TYPE_BLOB,
			Frozen:    false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{map[string]string{"a": "b"}},
		expected: "SELECT * FROM tbl WHERE pk0={a:textasblob('b')}",
	},
	{
		typ: &coltypes.TupleType{
			Types:  []coltypes.SimpleType{coltypes.TYPE_ASCII},
			Frozen: false,
		},
		query:    "SELECT * FROM tbl WHERE pk0=?",
		values:   []interface{}{"a"},
		expected: "SELECT * FROM tbl WHERE pk0='a'",
	},
	{
		typ: &coltypes.TupleType{
			Types:  []coltypes.SimpleType{coltypes.TYPE_ASCII, coltypes.TYPE_ASCII},
			Frozen: false,
		},
		query:    "SELECT * FROM tbl WHERE pk0={?,?}",
		values:   []interface{}{"a", "b"},
		expected: "SELECT * FROM tbl WHERE pk0={'a','b'}",
	},
}

func TestCQLPretty(t *testing.T) {
	for _, p := range prettytests {
		result, _ := p.typ.CQLPretty(p.query, p.values)
		if result != p.expected {
			t.Fatalf("expected '%s', got '%s' for values %v and type '%v'", p.expected, result, p.values, p.typ)
		}
	}
}
