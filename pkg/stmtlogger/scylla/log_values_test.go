// Copyright 2026 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scylla

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"math"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/stmtlogger"
)

func TestAppendJSONValue(t *testing.T) {
	t.Parallel()

	uuid, err := gocql.ParseUUID("8b2a5a2e-1c1f-11f1-8000-000000000001")
	require.NoError(t, err)
	ts := time.Date(2026, 11, 14, 10, 0, 0, 5, time.UTC)

	tests := []struct {
		value any
		name  string
		want  string
	}{
		{name: "nil", value: nil, want: `null`},
		{name: "string", value: "a\"b\n", want: `"a\"b\n"`},
		{name: "invalid utf8", value: "a\xffb", want: `"a` + "�" + `b"`},
		{name: "blob", value: []byte{0xde, 0xad}, want: `"0xdead"`},
		{name: "empty blob", value: []byte{}, want: `"0x"`},
		{name: "bool", value: true, want: `true`},
		{name: "ints", value: []any{int8(-1), int16(2), int32(-3), int64(4), 5}, want: `[-1,2,-3,4,5]`},
		{name: "float", value: 1.5, want: `1.5`},
		{name: "float32", value: float32(2.25), want: `2.25`},
		{name: "nan", value: math.NaN(), want: `"NaN"`},
		{name: "inf", value: math.Inf(-1), want: `"-Inf"`},
		{name: "uuid", value: uuid, want: `"8b2a5a2e-1c1f-11f1-8000-000000000001"`},
		{name: "duration", value: 3 * time.Second, want: `3000000000`},
		{name: "time", value: ts, want: `"2026-11-14T10:00:00.000000005Z"`},
		{name: "varint", value: big.NewInt(-12345678901234), want: `-12345678901234`},
		{name: "nil varint", value: (*big.Int)(nil), want: `null`},
		{name: "decimal", value: inf.NewDec(31415, 4), want: `3.1415`},
		{name: "nested list", value: []any{[]any{"x", nil}, []any(nil)}, want: `[["x",null],null]`},
		{name: "udt", value: map[string]any{"b": 2, "a": "x"}, want: `{"a":"x","b":2}`},
		{name: "map bool keys", value: map[bool]string{true: "y", false: "n"}, want: `{"false":"n","true":"y"}`},
		{name: "map uuid keys", value: map[gocql.UUID]time.Duration{uuid: time.Millisecond}, want: `{"8b2a5a2e-1c1f-11f1-8000-000000000001":1000000}`},
		{name: "map int keys", value: map[int32]bool{10: true, 9: false}, want: `{"10":true,"9":false}`},
		{name: "typed slice", value: []string{"a", "b"}, want: `["a","b"]`},
		{name: "inet", value: net.IP{10, 0, 0, 1}, want: `"10.0.0.1"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := appendJSONValue(nil, tt.value)
			assert.Equal(t, tt.want, string(got))
			assert.True(t, jsontext.Value(got).IsValid(), "must be valid JSON: %s", got)
		})
	}
}

func TestAppendUUIDMatchesString(t *testing.T) {
	t.Parallel()

	for range 100 {
		u := gocql.TimeUUID()
		assert.Equal(t, u.String(), string(appendUUID(nil, u)))
	}
}

func TestEncodeLogData(t *testing.T) {
	t.Parallel()

	test := stmtlogger.Item{
		Type:     stmtlogger.TypeTest,
		Start:    stmtlogger.Time{Time: time.Unix(0, 100)},
		Host:     "10.0.0.1:9042",
		Error:    mo.Left[error, string](errors.New("timeout")),
		Duration: stmtlogger.Duration{Duration: 5 * time.Millisecond},
		Values:   mo.Left[[]any, []byte]([]any{"k", int64(7)}),
	}
	oracle := test
	oracle.Type = stmtlogger.TypeOracle
	oracle.Start = stmtlogger.Time{Time: time.Unix(0, 90)}
	oracle.Host = "10.0.0.2:9042"
	oracle.Error = mo.Right[error, string]("")

	t.Run("both clusters", func(t *testing.T) {
		t.Parallel()

		pair := pairOf(test)
		require.True(t, pair.set(oracle))

		var record logRecord
		data := encodeLogData(pair, 42)
		require.NoError(t, json.Unmarshal(data, &record), "%s", data)

		assert.Equal(t, int64(42), record.Query)
		assert.JSONEq(t, `["k",7]`, string(record.Values))
		require.NotNil(t, record.Test)
		require.NotNil(t, record.Oracle)
		assert.Equal(t, logSide{TS: 100, Host: "10.0.0.1:9042", Error: "timeout", Duration: int64(5 * time.Millisecond)}, *record.Test)
		assert.Equal(t, logSide{TS: 90, Host: "10.0.0.2:9042", Duration: int64(5 * time.Millisecond)}, *record.Oracle)
		assert.NotContains(t, string(data), `"e":""`, "an empty error must be left out")
	})

	t.Run("one cluster", func(t *testing.T) {
		t.Parallel()

		var record logRecord
		require.NoError(t, json.Unmarshal(encodeLogData(pairOf(oracle), 1), &record))
		assert.Nil(t, record.Test, "an unobserved cluster must have no key")
		assert.NotNil(t, record.Oracle)
	})

	t.Run("raw values", func(t *testing.T) {
		t.Parallel()

		item := test
		item.Values = mo.Right[[]any, []byte]([]byte(`{"a":1}`))

		var record logRecord
		require.NoError(t, json.Unmarshal(encodeLogData(pairOf(item), 1), &record))
		assert.JSONEq(t, `[{"a":1}]`, string(record.Values))

		item.Values = mo.Right[[]any, []byte]([]byte("not json"))
		require.NoError(t, json.Unmarshal(encodeLogData(pairOf(item), 1), &record))
		assert.JSONEq(t, `["not json"]`, string(record.Values))
	})

	t.Run("no values", func(t *testing.T) {
		t.Parallel()

		item := test
		item.Values = mo.Left[[]any, []byte](nil)

		var record logRecord
		require.NoError(t, json.Unmarshal(encodeLogData(pairOf(item), 1), &record))
		assert.Equal(t, "null", string(record.Values))
	})
}

func TestQueryDict(t *testing.T) {
	t.Parallel()

	var d queryDict

	id := queryID("INSERT INTO ks.t (a) VALUES (?)")
	assert.Equal(t, id, queryID("INSERT INTO ks.t (a) VALUES (?)"), "an id must be stable within a process")
	assert.NotEqual(t, id, queryID("INSERT INTO ks.t (b) VALUES (?)"))

	assert.True(t, d.remember(id, "INSERT INTO ks.t (a) VALUES (?)"), "the first sight must write the queries row")
	assert.False(t, d.remember(id, "INSERT INTO ks.t (a) VALUES (?)"), "a known id must not write it again")

	text, ok := d.text(id)
	assert.True(t, ok)
	assert.Equal(t, "INSERT INTO ks.t (a) VALUES (?)", text)

	var none *queryDict
	_, ok = none.text(id)
	assert.False(t, ok, "a nil dictionary must report a miss, not panic")
}

func TestAppendStatementEntry(t *testing.T) {
	t.Parallel()

	side := &logSide{TS: 100, Host: "10.0.0.1:9042", Error: "boom", Duration: int64(1500 * time.Microsecond)}

	entry := appendStatementEntry(nil, side, "INSERT INTO ks.t (a) VALUES (?)", jsontext.Value(`["k"]`), 7, 2, 1)
	require.True(t, jsontext.Value(entry).IsValid(), "%s", entry)
	assert.JSONEq(t, `{"ts":100,"seq":7,"statement":"INSERT INTO ks.t (a) VALUES (?)","values":["k"],`+
		`"host":"10.0.0.1:9042","attempt":2,"gemini_attempt":1,"error":"boom","dur":"1.5ms"}`, string(entry))

	side.Error = ""
	entry = appendStatementEntry(nil, side, "q", nil, 1, 1, 0)
	assert.JSONEq(t, `{"ts":100,"seq":1,"statement":"q","values":null,"host":"10.0.0.1:9042",`+
		`"attempt":1,"gemini_attempt":0,"dur":"1.5ms"}`, string(entry))

	// The summary reads these fields back from the statement files.
	var parsed struct {
		Statement string `json:"statement"`
		Host      string `json:"host"`
		TS        int64  `json:"ts"`
	}
	require.NoError(t, json.Unmarshal(entry, &parsed))
	assert.Equal(t, "q", parsed.Statement)
	assert.Equal(t, int64(100), parsed.TS)
}

func BenchmarkEncodeLogData(b *testing.B) {
	item := stmtlogger.Item{
		Type:     stmtlogger.TypeTest,
		Start:    stmtlogger.Time{Time: time.Now()},
		Host:     "10.0.0.1:9042",
		Error:    mo.Right[error, string](""),
		Duration: stmtlogger.Duration{Duration: time.Millisecond},
		Values: mo.Left[[]any, []byte]([]any{
			[]any{"set element one", "set element two", "three"},
			int64(42), "some text value", gocql.TimeUUID(), int32(7),
			[]byte{1, 2, 3, 4, 5, 6, 7, 8},
			true, time.Now(), int16(3),
			map[bool]string{true: "a", false: "b"},
		}),
	}
	oracle := item
	oracle.Type = stmtlogger.TypeOracle

	pair := pairOf(item)
	pair.set(oracle)

	b.ReportAllocs()
	for b.Loop() {
		_ = encodeLogData(pair, 42)
	}
}
