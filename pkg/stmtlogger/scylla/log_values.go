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
	"cmp"
	"encoding/hex"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"fmt"
	"hash/maphash"
	"math"
	"math/big"
	"net"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/gocql/gocql"
	"github.com/samber/mo"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/stmtlogger"
)

// logDataSizeHint is the expected encoded size of the data cell of one row,
// used to size its buffer before encoding.
const logDataSizeHint = 256

// querySeed hashes CQL text to a query id. The _logs keyspace is created fresh
// for every run, so an id only has to be stable within one process.
var querySeed = maphash.MakeSeed()

// queryID returns the id under which the queries table stores stmt.
func queryID(stmt string) int64 {
	return int64(maphash.String(querySeed, stmt))
}

// queryDict holds the CQL text of every query id that the committers wrote. A
// row stores only the id, so the text of a statement crosses the network and
// the disk once per run instead of once per execution.
type queryDict struct {
	texts sync.Map // int64 -> string
}

// remember records stmt under id. It reports true the first time it sees id:
// the caller then writes the queries row. Two committers that race on a new id
// both write the same row, which is harmless.
func (d *queryDict) remember(id int64, stmt string) bool {
	if _, ok := d.texts.Load(id); ok {
		return false
	}

	_, loaded := d.texts.LoadOrStore(id, stmt)

	return !loaded
}

// text returns the CQL text of id, if a committer of this process wrote it.
func (d *queryDict) text(id int64) (string, bool) {
	if d == nil {
		return "", false
	}

	v, ok := d.texts.Load(id)
	if !ok {
		return "", false
	}

	return v.(string), true //nolint:forcetypeassert // only remember stores, and it stores strings
}

// logData is the data cell of one _logs row: everything about one execution
// that is not part of the primary key, as one JSON object. The committer binds
// it for every logged statement, so it is encoded in one pass into one buffer.
//
// The object holds the query id under "q" and the bound values as an array
// under "v". "t" holds the test cluster's observation and "o" the oracle's,
// each with "ts" (start, UTC nanoseconds), "h" (host), "e" (error, left out
// when empty) and "d" (duration, nanoseconds). A cluster that gemini did not
// observe for the execution has no key.
type logData []byte

// MarshalCQL returns the JSON text of the cell.
func (d logData) MarshalCQL(gocql.TypeInfo) ([]byte, error) {
	return d, nil
}

// encodeLogData encodes the data cell of the row that pair becomes.
func encodeLogData(pair logPair, id int64) logData {
	shared := pair.shared()

	buf := make([]byte, 0, logDataSizeHint)
	buf = append(buf, `{"q":`...)
	buf = strconv.AppendInt(buf, id, 10)
	buf = append(buf, `,"v":`...)
	buf = appendLogValues(buf, shared.Values)

	if pair.hasTest {
		buf = appendLogSide(append(buf, `,"t":`...), &pair.test)
	}

	if pair.hasOracle {
		buf = appendLogSide(append(buf, `,"o":`...), &pair.oracle)
	}

	return append(buf, '}')
}

func appendLogSide(buf []byte, item *stmtlogger.Item) []byte {
	buf = append(buf, `{"ts":`...)
	buf = strconv.AppendInt(buf, logTS(item.Start.Time), 10)
	buf = append(buf, `,"h":`...)
	buf = appendJSONString(buf, item.Host)

	if errStr := itemError(item); errStr != "" {
		buf = append(buf, `,"e":`...)
		buf = appendJSONString(buf, errStr)
	}

	buf = append(buf, `,"d":`...)
	buf = strconv.AppendInt(buf, int64(item.Duration.Duration), 10)

	return append(buf, '}')
}

func itemError(item *stmtlogger.Item) string {
	if item.Error.IsLeft() {
		if err := item.Error.MustLeft(); err != nil {
			return err.Error()
		}

		return ""
	}

	return item.Error.MustRight()
}

// appendLogValues appends the bound values as a JSON array, or null when the
// statement has none. Raw bytes become an array of one element: the bytes
// themselves when they are valid JSON, or else a JSON string.
func appendLogValues(buf []byte, values mo.Either[[]any, []byte]) []byte {
	if values.IsRight() {
		raw := values.MustRight()
		buf = append(buf, '[')
		if jsontext.Value(raw).IsValid() {
			buf = append(buf, raw...)
		} else {
			buf = appendJSONString(buf, string(raw))
		}

		return append(buf, ']')
	}

	vals := values.MustLeft()
	if vals == nil {
		return append(buf, "null"...)
	}

	return appendJSONArray(buf, vals)
}

func appendJSONArray(buf []byte, vals []any) []byte {
	buf = append(buf, '[')
	for i, val := range vals {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = appendJSONValue(buf, val)
	}

	return append(buf, ']')
}

// appendJSONValue appends the JSON form of one bound value. The CQL types that
// gemini generates skip reflection:
//   - blob is a string with the 0x prefix of a CQL blob literal
//   - uuid, timeuuid and inet are strings
//   - timestamp and date are RFC 3339 strings, time and duration are nanoseconds
//   - varint and decimal are JSON numbers with every digit
//   - a float that JSON cannot hold (NaN, ±Inf) is a string
//   - a map is an object with sorted keys, a list, set or tuple is an array
func appendJSONValue(buf []byte, val any) []byte {
	if out, ok := appendJSONNumber(buf, val); ok {
		return out
	}

	switch v := val.(type) {
	case nil:
		return append(buf, "null"...)
	case string:
		return appendJSONString(buf, v)
	case []byte:
		return append(hex.AppendEncode(append(buf, `"0x`...), v), '"')
	case bool:
		return strconv.AppendBool(buf, v)
	case gocql.UUID:
		return append(appendUUID(append(buf, '"'), v), '"')
	case net.IP:
		return append(append(append(buf, '"'), v.String()...), '"')
	case time.Time:
		return append(v.AppendFormat(append(buf, '"'), time.RFC3339Nano), '"')
	case *inf.Dec:
		if v == nil {
			return append(buf, "null"...)
		}

		return append(buf, v.String()...)
	case []any:
		if v == nil {
			return append(buf, "null"...)
		}

		return appendJSONArray(buf, v)
	case map[string]any:
		if v == nil {
			return append(buf, "null"...)
		}

		return appendJSONStringMap(buf, v)
	default:
		return appendJSONReflect(buf, val)
	}
}

// appendJSONNumber appends the integer, float, duration and varint values. It
// reports false for any other type.
func appendJSONNumber(buf []byte, val any) ([]byte, bool) {
	switch v := val.(type) {
	case int:
		return strconv.AppendInt(buf, int64(v), 10), true
	case int8:
		return strconv.AppendInt(buf, int64(v), 10), true
	case int16:
		return strconv.AppendInt(buf, int64(v), 10), true
	case int32:
		return strconv.AppendInt(buf, int64(v), 10), true
	case int64:
		return strconv.AppendInt(buf, v, 10), true
	case uint8:
		return strconv.AppendUint(buf, uint64(v), 10), true
	case uint16:
		return strconv.AppendUint(buf, uint64(v), 10), true
	case uint32:
		return strconv.AppendUint(buf, uint64(v), 10), true
	case uint64:
		return strconv.AppendUint(buf, v, 10), true
	case float32:
		return appendJSONFloat(buf, float64(v), 32), true
	case float64:
		return appendJSONFloat(buf, v, 64), true
	case time.Duration:
		return strconv.AppendInt(buf, int64(v), 10), true
	case *big.Int:
		if v == nil {
			return append(buf, "null"...), true
		}

		return v.Append(buf, 10), true
	default:
		return buf, false
	}
}

// appendUUID appends the canonical 8-4-4-4-12 text of u, as u.String() gives,
// without the allocation of an intermediate string.
func appendUUID(buf []byte, u gocql.UUID) []byte {
	buf = hex.AppendEncode(buf, u[0:4])
	buf = hex.AppendEncode(append(buf, '-'), u[4:6])
	buf = hex.AppendEncode(append(buf, '-'), u[6:8])
	buf = hex.AppendEncode(append(buf, '-'), u[8:10])

	return hex.AppendEncode(append(buf, '-'), u[10:16])
}

func appendJSONString(buf []byte, s string) []byte {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, string(utf8.RuneError))
	}

	out, err := jsontext.AppendQuote(buf, s)
	if err != nil {
		return strconv.AppendQuote(buf, s)
	}

	return out
}

func appendJSONFloat(buf []byte, f float64, bitSize int) []byte {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return append(append(append(buf, '"'), strconv.FormatFloat(f, 'g', -1, bitSize)...), '"')
	}

	return strconv.AppendFloat(buf, f, 'g', -1, bitSize)
}

// appendJSONStringMap appends a UDT value: an object with sorted keys.
func appendJSONStringMap(buf []byte, m map[string]any) []byte {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	buf = append(buf, '{')
	for i, k := range keys {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = appendJSONString(buf, k)
		buf = append(buf, ':')
		buf = appendJSONValue(buf, m[k])
	}

	return append(buf, '}')
}

// appendJSONReflect appends a value of a type that the fast cases do not name:
// a typed CQL map or a typed slice. Any other type goes to encoding/json, and a
// value that it rejects becomes a JSON string of its Go syntax.
func appendJSONReflect(buf []byte, val any) []byte {
	rv := reflect.ValueOf(val)

	switch rv.Kind() {
	case reflect.Map:
		if rv.IsNil() {
			return append(buf, "null"...)
		}

		return appendJSONMap(buf, rv)
	case reflect.Slice:
		if rv.IsNil() {
			return append(buf, "null"...)
		}

		return appendJSONList(buf, rv)
	case reflect.Array:
		return appendJSONList(buf, rv)
	default:
		data, err := json.Marshal(val, json.Deterministic(true))
		if err != nil {
			return appendJSONString(buf, fmt.Sprintf("%#v", val))
		}

		return append(buf, data...)
	}
}

func appendJSONList(buf []byte, rv reflect.Value) []byte {
	buf = append(buf, '[')
	for i := range rv.Len() {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = appendJSONValue(buf, rv.Index(i).Interface())
	}

	return append(buf, ']')
}

// appendJSONMap appends a typed CQL map as an object. A JSON object key is a
// string, so a key whose JSON form is not a string becomes the string of that
// JSON text. Keys are sorted by their string.
func appendJSONMap(buf []byte, rv reflect.Value) []byte {
	type entry struct {
		val any
		key string
	}

	entries := make([]entry, 0, rv.Len())
	var scratch []byte

	iter := rv.MapRange()
	for iter.Next() {
		scratch = appendJSONValue(scratch[:0], iter.Key().Interface())

		key := string(scratch)
		if len(scratch) > 0 && scratch[0] == '"' {
			var unquoted string
			if err := json.Unmarshal(scratch, &unquoted); err == nil {
				key = unquoted
			}
		}

		entries = append(entries, entry{key: key, val: iter.Value().Interface()})
	}

	slices.SortFunc(entries, func(a, b entry) int { return cmp.Compare(a.key, b.key) })

	buf = append(buf, '{')
	for i, e := range entries {
		if i > 0 {
			buf = append(buf, ',')
		}
		buf = appendJSONString(buf, e.key)
		buf = append(buf, ':')
		buf = appendJSONValue(buf, e.val)
	}

	return append(buf, '}')
}
