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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/samber/mo"
	"gopkg.in/inf.v0"
)

// logValueSizeHint is the expected encoded size of one value, used to size the
// buffer of the values column before encoding.
const logValueSizeHint = 24

// logValues is the values column of a log row, already encoded in the CQL wire
// format of a frozen<list<text>>. The committer binds it for every logged
// statement. Encoding it in one pass into one buffer skips the []string of
// formatted values and the growing buffer that gocql would build from it.
type logValues []byte

// MarshalCQL returns the encoded list. A nil logValues encodes as CQL null.
func (v logValues) MarshalCQL(gocql.TypeInfo) ([]byte, error) {
	return v, nil
}

// encodeLogValues encodes values as a frozen<list<text>>. Every element is the
// text that formatValueFast gives for that value. Raw bytes become a list of
// one element.
func encodeLogValues(values mo.Either[[]any, []byte]) logValues {
	if values.IsRight() {
		raw := values.MustRight()
		buf := make([]byte, 0, 8+len(raw))
		buf = binary.BigEndian.AppendUint32(buf, 1)
		buf = binary.BigEndian.AppendUint32(buf, uint32(len(raw)))

		return append(buf, raw...)
	}

	vals := values.MustLeft()
	if vals == nil {
		return nil
	}

	buf := make([]byte, 0, 4+len(vals)*(4+logValueSizeHint))
	buf = binary.BigEndian.AppendUint32(buf, uint32(len(vals)))

	for _, val := range vals {
		lenAt := len(buf)
		buf = append(buf, 0, 0, 0, 0)
		buf = appendValueFast(buf, val)
		binary.BigEndian.PutUint32(buf[lenAt:], uint32(len(buf)-lenAt-4))
	}

	return buf
}

// appendValueFast appends the text form of a value to buf. The common CQL types
// skip reflection. Every other type uses fmt with the %#v verb.
func appendValueFast(buf []byte, val any) []byte {
	if val == nil {
		return append(buf, "<nil>"...)
	}

	switch v := val.(type) {
	case string:
		return strconv.AppendQuote(buf, v)
	case []byte:
		return hex.AppendEncode(append(buf, "0x"...), v)
	case bool:
		return strconv.AppendBool(buf, v)
	case int:
		return strconv.AppendInt(buf, int64(v), 10)
	case int8:
		return strconv.AppendInt(buf, int64(v), 10)
	case int16:
		return strconv.AppendInt(buf, int64(v), 10)
	case int32:
		return strconv.AppendInt(buf, int64(v), 10)
	case int64:
		return strconv.AppendInt(buf, v, 10)
	case float32:
		return strconv.AppendFloat(buf, float64(v), 'g', -1, 32)
	case float64:
		return strconv.AppendFloat(buf, v, 'g', -1, 64)
	case gocql.UUID:
		return append(buf, v.String()...)
	case time.Duration:
		return strconv.AppendInt(buf, int64(v), 10)
	case time.Time:
		return append(buf, v.GoString()...)
	case *big.Int:
		return v.Append(buf, 10)
	case *inf.Dec:
		return append(buf, v.String()...)
	case []any:
		return appendAnySliceGoSyntax(buf, v)
	default:
		return fmt.Appendf(buf, "%#v", v)
	}
}

// appendAnySliceGoSyntax appends what fmt prints for v with the %#v verb. Lists
// and sets arrive as []any, and fmt formats them with reflection. The layout is
// the slice type, then every element in its own %#v form, comma separated.
func appendAnySliceGoSyntax(buf []byte, v []any) []byte {
	if v == nil {
		return append(buf, "[]interface {}(nil)"...)
	}

	buf = append(buf, "[]interface {}{"...)
	for i, elem := range v {
		if i > 0 {
			buf = append(buf, ", "...)
		}
		buf = appendElemGoSyntax(buf, elem)
	}

	return append(buf, '}')
}

// appendElemGoSyntax appends what fmt prints for one element of a []any with
// the %#v verb. It skips reflection only for the types whose %#v text it
// reproduces exactly.
func appendElemGoSyntax(buf []byte, elem any) []byte {
	switch e := elem.(type) {
	case nil:
		return append(buf, "interface {}(nil)"...)
	case string:
		return strconv.AppendQuote(buf, e)
	case bool:
		return strconv.AppendBool(buf, e)
	case int:
		return strconv.AppendInt(buf, int64(e), 10)
	case int8:
		return strconv.AppendInt(buf, int64(e), 10)
	case int16:
		return strconv.AppendInt(buf, int64(e), 10)
	case int32:
		return strconv.AppendInt(buf, int64(e), 10)
	case int64:
		return strconv.AppendInt(buf, e, 10)
	case time.Duration:
		return strconv.AppendInt(buf, int64(e), 10)
	case time.Time:
		return append(buf, e.GoString()...)
	case []byte:
		// Inside a slice fmt names the element type []uint8, not []byte.
		if e == nil {
			return append(buf, "[]uint8(nil)"...)
		}
		buf = append(buf, "[]uint8{"...)
		for i, b := range e {
			if i > 0 {
				buf = append(buf, ", "...)
			}
			buf = strconv.AppendUint(append(buf, "0x"...), uint64(b), 16)
		}

		return append(buf, '}')
	default:
		return fmt.Appendf(buf, "%#v", e)
	}
}
