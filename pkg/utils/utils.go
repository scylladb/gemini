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

package utils

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/gocql/gocql"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/testutils"
)

var ErrNoPartitionKeyValues = errors.New("no partition keys available")

var SingleToDoubleQuoteReplacer = strings.NewReplacer("'", "\"")

type Random interface {
	Uint32() uint32
	IntN(n int) int
	Int64() int64
	Uint64() uint64
	Uint64N(n uint64) uint64
	Int64N(n int64) int64
	Float64() float64
}

type QueryContextKey string

type MemoryFootprint interface {
	MemoryFootprint() uint64
}

var maxDateMs = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC).UTC().UnixMilli()

// RandTimestamp generates timestamp in nanoseconds
// Date limit needed to make sure that textual representation of the date is parsable by cql and drivers
// Currently CQL fails: unable to parse date '95260-10-10T19:09:07.148+0000': marshaling error: Unable to parse timestamp from '95260-10-10t19:09:07.148+0000'"
// if year is bigger than 9999, same as golang time.Parse
func RandTimestamp(rnd Random) int64 {
	return rnd.Int64N(maxDateMs)
}

func RandDate(rnd Random) time.Time {
	t := time.UnixMilli(rnd.Int64N(maxDateMs)).UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func RandDuration(rnd Random) time.Duration {
	// Duration is a signed int64 nanoseconds, so we can use the same range as for time
	// but we need to make sure that it is not negative, so we use Int64N with 2^63-1
	return time.Duration(rnd.Int64N(math.MaxInt64))
}

// RandTime - According to the CQL binary protocol, time is an int64 in range [0;86399999999999]
// https://github.com/apache/cassandra/blob/f5df4b219e063cb24b9cc0c22b6e614506b8d903/doc/native_protocol_v4.spec#L941
// An 8 byte two's complement long representing nanoseconds since midnight.
// Valid values are in the range 0 to 86399999999999
func RandTime(rnd Random) int64 {
	return rnd.Int64N(86400000000000)
}

func RandIPV4Address(rnd Random, v, pos int) string {
	if pos < 0 || pos > 4 {
		panic(
			fmt.Sprintf(
				"invalid position for the desired value of the IP part %d, 0-3 supported",
				pos,
			),
		)
	}
	if v < 0 || v > 255 {
		panic(
			fmt.Sprintf("invalid value for the desired position %d of the IP, 0-255 suppoerted", v),
		)
	}
	var blocks []string
	for i := range 4 {
		if i == pos {
			blocks = append(blocks, strconv.Itoa(v))
		} else {
			blocks = append(blocks, strconv.Itoa(rnd.IntN(255)))
		}
	}
	return strings.Join(blocks, ".")
}

func RandInt2(rnd Random, minimum, maximum int) int {
	if maximum <= minimum {
		return minimum
	}
	num := rnd.IntN(maximum - minimum)
	return minimum + num
}

func IgnoreError(fn func() error) {
	_ = fn()
}

// uuidTimeBase is the start of the version 1 UUID clock, 15 October 1582.
var uuidTimeBase = time.Date(1582, time.October, 15, 0, 0, 0, 0, time.UTC).Unix()

// UUIDFromTime returns a version 1 UUID for a random date. Every part comes
// from rnd: the clock sequence and the node are random instead of a global
// counter and the host's MAC address. The partition pool regenerates key values
// from a seed, so the same rnd state must give the same UUID.
func UUIDFromTime(rnd Random) gocql.UUID {
	if testutils.IsUnderTest() {
		return gocql.TimeUUIDWith(rnd.Int64(), 0, []byte("127.0.0.1"))
	}

	date := RandDate(rnd)
	ts := (date.Unix()-uuidTimeBase)*1e7 + int64(date.Nanosecond()/100)
	clock := uint32(rnd.Uint64N(1 << 14))

	var node [8]byte
	binary.LittleEndian.PutUint64(node[:], rnd.Uint64())

	return gocql.TimeUUIDWith(ts, clock, node[:6])
}

// RandomUUID returns a version 4 UUID whose bits all come from rnd. The
// partition pool regenerates key values from a seed, so the same rnd state
// must give the same UUID.
func RandomUUID(rnd Random) gocql.UUID {
	var u gocql.UUID
	binary.LittleEndian.PutUint64(u[:8], rnd.Uint64())
	binary.LittleEndian.PutUint64(u[8:], rnd.Uint64())
	u[6] = u[6]&0x0F | 0x40 // version 4
	u[8] = u[8]&0x3F | 0x80 // IETF variant

	return u
}

func CreateFile(input string, closeOnExit bool, def ...io.Writer) (io.Writer, error) {
	switch input {
	case "":
		if len(def) > 0 && def[0] != nil {
			return def[0], nil
		}

		return io.Discard, nil
	case "stderr":
		return os.Stderr, nil
	case "stdout":
		return os.Stdout, nil
	default:
		w, err := os.OpenFile(input, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %w", input, err)
		}

		if closeOnExit {
			AddFinalizer(func() {
				IgnoreError(w.Sync)
				IgnoreError(w.Close)
			})
		}

		return w, nil
	}
}

// UnsafeBytes returns the bytes of s without a copy. The caller must not modify them.
//
/*#nosec G103*/
func UnsafeBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

// UnsafeString returns b as a string without a copy. The caller must not modify b after the call.
//
/*#nosec G103*/
func UnsafeString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func Sizeof(v any) uint64 {
	if v == nil {
		return 0
	}

	switch val := v.(type) {
	case MemoryFootprint:
		return val.MemoryFootprint()
	case []any:
		var size uint64
		for _, value := range val {
			size += Sizeof(value)
		}
		return size
	case string:
		return uint64(len(val)) + uint64(unsafe.Sizeof(""))
	case []byte:
		return uint64(len(val)) + uint64(unsafe.Sizeof([]byte{}))
	case int, int8, int16, int32, int64,
		uint, uint8, uint16, uint32, uint64,
		float32, float64, bool, time.Duration:
		return uint64(unsafe.Sizeof(v))
	case *big.Int:
		return uint64(val.BitLen()/8 + 1)
	case *inf.Dec:
		return uint64(val.UnscaledBig().BitLen()/8+1) + uint64(unsafe.Sizeof(int32(0)))
	default:
		return uint64(unsafe.Sizeof(v))
	}
}

// MarshalJSONUnchecked returns the JSON encoding of v, or a JSON string that
// holds the marshal error. Call it only from String, Error and ToCQL, which
// cannot return an error.
func MarshalJSONUnchecked(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		return marshalFailure(err)
	}

	return data
}

// goSyntaxKeyPrefix starts the key of a value that JSON cannot encode.
// json.Marshal escapes every '<' as \u003c, so no encodable value gives a
// key that contains a raw '<'.
const goSyntaxKeyPrefix = "<go>"

// MarshalJSONKey returns the JSON encoding of v for use as a hash input, a sort
// key or a row in a report. JSON cannot encode every value, for example a NaN
// float. For such a value, MarshalJSONKey returns a JSON string that holds
// goSyntaxKeyPrefix and v in Go syntax. Two different values thus give two
// different results, and the result is always valid JSON.
func MarshalJSONKey(v any) []byte {
	data, err := json.Marshal(v)
	if err == nil {
		return data
	}

	quoted := MarshalJSONUnchecked(fmt.Sprintf("%#v", v))
	key := make([]byte, 0, len(quoted)+len(goSyntaxKeyPrefix))
	key = append(key, '"')
	key = append(key, goSyntaxKeyPrefix...)

	return append(key, quoted[1:]...)
}

func marshalFailure(cause error) []byte {
	data, err := json.Marshal("json marshal failed: " + cause.Error())
	if err != nil {
		return []byte(`"json marshal failed"`)
	}

	return data
}
