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
	"fmt"
	"io"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/testutils"
)

var ErrNoPartitionKeyValues = errors.New("no partition keys available")

var SingleToDoubleQuoteReplacer = strings.NewReplacer("'", "\"")

type Random interface {
	Uint32() uint32
	IntN(int) int
	Int64() int64
	Uint64() uint64
	Uint64N(uint64) uint64
	Int64N(int64) int64
	Float64() float64
}

type QueryContextKey string

type MemoryFootprint interface {
	MemoryFootprint() uint64
}

var maxDateMs = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC).UTC().UnixMilli()

// RandDateStr generates time in string representation
// it is done in such way because we wanted to make JSON statement to work
// but scylla supports only string representation of date in JSON format
func RandDateStr(rnd Random) time.Time {
	return time.UnixMilli(rnd.Int64N(maxDateMs)).UTC()
}

// RandTimestamp generates timestamp in nanoseconds
// Date limit needed to make sure that textual representation of the date is parsable by cql and drivers
// Currently CQL fails: unable to parse date '95260-10-10T19:09:07.148+0000': marshaling error: Unable to parse timestamp from '95260-10-10t19:09:07.148+0000'"
// if year is bigger than 9999, same as golang time.Parse
func RandTimestamp(rnd Random) int64 {
	return rnd.Int64N(maxDateMs)
}

func RandDate(rnd Random) time.Time {
	return time.Unix(rnd.Int64N(1<<63-2), rnd.Int64N(999999999)).UTC()
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
	return minimum + rnd.IntN(maximum-minimum)
}

func IgnoreError(fn func() error) {
	_ = fn()
}

func UUIDFromTime(rnd Random) gocql.UUID {
	if testutils.IsUnderTest() {
		return gocql.TimeUUIDWith(rnd.Int64(), 0, []byte("127.0.0.1"))
	}
	return gocql.UUIDFromTime(RandDate(rnd))
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
			return nil, errors.Wrapf(err, "failed to open file %s", input)
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

//nolint:gosec
/*#nosec G103*/
func UnsafeBytes(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}

//nolint:gosec
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
