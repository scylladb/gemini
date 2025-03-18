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
	"encoding/hex"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/gocql/gocql"
)

var maxDateMs = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC).UTC().UnixMilli()

// RandDateStr generates time in string representation
// it is done in such way because we wanted to make JSON statement to work
// but scylla supports only string representation of date in JSON format
func RandDateStr(rnd *rand.Rand) string {
	return time.UnixMilli(rnd.Int64N(maxDateMs)).UTC().Format("2006-01-02")
}

// RandTimestamp generates timestamp in nanoseconds
// Date limit needed to make sure that textual representation of the date is parsable by cql and drivers
// Currently CQL fails: unable to parse date '95260-10-10T19:09:07.148+0000': marshaling error: Unable to parse timestamp from '95260-10-10t19:09:07.148+0000'"
// if year is bigger than 9999, same as golang time.Parse
func RandTimestamp(rnd *rand.Rand) int64 {
	return rnd.Int64N(maxDateMs)
}

func RandDate(rnd *rand.Rand) time.Time {
	return time.Unix(rnd.Int64N(1<<63-2), rnd.Int64N(999999999)).UTC()
}

// According to the CQL binary protocol, time is an int64 in range [0;86399999999999]
// https://github.com/apache/cassandra/blob/f5df4b219e063cb24b9cc0c22b6e614506b8d903/doc/native_protocol_v4.spec#L941
// An 8 byte two's complement long representing nanoseconds since midnight.
// Valid values are in the range 0 to 86399999999999
func RandTime(rnd *rand.Rand) int64 {
	return rnd.Int64N(86400000000000)
}

func RandIPV4Address(rnd *rand.Rand, v, pos int) string {
	if pos < 0 || pos > 4 {
		panic(fmt.Sprintf("invalid position for the desired value of the IP part %d, 0-3 supported", pos))
	}
	if v < 0 || v > 255 {
		panic(fmt.Sprintf("invalid value for the desired position %d of the IP, 0-255 suppoerted", v))
	}
	var blocks []string
	for i := 0; i < 4; i++ {
		if i == pos {
			blocks = append(blocks, strconv.Itoa(v))
		} else {
			blocks = append(blocks, strconv.Itoa(rnd.IntN(255)))
		}
	}
	return strings.Join(blocks, ".")
}

func RandInt2(rnd *rand.Rand, minimum, maximum int) int {
	if maximum <= minimum {
		return minimum
	}
	return minimum + rnd.IntN(maximum-minimum)
}

func IgnoreError(fn func() error) {
	_ = fn()
}

func RandString(rnd *rand.Rand, ln int) string {
	length := ln
	if length%4 != 0 {
		length += 4 - (length % 4)
	}

	binBuff := make([]byte, length)

	for i := 0; i < len(binBuff); i += 4 {
		binary.LittleEndian.PutUint32(binBuff[i:], rnd.Uint32())
	}

	return hex.EncodeToString(binBuff)[:ln]
}

func UUIDFromTime(rnd *rand.Rand) string {
	if IsUnderTest() {
		return gocql.TimeUUIDWith(rnd.Int64(), 0, []byte("127.0.0.1")).String()
	}
	return gocql.UUIDFromTime(RandDate(rnd)).String()
}

func CreateFile(input string, def ...io.Writer) (io.Writer, error) {
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

		runtime.SetFinalizer(w, func(f *os.File) {
			IgnoreError(f.Sync)
			IgnoreError(f.Close)
		})

		return w, nil
	}
}
