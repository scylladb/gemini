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
	"encoding/hex"
	"fmt"
	"github.com/pkg/errors"
	"golang.org/x/exp/constraints"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"golang.org/x/exp/rand"
)

var maxDateMs = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC).UTC().UnixMilli()

// RandDateStr generates time in string representation
// it is done in such way because we wanted to make JSON statement to work
// but scylla supports only string representation of date in JSON format
func RandDateStr(rnd *rand.Rand) string {
	return time.UnixMilli(rnd.Int63n(maxDateMs)).UTC().Format("2006-01-02")
}

// RandTimestamp generates timestamp in nanoseconds
// Date limit needed to make sure that textual representation of the date is parsable by cql and drivers
// Currently CQL fails: unable to parse date '95260-10-10T19:09:07.148+0000': marshaling error: Unable to parse timestamp from '95260-10-10t19:09:07.148+0000'"
// if year is bigger than 9999, same as golang time.Parse
func RandTimestamp(rnd *rand.Rand) int64 {
	return rnd.Int63n(maxDateMs)
}

func RandDate(rnd *rand.Rand) time.Time {
	return time.Unix(rnd.Int63n(1<<63-2), rnd.Int63n(999999999)).UTC()
}

// According to the CQL binary protocol, time is an int64 in range [0;86399999999999]
// https://github.com/apache/cassandra/blob/f5df4b219e063cb24b9cc0c22b6e614506b8d903/doc/native_protocol_v4.spec#L941
// An 8 byte two's complement long representing nanoseconds since midnight.
// Valid values are in the range 0 to 86399999999999
func RandTime(rnd *rand.Rand) int64 {
	return rnd.Int63n(86400000000000)
}

func ipV4Builder[T constraints.Integer](bytes [4]T) string {
	var builder strings.Builder
	builder.Grow(16) // Maximum length of an IPv4 address

	for _, b := range bytes {
		builder.WriteString(strconv.FormatUint(uint64(b), 10))
		builder.WriteRune('.')
	}

	return builder.String()[:builder.Len()-1]
}

func RandIPV4Address(rnd *rand.Rand) string {
	return ipV4Builder([4]int{rnd.Intn(256), rnd.Intn(256), rnd.Intn(256), rnd.Intn(256)})
}

func RandIPV4AddressPositional(rnd *rand.Rand, v, pos int) string {
	if pos < 0 || pos > 4 {
		panic(fmt.Sprintf("invalid position for the desired value of the IP part %d, 0-3 supported", pos))
	}
	if v < 0 || v > 255 {
		panic(fmt.Sprintf("invalid value for the desired position %d of the IP, 0-255 suppoerted", v))
	}

	data := [4]int{rnd.Intn(255), rnd.Intn(255), rnd.Intn(255), rnd.Intn(255)}
	data[pos] = v

	return ipV4Builder(data)
}

func RandInt2(rnd *rand.Rand, min, max int) int {
	if max <= min {
		return min
	}
	return min + rnd.Intn(max-min)
}

func IgnoreError(fn func() error) {
	_ = fn()
}

func RandString(rnd *rand.Rand, ln int) string {
	buffLen := ln
	if buffLen > 32 {
		buffLen = 32
	}
	binBuff := make([]byte, buffLen/2+1)
	_, _ = rnd.Read(binBuff)
	buff := hex.EncodeToString(binBuff)[:buffLen]
	if ln <= 32 {
		return buff
	}
	out := make([]byte, ln)
	for idx := 0; idx < ln; idx += buffLen {
		copy(out[idx:], buff)
	}
	return string(out[:ln])
}

func UUIDFromTime(rnd *rand.Rand) string {
	if UnderTest {
		return gocql.TimeUUIDWith(rnd.Int63(), 0, []byte("127.0.0.1")).String()
	}
	return gocql.UUIDFromTime(RandDate(rnd)).String()
}

func UnwrapErr(err error) error {
	nextErr := err
	for nextErr != nil {
		err = nextErr
		nextErr = errors.Unwrap(err)
	}
	return err
}
