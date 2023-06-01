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
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"golang.org/x/exp/rand"
)

func RandDate(rnd *rand.Rand) string {
	return RandTime(rnd).Format("2006-01-02")
}

func RandTime(rnd *rand.Rand) time.Time {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2024, 1, 0, 0, 0, 0, 0, time.UTC).Unix()

	sec := rnd.Int63n(max-min) + min
	return time.Unix(sec, 0).UTC()
}

func RandIpV4Address(rnd *rand.Rand, v, pos int) string {
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
			blocks = append(blocks, strconv.Itoa(rnd.Intn(255)))
		}
	}
	return strings.Join(blocks, ".")
}

func RandInt2(rnd *rand.Rand, min, max int) int {
	if max <= min {
		return min
	}
	return min + rnd.Intn(max-min)
}

func RandInt(min, max int) int {
	if max <= min {
		return min
	}
	return min + rand.Intn(max-min)
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
	return gocql.UUIDFromTime(RandTime(rnd)).String()
}
