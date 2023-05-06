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

package gemini

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/ksuid"
	"golang.org/x/exp/rand"
)

func randStringWithTime(rnd *rand.Rand, len int, t time.Time) string {
	id, _ := ksuid.NewRandomWithTime(t)

	var buf strings.Builder
	buf.WriteString(id.String())
	if buf.Len() >= len {
		return buf.String()[:len]
	}

	// Pad some extra random data
	buff := make([]byte, len-buf.Len())
	_, _ = rnd.Read(buff)
	buf.WriteString(base64.StdEncoding.EncodeToString(buff))

	return buf.String()[:len]
}

func randDate(rnd *rand.Rand) string {
	time := randTime(rnd)
	return time.Format("2006-01-02")
}

func randTime(rnd *rand.Rand) time.Time {
	min := time.Date(1970, 1, 0, 0, 0, 0, 0, time.UTC).Unix()
	max := time.Date(2024, 1, 0, 0, 0, 0, 0, time.UTC).Unix()

	sec := rnd.Int63n(max-min) + min
	return time.Unix(sec, 0)
}

func randIpV4Address(rnd *rand.Rand, v, pos int) string {
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

func appendValue(columnType Type, r *rand.Rand, p *PartitionRangeConfig, values []interface{}) []interface{} {
	return append(values, columnType.GenValue(r, p)...)
}
