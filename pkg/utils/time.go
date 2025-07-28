// Copyright 2025 ScyllaDB
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
	"strconv"
	"time"
)

type durationUnit struct {
	name string
	ns   int64
}

var units = [10]durationUnit{
	{"y", 365 * 24 * 60 * 60 * 1_000_000_000},
	{"mo", 30 * 24 * 60 * 60 * 1_000_000_000},
	{"w", 7 * 24 * 60 * 60 * 1_000_000_000},
	{"d", 24 * 60 * 60 * 1_000_000_000},
	{"h", 60 * 60 * 1_000_000_000},
	{"m", 60 * 1_000_000_000},
	{"s", 1_000_000_000},
	{"ms", 1_000_000},
	{"us", 1_000},
	{"ns", 1},
}

func TimeDurationToScyllaDuration(d time.Duration) string {
	if d == 0 {
		return "0ns"
	}

	result := make([]byte, 0, 64)
	remaining := d.Nanoseconds()

	for _, unit := range units {
		if count := remaining / unit.ns; count > 0 {
			result = strconv.AppendInt(result, count, 10)
			result = append(result, unit.name...)
			remaining %= unit.ns
		}
	}

	return UnsafeString(result)
}
