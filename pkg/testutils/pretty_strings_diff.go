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

package testutils

import (
	"fmt"
	"strings"
)

func AppendIfNotEmpty(slice []string, val string) []string {
	if val == "" {
		return slice
	}
	return append(slice, val)
}

func GetErrorMsgIfDifferent(expected, received, errMsg string) string {
	if expected == received {
		return ""
	}
	errMsgList := make([]string, 0)
	subString := " "
	if strings.Count(expected, ",\"") > strings.Count(expected, subString) {
		subString = ",\""
	}
	tmpExpected := strings.Split(expected, subString)
	tmpReceived := strings.Split(received, subString)
	switch len(tmpExpected) == len(tmpReceived) {
	case true:
		// Inject nice row that highlights differences if length is not changed
		expected, received = addDiffHighlight(tmpExpected, tmpReceived, subString)
		errMsgList = []string{
			errMsg,
			fmt.Sprintf("Expected   %s", expected),
			diffHighlightString([]rune(expected), []rune(received)),
			fmt.Sprintf("Received   %s", received),
			"-------------------------------------------",
		}
	case false:
		errMsgList = []string{
			errMsg,
			fmt.Sprintf("Expected   %s", expected),
			fmt.Sprintf("Received   %s", received),
			"-------------------------------------------",
		}
	}
	return strings.Join(errMsgList, "\n")
}

func diffHighlightString(expected, received []rune) string {
	out := "Difference "
	for idx := range expected {
		if expected[idx] == received[idx] {
			out += " "
		} else {
			out += "↕"
		}
	}
	return out
}

func addDiffHighlight(expected, received []string, subString string) (string, string) {
	for idx := range expected {
		delta := len(expected[idx]) - len(received[idx])
		if delta > 0 {
			received[idx] += strings.Repeat("↔", delta)
		}
		if delta < 0 {
			expected[idx] += strings.Repeat("↔", -delta)
		}
	}
	return strings.Join(expected, subString), strings.Join(received, subString)
}
