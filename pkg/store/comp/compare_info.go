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

package comp

import (
	"fmt"
	"strings"
)

// Info contains information about responses difference.
type Info []string

func (d *Info) Len() int {
	return len(*d)
}

func (d *Info) Add(in ...string) {
	*d = append(*d, in...)
}

func (d *Info) String() string {
	return strings.Join(*d, "\n")
}

func GetCompareInfoSimple(d Results) Info {
	lenTest := d.LenRowsTest()
	lenOracle := d.LenRowsOracle()
	switch {
	case lenTest == 0 && lenOracle == 0:
		return nil
		// responses don`t have rows
	case lenTest == lenOracle:
		// responses have rows and have same rows count
		equalRowsCount := equalRowsSameLen(d)
		// equalRowsSameLen function simultaneously deletes equal rows in Test and Oracle stores.
		// So we can check rows only one of the stores.
		if d.LenRowsTest() < 1 {
			return nil
		}
		return Info{fmt.Sprintf("responses have %d equal rows and unequal rows %d", equalRowsCount, d.LenRowsTest())}
	default:
		// responses have different rows count
		return Info{fmt.Sprintf("different rows count in responses: from test store-%d, from oracle store-%d", lenTest, lenOracle)}
	}
}

func GetCompareInfoDetailed(d Results) Info {
	lenTest := d.LenRowsTest()
	lenOracle := d.LenRowsOracle()
	switch {
	case lenTest == 0 && lenOracle == 0:
		return nil
		// responses don`t have rows
	case lenTest < 1 || lenOracle < 1:
		// one of the responses without rows.
		diff := make(Info, 0)
		diff.Add(fmt.Sprintf("different rows count in responses: from test store-%d, from oracle store-%d", lenTest, lenOracle))
		diff.Add(d.StringAllRows("unequal")...)
		return diff
	case lenTest == lenOracle:
		// responses have rows and have same rows count
		equalRowsCount := equalRowsSameLen(d)
		// equalRowsSameLen function simultaneously deletes equal rows in Test and Oracle stores.
		// So we can check rows only one of the stores.
		if d.LenRowsTest() < 1 {
			return nil
		}
		diff := make(Info, 0)
		diff.Add(fmt.Sprintf("responses have %d equal rows and unequal rows: test store %d; oracle store %d", equalRowsCount, d.LenRowsTest(), d.LenRowsOracle()))
		diff.Add(d.StringAllRows("unequal")...)
		return diff
	default:
		// responses have rows and have different rows count
		diff := make(Info, 0)
		equalRowsCount := equalRowsDiffLen(d)
		diff.Add(fmt.Sprintf("responses have %d equal rows and unequal rows: test store %d; oracle store %d", equalRowsCount, d.LenRowsTest(), d.LenRowsOracle()))
		diff.Add(d.StringAllRows("unequal")...)
		return diff
	}
}

// equalRowsSameLen returns count of equal rows of stores simultaneously deletes equal rows.
// Applies when oracle and test stores have same rows count.
func equalRowsSameLen(d Results) int {
	if d.LenRowsTest() == 1 {
		return d.EqualSingeRow()
	}
	equalRowsCount := d.EasyEqualRowsTest()
	if d.LenRowsTest() != 0 {
		equalRowsCount += d.EqualRowsTest()
	}
	return equalRowsCount
}

// equalRowsDiffLen returns count of equal rows of stores simultaneously deletes equal rows.
// Applies when oracle and test stores have different rows count.
func equalRowsDiffLen(d Results) int {
	equalRowsCount := 0
	if d.LenRowsTest() > d.LenRowsOracle() {
		equalRowsCount = d.EasyEqualRowsOracle()
		if d.LenRowsOracle() > 0 {
			equalRowsCount += d.EqualRowsOracle()
		}
	} else {
		equalRowsCount = d.EasyEqualRowsTest()
		if d.LenRowsTest() > 0 {
			equalRowsCount += d.EqualRowsTest()
		}
	}
	return equalRowsCount
}
