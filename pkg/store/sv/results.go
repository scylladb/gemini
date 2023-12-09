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

package sv

type Results struct {
	Oracle Result
	Test   Result
}

func (d *Results) HaveRows() bool {
	return len(d.Test.Rows) != 0 || len(d.Oracle.Rows) != 0
}

func (d *Results) LenRowsTest() int {
	return len(d.Test.Rows)
}

func (d *Results) LenRowsOracle() int {
	return len(d.Oracle.Rows)
}

func (d *Results) StringAllRows(prefix string) []string {
	var out []string
	if d.Test.LenRows() != 0 {
		out = append(out, prefix+" test store rows:")
		out = append(out, d.Test.StringAllRows()...)
	}
	if d.Oracle.LenRows() != 0 {
		out = append(out, prefix+" oracle store rows:")
		out = append(out, d.Oracle.StringAllRows()...)
	}
	return out
}

func (d *Results) EqualSingeRow() int {
	if d.Test.Rows[0].Equal(d.Oracle.Rows[0]) {
		d.Test.Rows = nil
		d.Oracle.Rows = nil
		return 1
	}
	return 0
}

func (d *Results) EasyEqualRowsTest() int {
	var idx int
	// Travel through rows to find fist unequal row.
	for range d.Test.Rows {
		if d.Test.Rows[idx].Equal(d.Oracle.Rows[idx]) {
			idx++
			continue
		}
		break
	}
	d.Test.Rows = d.Test.Rows[idx:]
	d.Oracle.Rows = d.Oracle.Rows[idx:]
	return idx
}

func (d *Results) EasyEqualRowsOracle() int {
	var idx int
	// Travel through rows to find fist unequal row.
	for range d.Oracle.Rows {
		if d.Oracle.Rows[idx].Equal(d.Test.Rows[idx]) {
			idx++
			continue
		}
		break
	}
	d.Test.Rows = d.Test.Rows[idx:]
	d.Oracle.Rows = d.Oracle.Rows[idx:]
	return idx
}

func (d *Results) EqualRowsTest() int {
	var equalRowsCount int
	idxT := 0
	for range d.Test.Rows {
		idxO := d.Oracle.Rows.FindEqualRow(d.Test.Rows[idxT])
		if idxO < 0 {
			// No equal row founded - switch to next Test row.
			idxT++
			continue
		}
		// EqualColumn row founded - delete equal row from Test and Oracle stores, add equal rows counter
		d.Oracle.Rows[idxO] = d.Oracle.Rows[d.Oracle.LenRows()-1]
		d.Oracle.Rows = d.Oracle.Rows[:d.Oracle.LenRows()-1]
		d.Test.Rows[idxT] = d.Test.Rows[d.Test.LenRows()-1]
		d.Test.Rows = d.Test.Rows[:d.Test.LenRows()-1]
		equalRowsCount++
	}
	return equalRowsCount
}

func (d *Results) EqualRowsOracle() int {
	var equalRowsCount int
	idxO := 0
	for range d.Oracle.Rows {
		idxT := d.Test.Rows.FindEqualRow(d.Oracle.Rows[idxO])
		if idxT < 0 {
			// No equal row founded - switch to next Oracle row
			idxO++
			continue
		}
		// EqualColumn row founded - delete equal row from Test and Oracle stores, add equal rows counter
		d.Test.Rows[idxT] = d.Test.Rows[d.Test.LenRows()-1]
		d.Test.Rows = d.Test.Rows[:d.Test.LenRows()-1]
		d.Oracle.Rows[idxO] = d.Oracle.Rows[d.Oracle.LenRows()-1]
		d.Oracle.Rows = d.Oracle.Rows[:d.Oracle.LenRows()-1]
		equalRowsCount++
	}
	return equalRowsCount
}
