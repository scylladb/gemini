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

package count

import (
	"fmt"
	"strings"
)

const separator = " "

type printRows []printRow

type printRow [7]string

func (l printRows) alignRows() {
	maxNameAndValLen := 0
	maxUnitLen := 0
	for r := range l {
		nameAndValLen := len(l[r][2]) + len(l[r][3])
		if nameAndValLen > maxNameAndValLen {
			maxNameAndValLen = nameAndValLen
		}
		unitLen := len(l[r][4])
		if unitLen > maxUnitLen {
			maxUnitLen = unitLen
		}
	}
	for r := range l {
		nameAndValLen := len(l[r][2]) + len(l[r][3])
		if nameAndValLen < maxNameAndValLen {
			l[r][3] = strings.Repeat(" ", maxNameAndValLen-nameAndValLen) + l[r][3]
		}
		unitLen := len(l[r][4])
		if unitLen < maxUnitLen {
			l[r][4] += strings.Repeat(" ", maxUnitLen-unitLen)
		}
	}
}

func (p printRow) getString() string {
	out := ""
	for i := range p {
		out += p[i]
	}

	return out
}

func (l printRows) getStrings() []string {
	out := make([]string, len(l))
	for i := range l {
		out[i] = l[i].getString()
	}
	return out
}

func getPlHolder(idx, lastIdx int) string {
	plHolder := "  ├"
	if idx == lastIdx-1 {
		plHolder = "  └"
	}
	return plHolder
}

func (g *Group) printFull() printRows {
	out := make(printRows, 0, 20)

	active := "off"
	if g.active {
		active = "on "
	}

	fistRow := printRow{
		"",
		groupName,
		g.name + ":",
		"",
		"",
		separator + active,
		separator + "description:" + g.description,
	}
	out = append(out, fistRow)
	out = append(out, g.simpleCounters.printFull()...)
	out = append(out, g.totalCounters.printFull()...)
	if len(g.groups) != 0 {
		for idx := range g.groups {
			out = append(out, g.groups[idx].printFull()...)
		}
	}
	lastElem := false

	// add pretty grouping marks
	for idx := len(out) - 1; idx > 0; idx-- {
		plHolder := "   "
		if lastElem {
			plHolder = "  │"
		}
		if out[idx][0] == "" {
			if !lastElem {
				lastElem = true
				plHolder = "  └"
			} else {
				plHolder = "  ├"
			}
		}
		out[idx][0] = plHolder + out[idx][0]
	}
	return out
}

func (g *Group) Print() {
	g.mut.RLock()
	defer g.mut.RUnlock()
	tmp := g.printFull()
	out := tmp.getStrings()
	fmt.Println(strings.Join(out, "\n"))
}

func PrintAllGroups() {
	allGroupsMute.RLock()
	for idx := range allGroups {
		allGroups[idx].Print()
	}
	allGroupsMute.RUnlock()
}
