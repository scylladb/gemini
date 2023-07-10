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
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

type SimpleCounters []*SimpleCounter

func (l SimpleCounters) Add(idx, in int) {
	l[idx].Add(in)
}

func (l SimpleCounters) Inc(idx int) {
	l[idx].Inc()
}

func (l SimpleCounters) Get(idx int) uint64 {
	return l[idx].Get()
}

func (l SimpleCounters) GetCounter(idx int) *SimpleCounter {
	return l[idx]
}

func (l SimpleCounters) printFull() printRows {
	out := make(printRows, 0, len(l))
	for idx := range l {
		if l.Get(idx) == 0 {
			continue
		}
		out = append(out, l[idx].printFull())
	}
	out.alignRows()
	return out
}

type SimpleCounter struct {
	group        *Group
	name         string
	prometheus   prometheus.Counter
	unit         string
	description  string
	val          atomic.Uint64
	inPrometheus bool
	_            noCopy
}

func (c *SimpleCounter) Add(in int) {
	if in > 0 {
		c.val.Add(uint64(in))
		if c.inPrometheus {
			c.prometheus.Add(float64(in))
		}
	}
	if in < 0 {
		panic("add value should be >0")
	}
}

func (c *SimpleCounter) Inc() {
	c.val.Add(1)
	if c.inPrometheus {
		c.prometheus.Inc()
	}
}

func (c *SimpleCounter) Get() uint64 {
	return c.val.Load()
}

func (c *SimpleCounter) printFull() printRow {
	prometh := "prometheus:no "
	if c.inPrometheus {
		prometh = "prometheus:yes"
	}
	return printRow{
		"",
		simpleCounterName,
		c.name + ":",
		fmt.Sprintf("%d", c.val.Load()),
		separator + c.unit,
		separator + prometh,
		separator + "description:" + c.description,
	}
}

type noCopy struct{}

func (*noCopy) Lock() {}

func (*noCopy) Unlock() {}
