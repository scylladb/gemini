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
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

type TotalCounters []*TotalCounter

func (l TotalCounters) printFull() printRows {
	out := make(printRows, 0, len(l)*5)
	for idx := range l {
		out = append(out, l[idx].printFull()...)
	}
	return out
}

type TotalCounter struct {
	prometheus            *prometheus.CounterVec
	group                 *Group
	name                  string
	unit                  string
	description           string
	subCounters           SubCounters
	val                   atomic.Uint64
	prometheusIntegration bool
	_                     noCopy
}

type SubCounters []*SubCounter

type SubCounter struct {
	tc         *TotalCounter
	prometheus prometheus.Counter
	name       string
	val        atomic.Uint64
}

func (c *TotalCounter) Add(idx, in int) {
	c.subCounters[idx].Add(in)
}

func (c *TotalCounter) Inc(idx int) {
	c.subCounters[idx].Inc()
}

func (c *TotalCounter) Get(idx int) uint64 {
	return c.subCounters[idx].val.Load()
}

func (c *TotalCounter) GetTotal() uint64 {
	return c.val.Load()
}

func (c *TotalCounter) GetSubCounters() SubCounters {
	return c.subCounters
}

func (c *SubCounter) Add(in int) {
	if in > 0 {
		c.tc.val.Add(uint64(in))
		c.val.Add(uint64(in))
		if c.tc.prometheusIntegration {
			c.prometheus.Add(float64(in))
		}
	}
	if in < 0 {
		panic("add value should be >0")
	}
}

func (c *SubCounter) Inc() {
	c.tc.val.Add(1)
	c.val.Add(1)
	if c.tc.prometheusIntegration {
		c.prometheus.Inc()
	}
}

func (c *SubCounter) Get() uint64 {
	return c.val.Load()
}

func (c *TotalCounter) printFull() printRows {
	out := make(printRows, 0, len(c.subCounters)+1)
	prometh := "prometheus:no "
	if c.prometheusIntegration {
		prometh = "prometheus:yes"
	}

	fistRow := printRow{
		"",
		totalCounterName,
		c.name + ":",
		fmt.Sprintf("%d", c.val.Load()),
		separator + c.unit,
		separator + prometh,
		separator + "description:" + c.description,
	}
	out = append(out, fistRow)

	subCountRows := make(printRows, 0, len(c.subCounters))
	for idx := range c.subCounters {
		if c.subCounters[idx].Get() == 0 {
			continue
		}
		percent := separator + fmt.Sprintf("%.3f", 100*float64(c.subCounters[idx].Get())/float64(c.val.Load()))
		if len(percent) < 7 {
			percent = strings.Repeat(" ", 7-len(percent)) + percent
		}
		subCountRows = append(subCountRows, printRow{
			"",
			subCounterName,
			c.subCounters[idx].name + ":",
			fmt.Sprintf("%d", c.subCounters[idx].Get()),
			separator + c.unit,
			percent,
			separator + "%",
		})
	}
	for idx := range subCountRows {
		subCountRows[idx][0] = getPlHolder(idx, len(subCountRows))
	}
	subCountRows.alignRows()
	out = append(out, subCountRows...)

	return out
}

func (g *Group) AddTotalCounter(counter Info, prometheusLabel string, subCounters []string) *TotalCounter {
	tCounter := TotalCounter{
		group:                 g,
		name:                  counter.Name,
		unit:                  counter.Unit,
		prometheusIntegration: counter.PrometheusIntegration,
		description:           counter.Description,
		val:                   atomic.Uint64{},
	}

	sCounters := make(SubCounters, len(subCounters))
	if counter.PrometheusIntegration {
		tCounter.prometheus = prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: g.getParentGroupName(),
			Subsystem: g.name,
			Name:      counter.Name,
			Help:      counter.Description,
		}, []string{prometheusLabel})
	}

	for idx := range sCounters {
		sCounters[idx] = &SubCounter{
			tc:   &tCounter,
			val:  atomic.Uint64{},
			name: subCounters[idx],
		}
		if counter.PrometheusIntegration {
			sCounters[idx].prometheus = tCounter.prometheus.WithLabelValues(sCounters[idx].name)
		}
	}
	tCounter.subCounters = sCounters
	defer g.mut.Unlock()
	g.mut.Lock()
	g.totalCounters = append(g.totalCounters, &tCounter)
	return &tCounter
}
