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
	"sync"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	simpleCounterName = "sc."
	totalCounterName  = "tc."
	subCounterName    = "uc."
	groupName         = "gr."
)

var (
	allGroups     = make(Groups, 0)
	allGroupsMute sync.RWMutex
)

var StmtsCounters = InitGroup("generated stmt`s", "count of all generated stmt`s", true)

type Group struct {
	parentGroup    *Group
	description    string
	name           string
	groups         Groups
	simpleCounters SimpleCounters
	totalCounters  TotalCounters
	active         bool
	mut            sync.RWMutex
}

type Groups []*Group

type Info struct {
	Name                  string
	Unit                  string
	Description           string
	PrometheusIntegration bool
}

func InitGroup(name, description string, active bool) *Group {
	group := Group{
		parentGroup: nil,
		name:        name,
		description: description,
		active:      active,
	}
	allGroupsMute.Lock()
	allGroups = append(allGroups, &group)
	allGroupsMute.Unlock()
	return &group
}

func (g *Group) AddGroup(name, description string, active bool) *Group {
	group := Group{
		parentGroup: g,
		name:        name,
		description: description,
		active:      active,
	}
	if !g.active {
		group.active = false
	}

	g.mut.Lock()
	defer g.mut.Unlock()
	g.groups = append(g.groups, &group)

	return &group
}

func (g *Group) AddSimpleCounters(counters []Info) SimpleCounters {
	sCounters := make(SimpleCounters, len(counters))
	for idx := range sCounters {
		sCounters[idx] = g.initCounter(counters[idx])
	}

	g.mut.Lock()
	defer g.mut.Unlock()
	g.simpleCounters = sCounters
	return sCounters
}

func (g *Group) AddSimpleCounter(counter Info) *SimpleCounter {
	g.mut.Lock()
	defer g.mut.Unlock()

	sCounter := g.initCounter(counter)
	g.simpleCounters = append(g.simpleCounters, sCounter)
	return sCounter
}

func (g *Group) initCounter(counter Info) *SimpleCounter {
	newCounter := &SimpleCounter{
		name:         counter.Name,
		unit:         counter.Unit,
		inPrometheus: counter.PrometheusIntegration,
		description:  counter.Description,
		group:        g,
		val:          atomic.Uint64{},
	}
	if counter.PrometheusIntegration {
		newCounter.prometheus = prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: g.getParentGroupName(),
			Subsystem: g.name,
			Name:      counter.Name,
			Help:      counter.Description,
		})
	}
	return newCounter
}

func (g *Group) getParentGroupName() string {
	pgName := ""
	if g.parentGroup != nil {
		pgName = g.parentGroup.name
	}
	return pgName
}
