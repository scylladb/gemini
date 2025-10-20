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

package partitions

import (
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/typedef"
)

type (
	Interface interface {
		Stats() Stats
		Get(idx uint64) *typedef.Values
		Next() *typedef.Values

		Extend() *typedef.Values
		ReplaceNext() *typedef.Values
		Replace(idx uint64) *typedef.Values
		ReplaceWithoutOld(idx uint64)
		ReplaceNextWithoutOld()

		Len() uint64
	}

	Partition struct {
		values []any
	}

	Partitions struct {
		parts              []any
		r                  *rand.Rand
		table              *typedef.Table
		config             typedef.PartitionRangeConfig
		idxFunc            distributions.DistributionFunc
		partitionValuesLen uint64
		mu                 sync.RWMutex
		count              atomic.Uint64
		partitionsReplaced atomic.Uint64
		partitionsCreated  atomic.Uint64
	}

	Stats struct {
		CurrentPartitionCount uint64
		PartitionsReplaced    uint64
		PartitionsCreated     uint64
		MemoryUsage           uint64
	}
)

func New(r *rand.Rand, idxFunc distributions.DistributionFunc, table *typedef.Table, config typedef.PartitionRangeConfig, count uint64) *Partitions {
	partitionValuesLen := table.PartitionKeys.LenValues()

	p := &Partitions{
		parts:              make([]any, count*uint64(partitionValuesLen)),
		r:                  r,
		table:              table,
		config:             config,
		idxFunc:            idxFunc,
		partitionValuesLen: uint64(partitionValuesLen),
	}

	p.count.Store(count)

	for i := range count {
		p.fill(i)
	}

	return p
}

func (p Partition) size() uint64 {
	return 0
}

func (p *Partitions) partition(idx uint64) Partition {
	return Partition{values: p.parts[idx*p.partitionValuesLen : (idx+1)*p.partitionValuesLen]}
}

func (p *Partitions) values(idx uint64) *typedef.Values {
	m := make(map[string][]any, p.partitionValuesLen)

	p.mu.RLock()
	values := p.partition(idx).values
	for i, col := range p.table.PartitionKeys {
		m[col.Name] = values[i*col.Type.LenValue() : (i+1)*col.Type.LenValue()]
	}
	p.mu.RUnlock()

	return typedef.NewValuesFromMap(m)
}

func (p *Partitions) valuesCopy(idx uint64) *typedef.Values {
	m := make(map[string][]any, p.partitionValuesLen)

	p.mu.RLock()
	values := p.partition(idx).values
	for i, col := range p.table.PartitionKeys {
		m[col.Name] = append(m[col.Name], values[i*col.Type.LenValue():(i+1)*col.Type.LenValue()]...)
	}
	p.mu.RUnlock()

	return typedef.NewValuesFromMap(m)
}

func (p *Partitions) fill(idx uint64) {
	values := generateValue(p.r, p.table, &p.config)

	if uint64(len(values)) != p.partitionValuesLen {
		panic(fmt.Sprintf(
			"invalid partition values generated: length %d, expected %d",
			len(values),
			p.partitionValuesLen,
		))
	}

	p.mu.Lock()
	copy(p.partition(idx).values[:], values)
	p.mu.Unlock()
}

func (p *Partitions) Stats() Stats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	mem := uint64(0)
	for i := range p.count.Load() {
		mem += p.partition(i).size()
	}

	return Stats{
		CurrentPartitionCount: p.count.Load(),
		PartitionsReplaced:    p.partitionsReplaced.Load(),
		PartitionsCreated:     p.partitionsCreated.Load(),
		MemoryUsage:           mem,
	}
}

func (p *Partitions) Get(idx uint64) *typedef.Values {
	return p.values(idx)
}

func (p *Partitions) Next() *typedef.Values {
	return p.Get(p.idxFunc())
}

func (p *Partitions) Extend() *typedef.Values {
	p.mu.Lock()
	p.parts = append(p.parts, make([]any, p.partitionValuesLen)...)
	p.mu.Unlock()

	value := p.count.Add(1)
	p.fill(value)

	p.partitionsCreated.Add(1)

	return p.values(value)
}

func (p *Partitions) Replace(idx uint64) *typedef.Values {
	oldValues := p.valuesCopy(idx)
	p.fill(idx)

	p.partitionsReplaced.Add(1)
	return oldValues
}

func (p *Partitions) ReplaceNext() *typedef.Values {
	return p.Replace(p.idxFunc())
}

func (p *Partitions) ReplaceWithoutOld(idx uint64) {
	p.fill(idx)
	p.partitionsReplaced.Add(1)
}

func (p *Partitions) ReplaceNextWithoutOld() {
	p.fill(p.idxFunc())
	p.partitionsReplaced.Add(1)
}

func (p *Partitions) Len() uint64 {
	return p.count.Load()
}

func generateValue(r *rand.Rand, table *typedef.Table, config *typedef.PartitionRangeConfig) []any {
	values := make([]any, 0, table.PartitionKeys.LenValues())

	for _, pk := range table.PartitionKeys {
		values = pk.Type.GenValueOut(values, r, config)
	}

	return values
}

func NewPartitionKeys(r *rand.Rand, table *typedef.Table, config *typedef.PartitionRangeConfig) typedef.PartitionKeys {
	values := generateValue(r, table, config)

	m := make(map[string][]any, table.PartitionKeys.LenValues())

	for i, col := range table.PartitionKeys {
		m[col.Name] = append(m[col.Name], values[i*col.Type.LenValue():(i+1)*col.Type.LenValue()]...)
	}

	return typedef.PartitionKeys{
		Values: typedef.NewValuesFromMap(m),
	}
}
