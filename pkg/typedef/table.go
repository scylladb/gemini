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

package typedef

import (
	"sync"
)

type QueryCache interface {
	GetQuery(qct StatementCacheType) *StmtCache
	Reset()
	BindToTable(t *Table)
}

type KnownIssues map[string]bool

type Table struct {
	queryCache             QueryCache
	schema                 *Schema
	Name                   string             `json:"name"`
	PartitionKeys          Columns            `json:"partition_keys"`
	ClusteringKeys         Columns            `json:"clustering_keys"`
	Columns                Columns            `json:"columns"`
	Indexes                []IndexDef         `json:"indexes,omitempty"`
	MaterializedViews      []MaterializedView `json:"materialized_views,omitempty"`
	KnownIssues            KnownIssues        `json:"known_issues"`
	TableOptions           []string           `json:"table_options,omitempty"`
	partitionKeysLenValues int

	// mu protects the table during schema changes
	mu sync.RWMutex
}

func (t *Table) PartitionKeysLenValues() int {
	if t.partitionKeysLenValues == 0 {
		t.partitionKeysLenValues = t.PartitionKeys.LenValues()
	}
	return t.partitionKeysLenValues
}

func (t *Table) IsCounterTable() bool {
	if len(t.Columns) != 1 {
		return false
	}
	_, ok := t.Columns[0].Type.(*CounterType)
	return ok
}

func (t *Table) Lock() {
	t.mu.Lock()
}

func (t *Table) Unlock() {
	t.mu.Unlock()
}

func (t *Table) RLock() {
	t.mu.RLock()
}

func (t *Table) RUnlock() {
	t.mu.RUnlock()
}

func (t *Table) GetQueryCache(st StatementCacheType) *StmtCache {
	return t.queryCache.GetQuery(st)
}

func (t *Table) ResetQueryCache() {
	t.queryCache.Reset()
	t.partitionKeysLenValues = 0
}

func (t *Table) Init(s *Schema, c QueryCache) {
	t.schema = s
	t.queryCache = c
	t.queryCache.BindToTable(t)
}
