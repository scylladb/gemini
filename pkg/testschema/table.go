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

package testschema

import (
	"sync"

	"github.com/scylladb/gemini/pkg/coltypes"
	"github.com/scylladb/gemini/pkg/typedef"
)

type Table struct {
	Name              string             `json:"name"`
	PartitionKeys     Columns            `json:"partition_keys"`
	ClusteringKeys    Columns            `json:"clustering_keys"`
	Columns           Columns            `json:"columns"`
	Indexes           []typedef.IndexDef `json:"indexes,omitempty"`
	MaterializedViews []MaterializedView `json:"materialized_views,omitempty"`
	KnownIssues       map[string]bool    `json:"known_issues"`
	TableOptions      []string           `json:"table_options,omitempty"`

	// mu protects the table during schema changes
	mu sync.RWMutex
}

func (t *Table) IsCounterTable() bool {
	if len(t.Columns) != 1 {
		return false
	}
	_, ok := t.Columns[0].Type.(*coltypes.CounterType)
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
