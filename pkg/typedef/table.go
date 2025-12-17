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
	"slices"
	"sync"
)

type KnownIssues map[string]bool

type Table struct {
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

func (t *Table) SelectColumnNames() []string {
	columns := make([]string, 0, len(t.PartitionKeys)+len(t.ClusteringKeys)+len(t.Columns))

	for _, pk := range t.PartitionKeys {
		columns = append(columns, pk.Name)
	}
	for _, ck := range t.ClusteringKeys {
		columns = append(columns, ck.Name)
	}
	for _, col := range t.Columns {
		columns = append(columns, col.Name)
	}

	return columns
}

func (t *Table) IsCounterTable() bool {
	if len(t.Columns) != 1 {
		return false
	}
	_, ok := t.Columns[0].Type.(*CounterType)
	return ok
}

func (t *Table) Lock() {
	if len(t.MaterializedViews) != 0 {
		return
	}

	t.mu.Lock()
}

func (t *Table) Unlock() {
	if len(t.MaterializedViews) != 0 {
		return
	}

	t.mu.Unlock()
}

func (t *Table) RLock() {
	if len(t.MaterializedViews) != 0 {
		return
	}
	t.mu.RLock()
}

func (t *Table) RUnlock() {
	if len(t.MaterializedViews) != 0 {
		return
	}

	t.mu.RUnlock()
}

func (t *Table) SupportsChanges() bool {
	// If the table has materialized views, it does not support schema changes.
	// Scylla does not allow schema changes on tables with materialized views.
	return len(t.MaterializedViews) != 0
}

func (t *Table) Init(s *Schema) {
	t.schema = s
}

func (t *Table) ValidColumnsForDelete() Columns {
	if t.Columns.Len() == 0 {
		return nil
	}
	validCols := make(Columns, 0, len(t.Columns))
	validCols = append(validCols, t.Columns...)
	if len(t.Indexes) != 0 {
		for _, idx := range t.Indexes {
			for j := range validCols {
				if validCols[j].Name == idx.ColumnName {
					validCols = slices.Delete(validCols, j, j+1)
					break
				}
			}
		}
	}
	if len(t.MaterializedViews) != 0 {
		for _, mv := range t.MaterializedViews {
			if mv.HaveNonPrimaryKey() {
				nonPrimCol := mv.NonPrimaryKey.MustGet()
				for j := range validCols {
					if validCols[j].Name == nonPrimCol.Name {
						validCols = slices.Delete(validCols, j, j+1)
						break
					}
				}
			}
		}
	}
	return validCols
}

func (t *Table) LinkIndexAndColumns() {
	for i, index := range t.Indexes {
		for c, column := range t.Columns {
			if index.ColumnName == column.Name {
				t.Indexes[i].Column = t.Columns[c]
				break
			}
		}
	}
}
