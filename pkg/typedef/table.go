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

// ListColInfo describes a list-type column for efficient deduplication.
type ListColInfo struct {
	Name      string
	ValueType SimpleType
}

type Table struct {
	schema            *Schema
	KnownIssues       KnownIssues `json:"known_issues"`
	Name              string      `json:"name"`
	listCols          []ListColInfo
	PartitionKeys     Columns            `json:"partition_keys"`
	ClusteringKeys    Columns            `json:"clustering_keys"`
	Columns           Columns            `json:"columns"`
	Indexes           []IndexDef         `json:"indexes,omitempty"`
	MaterializedViews []MaterializedView `json:"materialized_views,omitempty"`
	TableOptions      []string           `json:"table_options,omitempty"`
	// Cached bind-value counts for the key/column groups. Computed once in
	// buildCache so the statement-generation hot path avoids re-summing
	// Type.LenValue() across every column on every statement. The schema is
	// immutable after construction, so these never go stale.
	pkLenValues  int
	ckLenValues  int
	colLenValues int
	mu           sync.RWMutex
	cacheOnce    sync.Once
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
	for _, col := range t.Columns {
		if _, ok := col.Type.(*CounterType); ok {
			return true
		}
	}

	return false
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
	return len(t.MaterializedViews) == 0
}

func (t *Table) Init(s *Schema) {
	t.schema = s
	// Pre-warm the metadata cache while still single-threaded.
	t.cacheOnce.Do(t.buildCache)
}

// buildCache scans columns once and caches metadata derived from the (immutable)
// schema: the list-column info used for dedup and the per-group bind-value
// counts used as slice-capacity hints in the statement hot path.
func (t *Table) buildCache() {
	t.listCols = nil
	for _, col := range t.Columns {
		if ct, ok := col.Type.(*Collection); ok && ct.ComplexType == TypeList {
			t.listCols = append(t.listCols, ListColInfo{
				Name:      col.Name,
				ValueType: ct.ValueType,
			})
		}
	}

	t.pkLenValues = t.PartitionKeys.LenValues()
	t.ckLenValues = t.ClusteringKeys.LenValues()
	t.colLenValues = t.Columns.LenValues()
}

// ListColumns returns cached list column info. Returns nil if the table
// has no list columns — callers can skip deduplication entirely.
// Thread-safe: the cache is built at most once using sync.Once.
func (t *Table) ListColumns() []ListColInfo {
	t.cacheOnce.Do(t.buildCache)
	return t.listCols
}

// PartitionKeysLenValues returns the cached total number of bind values across
// all partition-key columns. Equivalent to t.PartitionKeys.LenValues() but
// computed once. Safe whether or not Init was called (lazily built).
func (t *Table) PartitionKeysLenValues() int {
	t.cacheOnce.Do(t.buildCache)
	return t.pkLenValues
}

// ClusteringKeysLenValues returns the cached total number of bind values across
// all clustering-key columns.
func (t *Table) ClusteringKeysLenValues() int {
	t.cacheOnce.Do(t.buildCache)
	return t.ckLenValues
}

// ColumnsLenValues returns the cached total number of bind values across all
// non-key columns.
func (t *Table) ColumnsLenValues() int {
	t.cacheOnce.Do(t.buildCache)
	return t.colLenValues
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
			if !mv.HaveNonPrimaryKey() {
				continue
			}

			nonPrimCol := mv.NonPrimaryKey.MustGet()
			for j := range validCols {
				if validCols[j].Name == nonPrimCol.Name {
					validCols = slices.Delete(validCols, j, j+1)
					break
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
