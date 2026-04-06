// Copyright 2025 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package typedef_test

import (
	"testing"
	"time"

	"github.com/scylladb/gemini/pkg/typedef"
)

// buildSchema wraps a table in a minimal schema and calls Init(), which is
// what SchemaBuilder.Build() does in production.
func buildSchema(t *testing.T, tbl *typedef.Table) *typedef.Schema {
	t.Helper()
	var sb typedef.Schema
	sb.Tables = []*typedef.Table{tbl}
	tbl.Init(&sb)
	return &sb
}

func TestTable_SelectColumnNames(t *testing.T) {
	t.Parallel()

	tbl := &typedef.Table{
		Name: "t1",
		PartitionKeys: typedef.Columns{
			{Name: "pk1", Type: typedef.TypeInt},
			{Name: "pk2", Type: typedef.TypeText},
		},
		ClusteringKeys: typedef.Columns{
			{Name: "ck1", Type: typedef.TypeBigint},
		},
		Columns: typedef.Columns{
			{Name: "col1", Type: typedef.TypeBoolean},
			{Name: "col2", Type: typedef.TypeDouble},
		},
	}

	got := tbl.SelectColumnNames()
	want := []string{"pk1", "pk2", "ck1", "col1", "col2"}
	if len(got) != len(want) {
		t.Fatalf("SelectColumnNames() len = %d; want %d", len(got), len(want))
	}
	for i, name := range want {
		if got[i] != name {
			t.Errorf("SelectColumnNames()[%d] = %q; want %q", i, got[i], name)
		}
	}
}

func TestTable_SelectColumnNames_Empty(t *testing.T) {
	t.Parallel()
	tbl := &typedef.Table{Name: "empty"}
	got := tbl.SelectColumnNames()
	if len(got) != 0 {
		t.Errorf("SelectColumnNames() = %v; want []", got)
	}
}

func TestTable_IsCounterTable_True(t *testing.T) {
	t.Parallel()
	tbl := &typedef.Table{
		Name:          "counter_tbl",
		PartitionKeys: typedef.Columns{{Name: "pk", Type: typedef.TypeInt}},
		Columns: typedef.Columns{
			{Name: "cnt", Type: &typedef.CounterType{}},
		},
	}
	if !tbl.IsCounterTable() {
		t.Error("IsCounterTable() = false; want true for table with counter column")
	}
}

func TestTable_IsCounterTable_False(t *testing.T) {
	t.Parallel()
	tbl := &typedef.Table{
		Name:          "regular_tbl",
		PartitionKeys: typedef.Columns{{Name: "pk", Type: typedef.TypeInt}},
		Columns:       typedef.Columns{{Name: "v", Type: typedef.TypeText}},
	}
	if tbl.IsCounterTable() {
		t.Error("IsCounterTable() = true; want false for regular table")
	}
}

func TestTable_IsCounterTable_NoColumns(t *testing.T) {
	t.Parallel()
	tbl := &typedef.Table{Name: "no_cols"}
	if tbl.IsCounterTable() {
		t.Error("IsCounterTable() = true; want false for table with no columns")
	}
}

func TestTable_SupportsChanges(t *testing.T) {
	t.Parallel()

	cases := []struct {
		tbl  *typedef.Table
		name string
		want bool
	}{
		{
			name: "no-materialized-views",
			tbl:  &typedef.Table{Name: "t"},
			want: true,
		},
		{
			name: "with-materialized-view",
			tbl: &typedef.Table{
				Name:              "t_mv",
				MaterializedViews: []typedef.MaterializedView{{Name: "mv1"}},
			},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := tc.tbl.SupportsChanges(); got != tc.want {
				t.Errorf("SupportsChanges() = %v; want %v", got, tc.want)
			}
		})
	}
}

func TestTable_Init_SetsFields(t *testing.T) {
	t.Parallel()

	tbl := &typedef.Table{
		Name: "init_tbl",
		PartitionKeys: typedef.Columns{
			{Name: "pk1", Type: typedef.TypeInt},
			{Name: "pk2", Type: typedef.TypeText},
		},
		ClusteringKeys: typedef.Columns{
			{Name: "ck1", Type: typedef.TypeBigint},
		},
		Columns: typedef.Columns{
			{Name: "col1", Type: typedef.TypeBoolean},
		},
	}

	buildSchema(t, tbl)

	// SortKeyNames = pk1, pk2, ck1 (in order).
	wantSort := []string{"pk1", "pk2", "ck1"}
	if len(tbl.SortKeyNames) != len(wantSort) {
		t.Fatalf("SortKeyNames len = %d; want %d", len(tbl.SortKeyNames), len(wantSort))
	}
	for i, name := range wantSort {
		if tbl.SortKeyNames[i] != name {
			t.Errorf("SortKeyNames[%d] = %q; want %q", i, tbl.SortKeyNames[i], name)
		}
	}

	// PartitionKeysLenValues must be set (TypeInt has LenValues = 1).
	if tbl.PartitionKeysLenValues == 0 {
		t.Error("PartitionKeysLenValues == 0; Init() should set it")
	}
	if tbl.ClusteringKeysLenValues == 0 {
		t.Error("ClusteringKeysLenValues == 0")
	}
	if tbl.TotalLenValues != tbl.PartitionKeysLenValues+tbl.ClusteringKeysLenValues+tbl.ColumnsLenValues {
		t.Error("TotalLenValues is inconsistent")
	}
}

func TestTable_ListColumns_NoListCols(t *testing.T) {
	t.Parallel()
	tbl := &typedef.Table{
		Name:    "no_list",
		Columns: typedef.Columns{{Name: "v", Type: typedef.TypeText}},
	}
	buildSchema(t, tbl)
	got := tbl.ListColumns()
	if len(got) != 0 {
		t.Errorf("ListColumns() = %v; want nil/empty for table without list columns", got)
	}
}

func TestTable_ListColumns_WithListCol(t *testing.T) {
	t.Parallel()
	tbl := &typedef.Table{
		Name: "with_list",
		PartitionKeys: typedef.Columns{
			{Name: "pk", Type: typedef.TypeInt},
		},
		Columns: typedef.Columns{
			{Name: "lst", Type: &typedef.Collection{ComplexType: typedef.TypeList, ValueType: typedef.TypeInt}},
			{Name: "txt", Type: typedef.TypeText},
		},
	}
	buildSchema(t, tbl)

	cols := tbl.ListColumns()
	if len(cols) != 1 {
		t.Fatalf("ListColumns() len = %d; want 1", len(cols))
	}
	if cols[0].Name != "lst" {
		t.Errorf("ListColumns()[0].Name = %q; want lst", cols[0].Name)
	}
	if cols[0].ValueType != typedef.TypeInt {
		t.Errorf("ListColumns()[0].ValueType = %q; want int", cols[0].ValueType)
	}
}

func TestTable_Lock_Unlock_NoMV(t *testing.T) {
	t.Parallel()
	// A table without MVs should actually acquire the lock.
	tbl := &typedef.Table{Name: "no_mv"}
	// Lock/Unlock should not panic and should be balanced.
	tbl.Lock()
	tbl.Unlock()
}

func TestTable_RLock_RUnlock_NoMV(t *testing.T) {
	t.Parallel()
	tbl := &typedef.Table{Name: "no_mv_r"}
	tbl.RLock()
	tbl.RUnlock()
}

func TestTable_Lock_WithMV_IsNoOp(t *testing.T) {
	t.Parallel()
	// A table with MVs: Lock/Unlock return immediately (no-op).
	// Calling Lock twice (without Unlock) would deadlock for a real mutex,
	// but since MVs make it a no-op it must not block.
	tbl := &typedef.Table{
		Name:              "with_mv",
		MaterializedViews: []typedef.MaterializedView{{Name: "mv1"}},
	}
	done := make(chan struct{})
	go func() {
		tbl.Lock()
		tbl.Lock() // would deadlock on a real mutex
		tbl.Unlock()
		tbl.Unlock()
		close(done)
	}()
	select {
	case <-done:
		// Good - no deadlock
	case <-time.After(100 * time.Millisecond):
		t.Error("Lock() with MVs appears to block (should be no-op)")
	}
}

func TestTable_LinkIndexAndColumns(t *testing.T) {
	t.Parallel()

	col1 := typedef.ColumnDef{Name: "col1", Type: typedef.TypeInt}
	col2 := typedef.ColumnDef{Name: "col2", Type: typedef.TypeText}

	tbl := &typedef.Table{
		Name:    "link_tbl",
		Columns: typedef.Columns{col1, col2},
		Indexes: []typedef.IndexDef{
			{IndexName: "idx_col1", ColumnName: "col1"},
			{IndexName: "idx_col2", ColumnName: "col2"},
		},
	}

	tbl.LinkIndexAndColumns()

	if tbl.Indexes[0].Column.Name != "col1" {
		t.Errorf("Indexes[0].Column.Name = %q; want col1", tbl.Indexes[0].Column.Name)
	}
	if tbl.Indexes[1].Column.Name != "col2" {
		t.Errorf("Indexes[1].Column.Name = %q; want col2", tbl.Indexes[1].Column.Name)
	}
}

func TestTable_LinkIndexAndColumns_UnknownColumnRemainsZero(t *testing.T) {
	t.Parallel()

	tbl := &typedef.Table{
		Name:    "link_tbl2",
		Columns: typedef.Columns{{Name: "col1", Type: typedef.TypeInt}},
		Indexes: []typedef.IndexDef{
			{IndexName: "idx_missing", ColumnName: "nonexistent"},
		},
	}
	tbl.LinkIndexAndColumns()

	// Column was not found — Index.Column should remain zero value.
	if tbl.Indexes[0].Column.Name != "" {
		t.Errorf("Indexes[0].Column.Name = %q; want empty (not linked)", tbl.Indexes[0].Column.Name)
	}
}

func TestTable_ListColumns_LazyBuild(t *testing.T) {
	t.Parallel()
	// Call ListColumns WITHOUT Init() — should still work.
	tbl := &typedef.Table{
		Name:    "lazy",
		Columns: typedef.Columns{{Name: "v", Type: typedef.TypeText}},
	}
	got := tbl.ListColumns()
	if len(got) != 0 {
		t.Errorf("ListColumns() without Init() = %v; want empty", got)
	}
}
