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
	"encoding/json"
	"testing"

	"github.com/scylladb/gemini/pkg/typedef"
)

// ---------------------------------------------------------------------------
// StatementType.String()
// ---------------------------------------------------------------------------

func TestStatementType_String(t *testing.T) {
	t.Parallel()

	cases := []struct {
		want string
		st   typedef.StatementType
	}{
		{"SelectStatement", typedef.SelectStatementType},
		{"SelectRangeStatement", typedef.SelectRangeStatementType},
		{"SelectMultiPartitionType", typedef.SelectMultiPartitionType},
		{"SelectMultiPartitionRangeStatementType", typedef.SelectMultiPartitionRangeStatementType},
		{"SelectByIndexStatement", typedef.SelectByIndexStatementType},
		{"SelectFromMaterializedViewStatement", typedef.SelectFromMaterializedViewStatementType},
		{"DeleteSingleRow", typedef.DeleteSingleRowType},
		{"DeleteSingleColumn", typedef.DeleteSingleColumnType},
		{"DeleteMultiplePartitions", typedef.DeleteMultiplePartitionsType},
		{"DeleteWholePartition", typedef.DeleteWholePartitionType},
		{"InsertStatement", typedef.InsertStatementType},
		{"InsertJSONStatement", typedef.InsertJSONStatementType},
		{"UpdateStatement", typedef.UpdateStatementType},
		{"AlterColumnStatement", typedef.AlterColumnStatementType},
		{"DropColumnStatement", typedef.DropColumnStatementType},
		{"AddColumnStatement", typedef.AddColumnStatementType},
		{"DropKeyspaceStatement", typedef.DropKeyspaceStatementType},
		{"CreateKeyspaceStatement", typedef.CreateKeyspaceStatementType},
		{"CreateSchemaStatement", typedef.CreateSchemaStatementType},
		{"CreateIndexStatement", typedef.CreateIndexStatementType},
		{"DropIndexStatement", typedef.DropIndexStatementType},
		{"CreateTypeStatement", typedef.CreateTypeStatementType},
		{"DropTypeStatement", typedef.DropTypeStatementType},
		{"CreateTableStatement", typedef.CreateTableStatementType},
		{"DropTableStatement", typedef.DropTableStatementType},
	}

	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			if got := tc.st.String(); got != tc.want {
				t.Errorf("StatementType(%d).String() = %q; want %q", tc.st, got, tc.want)
			}
		})
	}
}

func TestStatementType_String_UnknownPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for unknown StatementType")
		}
	}()
	_ = typedef.StatementTypeCount.String()
}

// ---------------------------------------------------------------------------
// StatementType.IsSelect / IsInsert / IsUpdate / IsDelete / IsSchema
// ---------------------------------------------------------------------------

func TestStatementType_IsSelect(t *testing.T) {
	t.Parallel()

	selects := []typedef.StatementType{
		typedef.SelectStatementType,
		typedef.SelectRangeStatementType,
		typedef.SelectMultiPartitionType,
		typedef.SelectMultiPartitionRangeStatementType,
		typedef.SelectByIndexStatementType,
		typedef.SelectFromMaterializedViewStatementType,
	}
	nonSelects := []typedef.StatementType{
		typedef.InsertStatementType,
		typedef.UpdateStatementType,
		typedef.DeleteSingleRowType,
	}

	for _, st := range selects {
		if !st.IsSelect() {
			t.Errorf("%s.IsSelect() = false; want true", st)
		}
	}
	for _, st := range nonSelects {
		if st.IsSelect() {
			t.Errorf("%s.IsSelect() = true; want false", st)
		}
	}
}

func TestStatementType_IsInsert(t *testing.T) {
	t.Parallel()
	if !typedef.InsertStatementType.IsInsert() {
		t.Error("InsertStatementType.IsInsert() = false")
	}
	if !typedef.InsertJSONStatementType.IsInsert() {
		t.Error("InsertJSONStatementType.IsInsert() = false")
	}
	if typedef.UpdateStatementType.IsInsert() {
		t.Error("UpdateStatementType.IsInsert() = true")
	}
}

func TestStatementType_IsUpdate(t *testing.T) {
	t.Parallel()
	if !typedef.UpdateStatementType.IsUpdate() {
		t.Error("UpdateStatementType.IsUpdate() = false")
	}
	if typedef.InsertStatementType.IsUpdate() {
		t.Error("InsertStatementType.IsUpdate() = true")
	}
}

func TestStatementType_IsDelete(t *testing.T) {
	t.Parallel()
	deletes := []typedef.StatementType{
		typedef.DeleteWholePartitionType,
		typedef.DeleteMultiplePartitionsType,
		typedef.DeleteSingleColumnType,
		typedef.DeleteSingleRowType,
	}
	for _, st := range deletes {
		if !st.IsDelete() {
			t.Errorf("%s.IsDelete() = false; want true", st)
		}
	}
	if typedef.UpdateStatementType.IsDelete() {
		t.Error("UpdateStatementType.IsDelete() = true; want false")
	}
}

func TestStatementType_IsSchema(t *testing.T) {
	t.Parallel()
	schemas := []typedef.StatementType{
		typedef.AlterColumnStatementType,
		typedef.DropColumnStatementType,
		typedef.AddColumnStatementType,
		typedef.DropTableStatementType,
		typedef.CreateTableStatementType,
		typedef.DropTypeStatementType,
		typedef.CreateTypeStatementType,
		typedef.DropIndexStatementType,
		typedef.CreateIndexStatementType,
		typedef.CreateKeyspaceStatementType,
		typedef.DropKeyspaceStatementType,
		typedef.CreateSchemaStatementType,
	}
	for _, st := range schemas {
		if !st.IsSchema() {
			t.Errorf("%s.IsSchema() = false; want true", st)
		}
	}
	if typedef.SelectStatementType.IsSchema() {
		t.Error("SelectStatementType.IsSchema() = true; want false")
	}
}

// ---------------------------------------------------------------------------
// StatementType.OpType()
// ---------------------------------------------------------------------------

func TestStatementType_OpType(t *testing.T) {
	t.Parallel()

	cases := []struct {
		st   typedef.StatementType
		want typedef.OpType
	}{
		{typedef.SelectStatementType, typedef.OpSelect},
		{typedef.SelectRangeStatementType, typedef.OpSelect},
		{typedef.InsertStatementType, typedef.OpInsert},
		{typedef.InsertJSONStatementType, typedef.OpInsert},
		{typedef.UpdateStatementType, typedef.OpUpdate},
		{typedef.DeleteSingleRowType, typedef.OpDelete},
		{typedef.DeleteWholePartitionType, typedef.OpDelete},
		{typedef.AlterColumnStatementType, typedef.OpSchemaAlter},
		{typedef.DropKeyspaceStatementType, typedef.OpSchemaDrop},
		{typedef.CreateKeyspaceStatementType, typedef.OpSchemaCreate},
		{typedef.CreateSchemaStatementType, typedef.OpSchemaCreate},
	}

	for _, tc := range cases {
		t.Run(tc.st.String(), func(t *testing.T) {
			t.Parallel()
			if got := tc.st.OpType(); got != tc.want {
				t.Errorf("%s.OpType() = %v; want %v", tc.st, got, tc.want)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// StatementType.PossibleAsyncOperation()
// ---------------------------------------------------------------------------

func TestStatementType_PossibleAsyncOperation(t *testing.T) {
	t.Parallel()

	asyncTypes := []typedef.StatementType{
		typedef.SelectByIndexStatementType,
		typedef.SelectFromMaterializedViewStatementType,
	}
	syncTypes := []typedef.StatementType{
		typedef.SelectStatementType,
		typedef.InsertStatementType,
		typedef.UpdateStatementType,
	}
	for _, st := range asyncTypes {
		if !st.PossibleAsyncOperation() {
			t.Errorf("%s.PossibleAsyncOperation() = false; want true", st)
		}
	}
	for _, st := range syncTypes {
		if st.PossibleAsyncOperation() {
			t.Errorf("%s.PossibleAsyncOperation() = true; want false", st)
		}
	}
}

// ---------------------------------------------------------------------------
// OpType.String()
// ---------------------------------------------------------------------------

func TestOpType_String(t *testing.T) {
	t.Parallel()

	cases := []struct {
		want string
		op   typedef.OpType
	}{
		{"Select", typedef.OpSelect},
		{"Insert", typedef.OpInsert},
		{"Update", typedef.OpUpdate},
		{"Delete", typedef.OpDelete},
		{"SchemaAlter", typedef.OpSchemaAlter},
		{"SchemaDrop", typedef.OpSchemaDrop},
		{"SchemaCreate", typedef.OpSchemaCreate},
	}
	for _, tc := range cases {
		t.Run(tc.want, func(t *testing.T) {
			t.Parallel()
			if got := tc.op.String(); got != tc.want {
				t.Errorf("OpType(%d).String() = %q; want %q", tc.op, got, tc.want)
			}
		})
	}
}

func TestOpType_String_UnknownPanics(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for unknown OpType")
		}
	}()
	_ = typedef.OpCount.String()
}

// ---------------------------------------------------------------------------
// OpType.IsSelect / IsInsert / IsUpdate / IsDelete / IsSchemaChange
// ---------------------------------------------------------------------------

func TestOpType_Predicates(t *testing.T) {
	t.Parallel()

	cases := []struct {
		op     typedef.OpType
		sel    bool
		ins    bool
		upd    bool
		del    bool
		schema bool
	}{
		{typedef.OpSelect, true, false, false, false, false},
		{typedef.OpInsert, false, true, false, false, false},
		{typedef.OpUpdate, false, false, true, false, false},
		{typedef.OpDelete, false, false, false, true, false},
		{typedef.OpSchemaAlter, false, false, false, false, true},
		{typedef.OpSchemaDrop, false, false, false, false, true},
		{typedef.OpSchemaCreate, false, false, false, false, true},
	}

	for _, tc := range cases {
		t.Run(tc.op.String(), func(t *testing.T) {
			t.Parallel()
			if tc.op.IsSelect() != tc.sel {
				t.Errorf("IsSelect() = %v; want %v", tc.op.IsSelect(), tc.sel)
			}
			if tc.op.IsInsert() != tc.ins {
				t.Errorf("IsInsert() = %v; want %v", tc.op.IsInsert(), tc.ins)
			}
			if tc.op.IsUpdate() != tc.upd {
				t.Errorf("IsUpdate() = %v; want %v", tc.op.IsUpdate(), tc.upd)
			}
			if tc.op.IsDelete() != tc.del {
				t.Errorf("IsDelete() = %v; want %v", tc.op.IsDelete(), tc.del)
			}
			if tc.op.IsSchemaChange() != tc.schema {
				t.Errorf("IsSchemaChange() = %v; want %v", tc.op.IsSchemaChange(), tc.schema)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Values
// ---------------------------------------------------------------------------

func TestValues_NewValues(t *testing.T) {
	t.Parallel()
	v := typedef.NewValues(4)
	if v == nil {
		t.Fatal("NewValues returned nil")
	}
	if v.Len() != 0 {
		t.Errorf("Len() = %d; want 0", v.Len())
	}
}

func TestValues_NewValuesFromMap(t *testing.T) {
	t.Parallel()
	m := map[string][]any{"col1": {1, 2}, "col2": {"a"}}
	v := typedef.NewValuesFromMap(m)
	if v.Len() != 2 {
		t.Errorf("Len() = %d; want 2", v.Len())
	}
}

func TestValues_Get(t *testing.T) {
	t.Parallel()
	v := typedef.NewValuesFromMap(map[string][]any{"k": {42}})
	got := v.Get("k")
	if len(got) != 1 || got[0] != 42 {
		t.Errorf("Get(k) = %v; want [42]", got)
	}
	if v.Get("missing") != nil {
		t.Error("Get(missing) != nil")
	}
}

func TestValues_Keys(t *testing.T) {
	t.Parallel()
	v := typedef.NewValuesFromMap(map[string][]any{"b": {1}, "a": {2}, "c": {3}})
	keys := v.Keys()
	if len(keys) != 3 {
		t.Fatalf("Keys() len = %d; want 3", len(keys))
	}
	// Must be sorted.
	want := []string{"a", "b", "c"}
	for i, k := range want {
		if keys[i] != k {
			t.Errorf("Keys()[%d] = %q; want %q", i, keys[i], k)
		}
	}
}

func TestValues_Len(t *testing.T) {
	t.Parallel()
	v := typedef.NewValues(0)
	if v.Len() != 0 {
		t.Error("empty Len != 0")
	}
	v2 := typedef.NewValuesFromMap(map[string][]any{"x": {1}, "y": {2}})
	if v2.Len() != 2 {
		t.Errorf("Len() = %d; want 2", v2.Len())
	}
}

func TestValues_Merge(t *testing.T) {
	t.Parallel()
	base := typedef.NewValuesFromMap(map[string][]any{"a": {1}})
	extra := typedef.NewValuesFromMap(map[string][]any{"a": {2}, "b": {3}})
	base.Merge(extra)

	if got := base.Get("a"); len(got) != 2 {
		t.Errorf("after Merge, a = %v; want [1 2]", got)
	}
	if got := base.Get("b"); len(got) != 1 {
		t.Errorf("after Merge, b = %v; want [3]", got)
	}
}

func TestValues_Copy(t *testing.T) {
	t.Parallel()
	original := typedef.NewValuesFromMap(map[string][]any{"k": {"original"}})
	cp := original.Copy()
	if cp == original {
		t.Error("Copy() returned same pointer")
	}
	if got := cp.Get("k"); len(got) != 1 || got[0] != "original" {
		t.Errorf("Copy().Get(k) = %v; want [original]", got)
	}
}

func TestValues_Copy_NilIsNil(t *testing.T) {
	t.Parallel()
	var v *typedef.Values
	if v.Copy() != nil {
		t.Error("nil.Copy() != nil")
	}
}

func TestValues_Data(t *testing.T) {
	t.Parallel()
	v := typedef.NewValuesFromMap(map[string][]any{"b": {2}, "a": {1}})
	data := v.Data()
	// Sorted by key: a first, then b.
	if len(data) != 2 {
		t.Fatalf("Data() len = %d; want 2", len(data))
	}
	if data[0] != 1 {
		t.Errorf("Data()[0] = %v; want 1", data[0])
	}
	if data[1] != 2 {
		t.Errorf("Data()[1] = %v; want 2", data[1])
	}
}

func TestValues_MarshalUnmarshalJSON(t *testing.T) {
	t.Parallel()
	original := typedef.NewValuesFromMap(map[string][]any{"k": {float64(42)}})

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}

	var got typedef.Values
	if err = json.Unmarshal(data, &got); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if got.Len() != 1 {
		t.Errorf("after round-trip Len() = %d; want 1", got.Len())
	}
}

func TestValues_MarshalJSON_Nil(t *testing.T) {
	t.Parallel()
	var v *typedef.Values
	data, err := v.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON(nil): %v", err)
	}
	if string(data) != "null" {
		t.Errorf("MarshalJSON(nil) = %s; want null", data)
	}
}

func TestValues_UnmarshalJSON_Null(t *testing.T) {
	t.Parallel()
	v := typedef.NewValues(0)
	if err := json.Unmarshal([]byte("null"), v); err != nil {
		t.Fatalf("UnmarshalJSON(null): %v", err)
	}
	if v.Len() != 0 {
		t.Errorf("after null unmarshal Len() = %d; want 0", v.Len())
	}
}

func TestValues_ToCQLValues(t *testing.T) {
	t.Parallel()
	v := typedef.NewValuesFromMap(map[string][]any{
		"pk1": {1, 2},
		"pk2": {"x"},
	})
	pks := typedef.Columns{
		{Name: "pk1", Type: typedef.TypeInt},
		{Name: "pk2", Type: typedef.TypeText},
	}
	got := v.ToCQLValues(pks)
	// pk1 contributes 2 values, pk2 contributes 1.
	if len(got) != 3 {
		t.Fatalf("ToCQLValues len = %d; want 3", len(got))
	}
}
