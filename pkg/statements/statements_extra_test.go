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

package statements_test

import (
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/google/uuid"

	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/statements"
	"github.com/scylladb/gemini/pkg/typedef"
)

// ---------------------------------------------------------------------------
// mockPartitions is a minimal stub for partitions.Interface.
// It is NOT thread-safe: callers must not invoke methods concurrently.
// ---------------------------------------------------------------------------

type mockPartitions struct {
	table  *typedef.Table
	random *rand.Rand
}

func newMockPartitions(table *typedef.Table) *mockPartitions {
	return &mockPartitions{
		table:  table,
		random: rand.New(rand.NewChaCha8([32]byte{})),
	}
}

func (m *mockPartitions) makePK() typedef.PartitionKeys {
	data := make(map[string][]any, len(m.table.PartitionKeys))
	cfg := typedef.ValueRangeConfig{MaxStringLength: 10, MinStringLength: 1}
	for _, pk := range m.table.PartitionKeys {
		data[pk.Name] = pk.Type.GenValue(m.random, &cfg)
	}
	return typedef.PartitionKeys{
		Values:  typedef.NewValuesFromMap(data),
		ID:      uuid.New(),
		Release: func() {},
	}
}

func (m *mockPartitions) Stats() partitions.Stats { return partitions.Stats{} }

func (m *mockPartitions) Get(_ uint64) typedef.PartitionKeys         { return m.makePK() }
func (m *mockPartitions) Next() typedef.PartitionKeys                { return m.makePK() }
func (m *mockPartitions) Extend() typedef.PartitionKeys              { return m.makePK() }
func (m *mockPartitions) ReplaceNext() typedef.PartitionKeys         { return m.makePK() }
func (m *mockPartitions) Replace(_ uint64) typedef.PartitionKeys     { return m.makePK() }
func (m *mockPartitions) ReplaceWithoutOld(_ uint64)                 {}
func (m *mockPartitions) ReplaceNextWithoutOld()                     {}
func (m *mockPartitions) Deleted() <-chan typedef.PartitionKeys      { return nil }
func (m *mockPartitions) ValidationSuccess(_ *typedef.PartitionKeys) {}
func (m *mockPartitions) ValidationFailure(_ *typedef.PartitionKeys) {}
func (m *mockPartitions) ValidationStats(_ uuid.UUID) (first, last, failure uint64, recent []uint64, successCount uint64) {
	return 0, 0, 0, nil, 0
}
func (m *mockPartitions) Len() uint64 { return 100 }
func (m *mockPartitions) Close()      {}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func defaultRatioController(tb testing.TB) *statements.RatioController {
	tb.Helper()
	r := rand.New(rand.NewChaCha8([32]byte{}))
	rc, err := statements.NewRatioController(statements.DefaultStatementRatios(), r)
	if err != nil {
		tb.Fatalf("NewRatioController: %v", err)
	}
	return rc
}

func simpleTable(name string) *typedef.Table {
	return &typedef.Table{
		Name: name,
		PartitionKeys: typedef.Columns{
			{Name: "pk0", Type: typedef.TypeText},
		},
	}
}

func tableWithCK(name string) *typedef.Table {
	return &typedef.Table{
		Name:          name,
		PartitionKeys: typedef.Columns{{Name: "pk0", Type: typedef.TypeText}},
		ClusteringKeys: typedef.Columns{
			{Name: "ck0", Type: typedef.TypeInt},
		},
		Columns: typedef.Columns{
			{Name: "col0", Type: typedef.TypeInt},
		},
	}
}

func newGenerator(tb testing.TB, table *typedef.Table) *statements.Generator {
	tb.Helper()
	r := rand.New(rand.NewChaCha8([32]byte{}))
	rc := defaultRatioController(tb)
	vrc := &typedef.ValueRangeConfig{MaxStringLength: 10, MinStringLength: 1}
	return statements.New("ks1", newMockPartitions(table), table, r, vrc, rc, false)
}

// ---------------------------------------------------------------------------
// GetCreateKeyspaces
// ---------------------------------------------------------------------------

func TestGetCreateKeyspaces(t *testing.T) {
	t.Parallel()

	schema := &typedef.Schema{
		Keyspace: typedef.Keyspace{
			Name:              "ks1",
			Replication:       replication.NewSimpleStrategy(),
			OracleReplication: replication.NewSimpleStrategy(),
		},
		Tables: []*typedef.Table{simpleTable("tbl0")},
	}

	sut, oracle := statements.GetCreateKeyspaces(schema)

	if !strings.HasPrefix(sut, "CREATE KEYSPACE IF NOT EXISTS ks1") {
		t.Errorf("unexpected SUT stmt: %q", sut)
	}
	if !strings.HasPrefix(oracle, "CREATE KEYSPACE IF NOT EXISTS ks1") {
		t.Errorf("unexpected oracle stmt: %q", oracle)
	}
}

// ---------------------------------------------------------------------------
// GetCreateSchema
// ---------------------------------------------------------------------------

func TestGetCreateSchemaContent(t *testing.T) {
	t.Parallel()

	ks := typedef.Keyspace{Name: "ks1", Replication: replication.NewSimpleStrategy()}

	t.Run("single_table_no_indexes_no_mvs", func(t *testing.T) {
		t.Parallel()
		schema := &typedef.Schema{
			Keyspace: ks,
			Tables: []*typedef.Table{
				{
					Name:          "tbl0",
					PartitionKeys: createColumns(1, "pk"),
				},
			},
		}
		stmts := statements.GetCreateSchema(schema)
		if len(stmts) == 0 {
			t.Fatal("expected at least one statement")
		}
		found := false
		for _, s := range stmts {
			if strings.Contains(s, "CREATE TABLE IF NOT EXISTS ks1.tbl0") {
				found = true
			}
		}
		if !found {
			t.Errorf("expected CREATE TABLE statement, got: %v", stmts)
		}
	})

	t.Run("table_with_indexes", func(t *testing.T) {
		t.Parallel()
		table := &typedef.Table{
			Name:          "tbl1",
			PartitionKeys: createColumns(1, "pk"),
			Columns: typedef.Columns{
				{Name: "col0", Type: typedef.TypeInt},
			},
			Indexes: []typedef.IndexDef{
				{IndexName: "tbl1_col0_idx", ColumnName: "col0"},
			},
		}
		schema := &typedef.Schema{Keyspace: ks, Tables: []*typedef.Table{table}}
		stmts := statements.GetCreateSchema(schema)

		hasIndex := false
		for _, s := range stmts {
			if strings.Contains(s, "CREATE INDEX IF NOT EXISTS tbl1_col0_idx") {
				hasIndex = true
			}
		}
		if !hasIndex {
			t.Errorf("expected CREATE INDEX statement; got %v", stmts)
		}
	})

	t.Run("table_with_udt_type", func(t *testing.T) {
		t.Parallel()
		udt := &typedef.UDTType{
			ComplexType: typedef.TypeUdt,
			TypeName:    "my_udt",
			ValueTypes:  map[string]typedef.SimpleType{"field1": typedef.TypeInt},
			Frozen:      true,
		}
		table := &typedef.Table{
			Name:          "tbl2",
			PartitionKeys: createColumns(1, "pk"),
			Columns: typedef.Columns{
				{Name: "col0", Type: udt},
			},
		}
		schema := &typedef.Schema{Keyspace: ks, Tables: []*typedef.Table{table}}
		stmts := statements.GetCreateSchema(schema)

		hasType := false
		for _, s := range stmts {
			if strings.Contains(s, "CREATE TYPE IF NOT EXISTS ks1.my_udt") {
				hasType = true
			}
		}
		if !hasType {
			t.Errorf("expected CREATE TYPE statement; got %v", stmts)
		}
	})
}

// ---------------------------------------------------------------------------
// GetDropKeyspace
// ---------------------------------------------------------------------------

func TestGetDropKeyspace(t *testing.T) {
	t.Parallel()

	schema := &typedef.Schema{
		Keyspace: typedef.Keyspace{Name: "ks_drop"},
		Tables:   []*typedef.Table{simpleTable("t")},
	}

	stmts := statements.GetDropKeyspace(schema)
	if len(stmts) != 1 {
		t.Fatalf("expected 1 statement, got %d", len(stmts))
	}
	if stmts[0] != "DROP KEYSPACE IF EXISTS ks_drop" {
		t.Errorf("got %q", stmts[0])
	}
}

// ---------------------------------------------------------------------------
// GetCreateTypes
// ---------------------------------------------------------------------------

func TestGetCreateTypes(t *testing.T) {
	t.Parallel()

	ks := typedef.Keyspace{Name: "ks1"}

	t.Run("no_udt", func(t *testing.T) {
		t.Parallel()
		table := &typedef.Table{
			Name:          "t",
			PartitionKeys: createColumns(1, "pk"),
			Columns:       typedef.Columns{{Name: "c0", Type: typedef.TypeText}},
		}
		stmts := statements.GetCreateTypes(table, ks)
		if len(stmts) != 0 {
			t.Errorf("expected 0 stmts for non-UDT table, got %v", stmts)
		}
	})

	t.Run("with_udt", func(t *testing.T) {
		t.Parallel()
		udt := &typedef.UDTType{
			ComplexType: typedef.TypeUdt,
			TypeName:    "addr",
			ValueTypes:  map[string]typedef.SimpleType{"street": typedef.TypeText, "zip": typedef.TypeInt},
			Frozen:      true,
		}
		table := &typedef.Table{
			Name:          "t2",
			PartitionKeys: createColumns(1, "pk"),
			Columns:       typedef.Columns{{Name: "address", Type: udt}},
		}
		stmts := statements.GetCreateTypes(table, ks)
		if len(stmts) != 1 {
			t.Fatalf("expected 1 stmt, got %d: %v", len(stmts), stmts)
		}
		if !strings.Contains(stmts[0], "CREATE TYPE IF NOT EXISTS ks1.addr") {
			t.Errorf("unexpected stmt: %q", stmts[0])
		}
	})
}

// ---------------------------------------------------------------------------
// GenIndexName
// ---------------------------------------------------------------------------

func TestGenIndexName(t *testing.T) {
	t.Parallel()

	got := statements.GenIndexName("my_table_col", 3)
	want := "my_table_col3_idx"
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// CreateIndexesForColumn
// ---------------------------------------------------------------------------

func TestCreateIndexesForColumn(t *testing.T) {
	t.Parallel()

	t.Run("no_indexable_columns", func(t *testing.T) {
		t.Parallel()
		table := &typedef.Table{
			Name: "t",
			Columns: typedef.Columns{
				{Name: "col0", Type: typedef.TypeText}, // not in TypesForIndex
			},
		}
		idxs := statements.CreateIndexesForColumn(table, 5)
		if len(idxs) != 0 {
			t.Errorf("expected 0 indexes for text column, got %d", len(idxs))
		}
	})

	t.Run("indexable_columns_capped_by_max", func(t *testing.T) {
		t.Parallel()
		table := &typedef.Table{
			Name: "t",
			Columns: typedef.Columns{
				{Name: "col0", Type: typedef.TypeInt},
				{Name: "col1", Type: typedef.TypeFloat},
				{Name: "col2", Type: typedef.TypeDouble},
			},
		}
		idxs := statements.CreateIndexesForColumn(table, 2)
		if len(idxs) != 2 {
			t.Errorf("expected 2 indexes (capped), got %d", len(idxs))
		}
	})

	t.Run("fewer_indexable_than_max", func(t *testing.T) {
		t.Parallel()
		table := &typedef.Table{
			Name: "t",
			Columns: typedef.Columns{
				{Name: "col0", Type: typedef.TypeInt},
				{Name: "col1", Type: typedef.TypeText}, // not indexable
			},
		}
		idxs := statements.CreateIndexesForColumn(table, 10)
		if len(idxs) != 1 {
			t.Errorf("expected 1 index, got %d", len(idxs))
		}
		if idxs[0].ColumnName != "col0" {
			t.Errorf("unexpected column: %q", idxs[0].ColumnName)
		}
	})
}

// ---------------------------------------------------------------------------
// CreateMaterializedViews
// ---------------------------------------------------------------------------

func TestCreateMaterializedViews(t *testing.T) {
	t.Parallel()

	r := rand.New(rand.NewChaCha8([32]byte{}))
	columns := typedef.Columns{
		{Name: "col0", Type: typedef.TypeInt},
		{Name: "col1", Type: typedef.TypeFloat},
	}
	pks := typedef.Columns{{Name: "pk0", Type: typedef.TypeText}}
	cks := typedef.Columns{{Name: "ck0", Type: typedef.TypeInt}}

	mvs := statements.CreateMaterializedViews(columns, "tbl0", pks, cks, r)
	if len(mvs) != 1 {
		t.Fatalf("expected 1 materialized view, got %d", len(mvs))
	}
	mv := mvs[0]
	if !strings.HasPrefix(mv.Name, "tbl0_mv_") {
		t.Errorf("unexpected mv name: %q", mv.Name)
	}
	if len(mv.PartitionKeys) == 0 {
		t.Error("expected at least one partition key in mv")
	}
}

// ---------------------------------------------------------------------------
// GetValidationStatementType
// ---------------------------------------------------------------------------

func TestGetValidationStatementType(t *testing.T) {
	t.Parallel()
	rc := defaultRatioController(t)
	got := rc.GetValidationStatementType()
	if got != statements.StatementTypeSelect {
		t.Errorf("expected StatementTypeSelect (%d), got %d", statements.StatementTypeSelect, got)
	}
}

// ---------------------------------------------------------------------------
// GetSelectSubtype distribution
// ---------------------------------------------------------------------------

func TestGetSelectSubtypeReturnsValidRange(t *testing.T) {
	t.Parallel()
	rc := defaultRatioController(t)
	for range 1000 {
		v := rc.GetSelectSubtype()
		if v < 0 || v >= statements.SelectStatementsCount {
			t.Fatalf("GetSelectSubtype() = %d, out of [0, %d)", v, statements.SelectStatementsCount)
		}
	}
}

// ---------------------------------------------------------------------------
// Generator.Insert
// ---------------------------------------------------------------------------

func TestGeneratorInsert(t *testing.T) {
	t.Parallel()

	table := tableWithCK("tbl_insert")
	gen := newGenerator(t, table)

	stmt, err := gen.Insert(t.Context())
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if stmt == nil {
		t.Fatal("expected non-nil stmt")
	}
	if !strings.Contains(stmt.Query, "INSERT INTO") {
		t.Errorf("expected INSERT INTO in query, got %q", stmt.Query)
	}
	if stmt.QueryType != typedef.InsertStatementType {
		t.Errorf("expected InsertStatementType, got %v", stmt.QueryType)
	}
	if len(stmt.PartitionKeys) == 0 {
		t.Error("expected at least one partition key")
	}
}

// ---------------------------------------------------------------------------
// Generator.InsertJSON
// ---------------------------------------------------------------------------

func TestGeneratorInsertJSON(t *testing.T) {
	t.Parallel()

	table := tableWithCK("tbl_insert_json")
	// Remove KnownIssues so JSON is allowed
	table.KnownIssues = map[string]bool{}
	gen := newGenerator(t, table)

	stmt, err := gen.InsertJSON(t.Context())
	if err != nil {
		t.Fatalf("InsertJSON: %v", err)
	}
	if stmt == nil {
		t.Fatal("expected non-nil stmt")
	}
	if !strings.Contains(stmt.Query, "INSERT INTO") {
		t.Errorf("expected INSERT INTO in query, got %q", stmt.Query)
	}
	if stmt.QueryType != typedef.InsertJSONStatementType {
		t.Errorf("expected InsertJSONStatementType, got %v", stmt.QueryType)
	}
}

// ---------------------------------------------------------------------------
// Generator.InsertJSON returns nil for counter table
// ---------------------------------------------------------------------------

func TestGeneratorInsertJSONCounterTable(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name:          "tbl_counter",
		PartitionKeys: typedef.Columns{{Name: "pk0", Type: typedef.TypeText}},
		Columns:       typedef.Columns{{Name: "cnt", Type: &typedef.CounterType{}}},
	}
	gen := newGenerator(t, table)

	stmt, err := gen.InsertJSON(t.Context())
	if err != nil {
		t.Fatalf("InsertJSON on counter table: %v", err)
	}
	if stmt != nil {
		t.Error("expected nil stmt for counter table")
	}
}

// ---------------------------------------------------------------------------
// Generator.Update
// ---------------------------------------------------------------------------

func TestGeneratorUpdate(t *testing.T) {
	t.Parallel()

	table := tableWithCK("tbl_update")
	gen := newGenerator(t, table)

	stmt, err := gen.Update(t.Context())
	if err != nil {
		t.Fatalf("Update: %v", err)
	}
	if stmt == nil {
		t.Fatal("expected non-nil stmt")
	}
	if !strings.Contains(stmt.Query, "UPDATE") {
		t.Errorf("expected UPDATE in query, got %q", stmt.Query)
	}
	if stmt.QueryType != typedef.UpdateStatementType {
		t.Errorf("expected UpdateStatementType, got %v", stmt.QueryType)
	}
}

// ---------------------------------------------------------------------------
// Generator.Delete
// ---------------------------------------------------------------------------

func TestGeneratorDelete(t *testing.T) {
	t.Parallel()

	table := simpleTable("tbl_delete")
	gen := newGenerator(t, table)

	stmt, err := gen.Delete(t.Context())
	if err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if stmt == nil {
		t.Fatal("expected non-nil stmt")
	}
	if !strings.Contains(stmt.Query, "DELETE") {
		t.Errorf("expected DELETE in query, got %q", stmt.Query)
	}
}

// ---------------------------------------------------------------------------
// Generator.Select
// ---------------------------------------------------------------------------

func TestGeneratorSelect(t *testing.T) {
	t.Parallel()

	table := simpleTable("tbl_select")
	gen := newGenerator(t, table)

	// Run many times to exercise different select subtypes
	for range 20 {
		stmt, err := gen.Select(t.Context())
		if err != nil {
			t.Fatalf("Select: %v", err)
		}
		if stmt == nil {
			t.Fatal("expected non-nil stmt")
		}
		if !strings.Contains(stmt.Query, "SELECT") {
			t.Errorf("expected SELECT in query, got %q", stmt.Query)
		}
	}
}

// ---------------------------------------------------------------------------
// Generator.MutateStatement
// ---------------------------------------------------------------------------

func TestGeneratorMutateStatement(t *testing.T) {
	t.Parallel()

	table := tableWithCK("tbl_mutate")
	table.KnownIssues = map[string]bool{typedef.KnownIssuesJSONWithTuples: true}
	gen := newGenerator(t, table)

	// Run many times to exercise all mutation paths
	for range 30 {
		stmt, err := gen.MutateStatement(t.Context(), true)
		if err != nil {
			t.Fatalf("MutateStatement: %v", err)
		}
		if stmt == nil {
			t.Fatal("expected non-nil stmt")
		}
	}
}

func TestGeneratorMutateStatementNoDelete(t *testing.T) {
	t.Parallel()

	table := tableWithCK("tbl_mutate_nodelete")
	table.KnownIssues = map[string]bool{typedef.KnownIssuesJSONWithTuples: true}
	gen := newGenerator(t, table)

	for range 20 {
		stmt, err := gen.MutateStatement(t.Context(), false)
		if err != nil {
			t.Fatalf("MutateStatement(noDelete): %v", err)
		}
		if stmt == nil {
			t.Fatal("expected non-nil stmt")
		}
		if stmt.QueryType == typedef.DeleteWholePartitionType ||
			stmt.QueryType == typedef.DeleteMultiplePartitionsType {
			t.Errorf("got delete statement when generateDelete=false: %v", stmt.QueryType)
		}
	}
}

// ---------------------------------------------------------------------------
// Generator.New — basic smoke
// ---------------------------------------------------------------------------

func TestGeneratorNew(t *testing.T) {
	t.Parallel()

	table := simpleTable("tbl_new")
	r := rand.New(rand.NewChaCha8([32]byte{}))
	rc := defaultRatioController(t)
	vrc := &typedef.ValueRangeConfig{MaxStringLength: 10, MinStringLength: 1}

	gen := statements.New("ks1", newMockPartitions(table), table, r, vrc, rc, false)
	if gen == nil {
		t.Fatal("expected non-nil generator")
	}
}

func TestGeneratorNewWithLWT(t *testing.T) {
	t.Parallel()

	table := tableWithCK("tbl_lwt")
	r := rand.New(rand.NewChaCha8([32]byte{}))
	rc := defaultRatioController(t)
	vrc := &typedef.ValueRangeConfig{MaxStringLength: 10, MinStringLength: 1}

	gen := statements.New("ks1", newMockPartitions(table), table, r, vrc, rc, true)
	if gen == nil {
		t.Fatal("expected non-nil generator")
	}

	// Verify Insert works when useLWT=true
	for range 10 {
		stmt, err := gen.Insert(t.Context())
		if err != nil {
			t.Fatalf("Insert with LWT: %v", err)
		}
		if stmt == nil {
			t.Fatal("expected non-nil stmt")
		}
	}
}
