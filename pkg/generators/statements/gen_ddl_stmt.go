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

package statements

import (
	"fmt"
	"strings"

	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/builders"
	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/typedef"
)

func GenDDLStmt(s *typedef.Schema, t *typedef.Table, r *rand.Rand, sc *typedef.SchemaConfig) (*typedef.Stmts, error) {
	maxVariant := 1
	validCols := t.ValidColumnsForDelete()
	if validCols.Len() > 0 {
		maxVariant = 2
	}
	switch n := r.Intn(maxVariant + 2); n {
	//case 0: // Alter column not supported in Cassandra from 3.0.11
	//	return t.alterColumn(s.Keyspace.Name)
	case 2:
		return genDropColumnStmt(t, s.Keyspace.Name, validCols.Random(r))
	default:
		column := typedef.ColumnDef{
			Name: generators.GenColumnName("col", len(t.Columns)+1),
			Type: generators.GenColumnType(len(t.Columns)+1, sc, r),
		}
		return genAddColumnStmt(t, s.Keyspace.Name, &column)
	}
}

func appendValue(columnType typedef.Type, r *rand.Rand, p *typedef.PartitionRangeConfig, values []any) []any {
	return append(values, columnType.GenValue(r, p)...)
}

func genAddColumnStmt(t *typedef.Table, keyspace string, column *typedef.ColumnDef) (*typedef.Stmts, error) {
	var stmts []*typedef.Stmt
	if c, ok := column.Type.(*typedef.UDTType); ok {
		createType := "CREATE TYPE IF NOT EXISTS %s.%s (%s);"
		var typs []string
		for name, typ := range c.ValueTypes {
			typs = append(typs, name+" "+typ.CQLDef())
		}
		stmt := fmt.Sprintf(createType, keyspace, c.TypeName, strings.Join(typs, ","))
		stmts = append(stmts, &typedef.Stmt{
			StmtCache: &typedef.StmtCache{
				Query: &builders.AlterTableBuilder{
					Stmt: stmt,
				},
			},
		})
	}
	stmt := "ALTER TABLE " + keyspace + "." + t.Name + " ADD " + column.Name + " " + column.Type.CQLDef()
	stmts = append(stmts, &typedef.Stmt{
		StmtCache: &typedef.StmtCache{
			Query: &builders.AlterTableBuilder{
				Stmt: stmt,
			},
		},
	})
	return &typedef.Stmts{
		List:      stmts,
		QueryType: typedef.AddColumnStatementType,
		PostStmtHook: func() {
			t.Columns = append(t.Columns, column)
			t.ResetQueryCache()
		},
	}, nil
}

func genDropColumnStmt(t *typedef.Table, keyspace string, column *typedef.ColumnDef) (*typedef.Stmts, error) {
	var stmts []*typedef.Stmt

	stmt := "ALTER TABLE " + keyspace + "." + t.Name + " DROP " + column.Name
	stmts = append(stmts, &typedef.Stmt{
		StmtCache: &typedef.StmtCache{
			Query: &builders.AlterTableBuilder{
				Stmt: stmt,
			},
			QueryType: typedef.DropColumnStatementType,
		},
	})
	return &typedef.Stmts{
		List:      stmts,
		QueryType: typedef.DropColumnStatementType,
		PostStmtHook: func() {
			t.Columns = t.Columns.Remove(column)
			t.ResetQueryCache()
		},
	}, nil
}
