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

// nolint
package jobs

import (
	"fmt"

	"github.com/scylladb/gemini/pkg/statements"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

const (
	DDLAddColumnStatement = iota
	DDLDropColumnStatement
	DDLStatements
)

func GenDDLStmt(
	s *typedef.Schema,
	t *typedef.Table,
	r utils.Random,
	_ *typedef.PartitionRangeConfig,
	sc typedef.SchemaConfig,
) (*typedef.Stmts, error) {
	validCols := t.ValidColumnsForDelete()
	if validCols.Len() == 0 {
		return genAddColumnStmt(t, s.Keyspace.Name, &typedef.ColumnDef{
			Name: statements.GenColumnName("col", len(t.Columns)+1),
			Type: statements.GenColumnType(len(t.Columns)+1, &sc, r),
		})
	}

	switch n := r.IntN(DDLStatements); n {
	case DDLAddColumnStatement:
		return genAddColumnStmt(t, s.Keyspace.Name, &typedef.ColumnDef{
			Name: statements.GenColumnName("col", len(t.Columns)+1),
			Type: statements.GenColumnType(len(t.Columns)+1, &sc, r),
		})
	case DDLDropColumnStatement:
		return genDropColumnStmt(t, s.Keyspace.Name, validCols.Random(r))
	default:
		panic("invalid DDL statement type: " + fmt.Sprint(n))
	}
}

func genAddColumnStmt(
	t *typedef.Table,
	keyspace string,
	column *typedef.ColumnDef,
) (*typedef.Stmts, error) {
	return nil, nil
	//var stmts []*typedef.Stmt
	//if c, ok := column.Type.(*typedef.UDTType); ok {
	//	createType := "CREATE TYPE IF NOT EXISTS %s.%s (%s);"
	//	var typs []string
	//	for name, typ := range c.ValueTypes {
	//		typs = append(typs, name+" "+typ.CQLDef())
	//	}
	//	stmt := fmt.Sprintf(createType, keyspace, c.TypeName, strings.Join(typs, ","))
	//	stmts = append(stmts, &typedef.Stmt{
	//		StmtCache: typedef.StmtCache{
	//			Query: builders.AlterTableBuilder{
	//				Stmt: stmt,
	//			},
	//		},
	//	})
	//}
	//stmt := "ALTER TABLE " + keyspace + "." + t.Name + " ADD " + column.Name + " " + column.Type.CQLDef()
	//stmts = append(stmts, &typedef.Stmt{
	//	StmtCache: typedef.StmtCache{
	//		Query: builders.AlterTableBuilder{
	//			Stmt: stmt,
	//		},
	//	},
	//})
	//return &typedef.Stmts{
	//	Jobs:      stmts,
	//	QueryType: typedef.AddColumnStatementType,
	//	PostStmtHook: func() {
	//		t.Columns = append(t.Columns, column)
	//	},
	//}, nil
}

func genDropColumnStmt(
	t *typedef.Table,
	keyspace string,
	column typedef.ColumnDef,
) (*typedef.Stmts, error) {
	return nil, nil
	//var stmts []*typedef.Stmt
	//
	//stmt := "ALTER TABLE " + keyspace + "." + t.Name + " DROP " + column.Name
	//stmts = append(stmts, &typedef.Stmt{
	//	StmtCache: typedef.StmtCache{
	//		Query: builders.AlterTableBuilder{
	//			Stmt: stmt,
	//		},
	//		QueryType: typedef.DropColumnStatementType,
	//	},
	//})
	//return &typedef.Stmts{
	//	Jobs:      stmts,
	//	QueryType: typedef.DropColumnStatementType,
	//	PostStmtHook: func() {
	//		t.Columns = t.Columns.Remove(column)
	//	},
	//}, nil
}
