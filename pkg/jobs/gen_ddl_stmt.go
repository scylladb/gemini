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

package jobs

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/builders"
	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/typedef"
)

func GenDDLStmt(s *typedef.Schema, t *typedef.Table, r *rand.Rand, _ *typedef.PartitionRangeConfig, sc *typedef.SchemaConfig) (*typedef.Stmts, error) {
	maxVariant := 1
	validCols := t.ValidColumnsForDelete()
	if len(t.Columns) > 0 && len(validCols) > 0 {
		maxVariant = 2
	}
	switch n := r.Intn(maxVariant + 2); n {
	// case 0: // Alter column not supported in Cassandra from 3.0.11
	//	return t.alterColumn(s.Keyspace.Name)
	case 2:
		num := r.Intn(len(validCols))
		return genDropColumnStmt(t, s.Keyspace.Name, validCols[num])
	default:
		column := typedef.ColumnDef{Name: generators.GenColumnName("col", len(t.Columns)+1), Type: generators.GenColumnType(len(t.Columns)+1, sc)}
		return genAddColumnStmt(t, s.Keyspace.Name, &column)
	}
}

func appendValue(columnType typedef.Type, r *rand.Rand, p *typedef.PartitionRangeConfig, values []interface{}) []interface{} {
	return append(values, columnType.GenValue(r, p)...)
}

func genAddColumnStmt(t *typedef.Table, keyspace string, column *typedef.ColumnDef) (*typedef.Stmts, error) {
	var stmts []*typedef.Stmt
	if c, ok := column.Type.(*typedef.UDTType); ok {
		createType := "CREATE TYPE IF NOT EXISTS %s.%s (%s);"
		var typs []string
		for name, typ := range c.Types {
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
		List: stmts,
		PostStmtHook: func() {
			t.Columns = append(t.Columns, column)
			t.ResetQueryCache()
		},
	}, nil
}

//nolint:unused
func alterColumn(t *typedef.Table, keyspace string) ([]*typedef.Stmt, func(), error) {
	var stmts []*typedef.Stmt
	idx := rand.Intn(len(t.Columns))
	column := t.Columns[idx]
	oldType, isSimpleType := column.Type.(typedef.SimpleType)
	if !isSimpleType {
		return nil, func() {}, errors.Errorf("complex type=%s cannot be altered", column.Name)
	}
	compatTypes := typedef.CompatibleColumnTypes[oldType]
	if len(compatTypes) == 0 {
		return nil, func() {}, errors.Errorf("simple type=%s has no compatible coltypes so it cannot be altered", column.Name)
	}
	newType := compatTypes.Random()
	newColumn := typedef.ColumnDef{Name: column.Name, Type: newType}
	stmt := "ALTER TABLE " + keyspace + "." + t.Name + " ALTER " + column.Name + " TYPE " + column.Type.CQLDef()
	stmts = append(stmts, &typedef.Stmt{
		StmtCache: &typedef.StmtCache{
			Query: &builders.AlterTableBuilder{
				Stmt: stmt,
			},
			QueryType: typedef.AlterColumnStatementType,
		},
	})
	return stmts, func() {
		t.Columns[idx] = &newColumn
		t.ResetQueryCache()
	}, nil
}

func genDropColumnStmt(t *typedef.Table, keyspace string, colNum int) (*typedef.Stmts, error) {
	var stmts []*typedef.Stmt

	column := t.Columns[colNum]
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
		List: stmts,
		PostStmtHook: func() {
			t.Columns = t.Columns.Remove(colNum)
			t.ResetQueryCache()
		},
	}, nil
}
