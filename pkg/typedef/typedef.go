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
	"bytes"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/scylladb/gocqlx/v3/qb"

	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/utils"
)

type (
	CQLFeature int
	OpType     uint8

	ValueWithToken struct {
		Value Values
		Token uint64
	}
	Keyspace struct {
		Replication       replication.Replication `json:"replication"`
		OracleReplication replication.Replication `json:"oracle_replication"`
		Name              string                  `json:"name"`
	}

	IndexDef struct {
		Column     *ColumnDef
		IndexName  string `json:"index_name"`
		ColumnName string `json:"column_name"`
	}

	PartitionRangeConfig struct {
		MaxBlobLength   int
		MinBlobLength   int
		MaxStringLength int
		MinStringLength int
		UseLWT          bool
	}

	Stmts struct {
		PostStmtHook func()
		List         []*Stmt
		QueryType    StatementType
	}

	StmtCache struct {
		Query     qb.Builder
		Types     Types
		QueryType StatementType
		LenValue  int
	}
)

type SimpleQuery struct {
	query string
}

func (q SimpleQuery) ToCql() (stmt string, names []string) {
	return q.query, nil
}

type Stmt struct {
	ValuesWithToken []ValueWithToken
	Values          Values
	StmtCache
}

func SimpleStmt(query string, queryType StatementType) *Stmt {
	return &Stmt{
		StmtCache: StmtCache{
			Query:     SimpleQuery{query},
			QueryType: queryType,
		},
	}
}

func (s *Stmt) PrettyCQL() (string, error) {
	if s == nil {
		return "", nil
	}

	buffer := bytes.NewBuffer(nil)

	if err := s.PrettyCQLBuffered(buffer); err != nil {
		return "", err
	}

	return buffer.String(), nil
}

func (s *Stmt) PrettyCQLBuffered(buffer *bytes.Buffer) error {
	query, _ := s.Query.ToCql()
	values := s.Values.Copy()
	return prettyCQL(buffer, query, values, s.Types)
}

func (s *Stmt) ToCql() (string, []string) {
	return s.Query.ToCql()
}

func (s *Stmt) Clone() *Stmt {
	return &Stmt{
		StmtCache: s.StmtCache,
		Values:    s.Values.Copy(),
	}
}

type StatementType uint8

//nolint:gocyclo
func ParseStatementTypeFromQuery(query string) StatementType {
	if strings.HasPrefix(query, "SELECT") {
		if strings.Contains(query, "FROM") {
			return SelectStatementType
		}
		if strings.Contains(query, "RANGE") {
			return SelectRangeStatementType
		}
		if strings.Contains(query, "BY INDEX") {
			return SelectByIndexStatementType
		}
		if strings.Contains(query, "FROM MATERIALIZED VIEW") {
			return SelectFromMaterializedViewStatementType
		}
	}

	if strings.HasPrefix(query, "INSERT") {
		if strings.Contains(query, "JSON") {
			return InsertJSONStatementType
		}
		return InsertStatementType
	}

	if strings.HasPrefix(query, "UPDATE") {
		return UpdateStatementType
	}

	if strings.HasPrefix(query, "DELETE") {
		return DeleteStatementType
	}

	if strings.HasPrefix(query, "ALTER COLUMN") {
		return AlterColumnStatementType
	}

	if strings.HasPrefix(query, "DROP COLUMN") {
		return DropColumnStatementType
	}

	if strings.HasPrefix(query, "ADD COLUMN") {
		return AddColumnStatementType
	}

	if strings.HasPrefix(query, "CREATE KEYSPACE") {
		return CreateKeyspaceStatementType
	}

	if strings.HasPrefix(query, "CREATE SCHEMA") {
		return CreateSchemaStatementType
	}

	if strings.HasPrefix(query, "CREATE TABLE") {
		return CreateTableStatementType
	}

	if strings.HasPrefix(query, "DROP TABLE") {
		return DropTableStatementType
	}

	if strings.HasPrefix(query, "DROP KEYSPACE") {
		return DropKeyspaceStatementType
	}

	if strings.HasPrefix(query, "CREATE INDEX") {
		return CreateIndexStatementType
	}

	if strings.HasPrefix(query, "DROP INDEX") {
		return DropIndexStatementType
	}

	if strings.HasPrefix(query, "CREATE TYPE") {
		return CreateTypeStatementType
	}

	if strings.HasPrefix(query, "DROP TYPE") {
		return DropTypeStatementType
	}

	panic(fmt.Sprintf("unknown statement type for query: %s", query))
}

//nolint:gocyclo
func (st StatementType) String() string {
	switch st {
	case SelectStatementType:
		return "SelectStatement"
	case SelectRangeStatementType:
		return "SelectRangeStatement"
	case SelectByIndexStatementType:
		return "SelectByIndexStatement"
	case SelectFromMaterializedViewStatementType:
		return "SelectFromMaterializedViewStatement"
	case DeleteStatementType:
		return "DeleteStatement"
	case InsertStatementType:
		return "InsertStatement"
	case InsertJSONStatementType:
		return "InsertJSONStatement"
	case UpdateStatementType:
		return "UpdateStatement"
	case AlterColumnStatementType:
		return "AlterColumnStatement"
	case DropColumnStatementType:
		return "DropColumnStatement"
	case AddColumnStatementType:
		return "AddColumnStatement"
	case DropKeyspaceStatementType:
		return "DropKeyspaceStatement"
	case CreateKeyspaceStatementType:
		return "CreateKeyspaceStatement"
	case CreateSchemaStatementType:
		return "CreateSchemaStatement"
	case CreateIndexStatementType:
		return "CreateIndexStatement"
	case DropIndexStatementType:
		return "DropIndexStatement"
	case CreateTypeStatementType:
		return "CreateTypeStatement"
	case DropTypeStatementType:
		return "DropTypeStatement"
	case CreateTableStatementType:
		return "CreateTableStatement"
	case DropTableStatementType:
		return "DropTableStatement"
	default:
		panic(fmt.Sprintf("unknown statement type %d", st))
	}
}

func (st StatementType) OpType() OpType {
	switch st {
	case SelectStatementType,
		SelectRangeStatementType,
		SelectByIndexStatementType,
		SelectFromMaterializedViewStatementType:
		return OpSelect
	case InsertStatementType, InsertJSONStatementType:
		return OpInsert
	case UpdateStatementType:
		return OpUpdate
	case DeleteStatementType:
		return OpDelete
	case AlterColumnStatementType, DropColumnStatementType, AddColumnStatementType:
		return OpSchemaAlter
	case DropKeyspaceStatementType:
		return OpSchemaDrop
	case CreateKeyspaceStatementType, CreateSchemaStatementType:
		return OpSchemaCreate
	default:
		panic(fmt.Sprintf("unknown statement type %d", st))
	}
}

func (st StatementType) PossibleAsyncOperation() bool {
	switch st {
	case SelectByIndexStatementType, SelectFromMaterializedViewStatementType:
		return true
	default:
		return false
	}
}

type Values []any

func (v Values) MemoryFootprint() uint64 {
	if v == nil {
		return 0
	}

	return utils.DataSize(v)
}

func (v Values) Copy() Values {
	values := make(Values, len(v))
	copy(values, v)
	return values
}

func (v Values) CopyFrom(src Values) Values {
	out := v[len(v) : len(v)+len(src)]
	copy(out, src)
	return v[:len(v)+len(src)]
}

type StatementCacheType uint8

func (t StatementCacheType) ToString() string {
	switch t {
	case CacheInsert:
		return "CacheInsert"
	case CacheInsertIfNotExists:
		return "CacheInsertIfNotExists"
	case CacheUpdate:
		return "CacheUpdate"
	case CacheDelete:
		return "CacheDelete"
	default:
		panic(fmt.Sprintf("unknown statement cache type %d", t))
	}
}

const (
	CacheInsert StatementCacheType = iota
	CacheInsertIfNotExists
	CacheUpdate
	CacheDelete
	CacheArrayLen
)

func prettyCQL(builder *bytes.Buffer, q string, values Values, types []Type) error {
	builder.Grow(len(q))

	if len(types) == 0 {
		builder.WriteString(q)
		return nil
	}

	if len(values) < len(types) {
		return errors.Errorf("expected at least %d values, got %d", len(types), len(values))
	}

	var (
		skip int
		idx  int
	)

	for pos, i := strings.Index(q[idx:], "?"), 0; pos != -1; pos = strings.Index(q[idx:], "?") {
		str := q[idx : idx+pos]
		// Just to skip the ? in the query, happens only in TUPLE TYPES
		if skip > 0 {
			skip--
			idx += pos + 1
			continue
		}

		builder.WriteString(str)

		var value any

		if i >= len(types) {
			return errors.Errorf(
				"there are more(%d) ? in the query than types(%d), invalid Query: %s",
				len(types),
				i,
				q,
			)
		}

		switch tt := types[i].(type) {
		case *TupleType:
			skip = tt.LenValue()
			value = values[i : i+skip]
			values = values[skip:]
		default:
			value = values[i]
		}

		if err := types[i].CQLPretty(builder, value); err != nil {
			return err
		}

		i++
		idx += pos + 1
	}

	builder.WriteString(q[idx:])
	return nil
}

func (v ValueWithToken) MemoryFootprint() uint64 {
	return utils.Sizeof(v) + v.Value.MemoryFootprint()
}
