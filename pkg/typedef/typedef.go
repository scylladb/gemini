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
	"encoding/json"
	"fmt"
	"unsafe"

	"github.com/scylladb/gocqlx/v3/qb"
	"golang.org/x/exp/maps"

	"github.com/scylladb/gemini/pkg/replication"
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

type (
	PartitionKeys struct {
		Values Values
		Token  uint64
	}

	Stmt struct {
		PartitionKeys PartitionKeys
		Query         string
		Values        []any
		QueryType     StatementType
	}
)

func SimpleStmt(query string, queryType StatementType) *Stmt {
	return &Stmt{
		Query:     query,
		QueryType: queryType,
	}
}

func (s *Stmt) ToCql() (string, []string) {
	return s.Query, nil
}

type StatementType uint8

//nolint:gocyclo
func (st StatementType) String() string {
	switch st {
	case SelectStatementType:
		return "SelectStatement"
	case SelectRangeStatementType:
		return "SelectRangeStatement"
	case SelectMultiPartitionType:
		return "SelectMultiPartitionType"
	case SelectMultiPartitionRangeStatementType:
		return "SelectMultiPartitionRangeStatementType"
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

func (st StatementType) MarshalJSON() ([]byte, error) {
	return json.Marshal(st.String())
}

func (o OpType) String() string {
	switch o {
	case OpSelect:
		return "Select"
	case OpInsert:
		return "Insert"
	case OpUpdate:
		return "Update"
	case OpDelete:
		return "Delete"
	case OpSchemaAlter:
		return "SchemaAlter"
	case OpSchemaDrop:
		return "SchemaDrop"
	case OpSchemaCreate:
		return "SchemaCreate"
	default:
		panic(fmt.Sprintf("unknown operation type %d", o))
	}
}

func (st StatementType) IsSelect() bool {
	switch st {
	case SelectStatementType,
		SelectRangeStatementType,
		SelectMultiPartitionType,
		SelectMultiPartitionRangeStatementType,
		SelectByIndexStatementType,
		SelectFromMaterializedViewStatementType:
		return true
	default:
		return false
	}
}

func (st StatementType) IsInsert() bool {
	return st == InsertStatementType || st == InsertJSONStatementType
}

func (st StatementType) IsUpdate() bool {
	return st == UpdateStatementType
}

func (st StatementType) IsDelete() bool {
	return st == DeleteStatementType
}

func (st StatementType) IsSchema() bool {
	switch st {
	case AlterColumnStatementType, DropColumnStatementType, AddColumnStatementType,
		DropTableStatementType, CreateTableStatementType, DropTypeStatementType,
		CreateTypeStatementType, DropIndexStatementType, CreateIndexStatementType,
		CreateKeyspaceStatementType, DropKeyspaceStatementType, CreateSchemaStatementType:
		return true
	default:
		return false
	}
}

func (st StatementType) OpType() OpType {
	switch st {
	case SelectStatementType,
		SelectRangeStatementType,
		SelectMultiPartitionType,
		SelectMultiPartitionRangeStatementType,
		SelectByIndexStatementType,
		SelectFromMaterializedViewStatementType:
		return OpSelect
	case InsertStatementType, InsertJSONStatementType:
		return OpInsert
	case UpdateStatementType:
		return OpUpdate
	case DeleteStatementType:
		return OpDelete
	case AlterColumnStatementType, DropColumnStatementType, AddColumnStatementType,
		DropTableStatementType, CreateTableStatementType, DropTypeStatementType,
		CreateTypeStatementType, DropIndexStatementType, CreateIndexStatementType:
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

type Values map[string][]any

func (v Values) ToCQLValues(pks Columns) []any {
	if v == nil {
		return nil
	}

	values := make([]any, 0, len(v))

	for _, pk := range pks {
		values = append(values, v[pk.Name]...)
	}

	return values
}

func (v Values) Merge(values Values) {
	for k, value := range values {
		v[k] = append(v[k], value...)
	}
}

func (v Values) MemoryFootprint() uint64 {
	if v == nil {
		return 0
	}

	return 0
	// return utils.Sizeof([]any(v))
}

func (v Values) Copy() Values {
	return maps.Clone(v)
}

type StatementCacheType uint8

func (v ValueWithToken) MemoryFootprint() uint64 {
	return uint64(unsafe.Sizeof(v)) + v.Value.MemoryFootprint()
}
