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
	"encoding/json"
	"fmt"
	"sync"
	"unsafe"

	"github.com/scylladb/gocqlx/v3/qb"
	"golang.org/x/exp/maps"

	"github.com/scylladb/gemini/pkg/replication"
)

type (
	CQLFeature int
	OpType     uint8

	ValueWithToken struct {
		Value *Values
		Token uint64
	}
	Keyspace struct {
		Replication       replication.Replication `json:"replication"`
		OracleReplication replication.Replication `json:"oracle_replication"`
		Name              string                  `json:"name"`
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
		Values *Values
		Token  uint32
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

func PreparedStmt(query string, pks map[string][]any, values []any, queryType StatementType) *Stmt {
	return &Stmt{
		Query:         query,
		Values:        values,
		QueryType:     queryType,
		PartitionKeys: PartitionKeys{Values: NewValuesFromMap(pks), Token: 0},
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
	case DeleteSingleRowType:
		return "DeleteSingleRow"
	case DeleteSingleColumnType:
		return "DeleteSingleColumn"
	case DeleteMultiplePartitionsType:
		return "DeleteMultiplePartitions"
	case DeleteWholePartitionType:
		return "DeleteWholePartition"
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

func (op OpType) String() string {
	switch op {
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
		panic(fmt.Sprintf("unknown operation type %d", op))
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
	switch st {
	case DeleteWholePartitionType, DeleteMultiplePartitionsType,
		DeleteSingleColumnType, DeleteSingleRowType:
		return true
	default:
		return false
	}
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
	case DeleteWholePartitionType, DeleteMultiplePartitionsType,
		DeleteSingleColumnType, DeleteSingleRowType:
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

type Values struct {
	data map[string][]any
	mu   sync.RWMutex
}

func NewValues(initial int) *Values {
	return &Values{
		data: make(map[string][]any, initial),
	}
}

func NewValuesFromMap(m map[string][]any) *Values {
	return &Values{
		data: m,
	}
}

func (v *Values) Get(name string) []any {
	v.mu.RLock()
	defer v.mu.RUnlock()
	if values, ok := v.data[name]; ok {
		return values
	}

	return nil
}

func (v *Values) Len() int {
	return len(v.data)
}

func (v *Values) ToCQLValues(pks Columns) []any {
	if v == nil {
		return []any{}
	}

	v.mu.RLock()
	values := make([]any, 0, len(v.data)*len(v.data[pks[0].Name]))

	for _, pk := range pks {
		values = append(values, v.data[pk.Name]...)
	}
	v.mu.RUnlock()

	return values
}

func (v *Values) Merge(values *Values) {
	values.mu.RLock()
	v.mu.Lock()
	defer v.mu.Unlock()
	defer values.mu.RUnlock()

	for k, value := range values.data {
		v.data[k] = append(v.data[k], value...)
	}
}

func (v *Values) ToMap() map[string][]any {
	n := v.Copy()
	return n.data
}

func (v *Values) MemoryFootprint() uint64 {
	return 0
}

func (v *Values) MarshalJSON() ([]byte, error) {
	if v == nil {
		return []byte("null"), nil
	}

	v.mu.RLock()
	defer v.mu.RUnlock()

	return json.Marshal(v.data)
}

func (v *Values) UnmarshalJSON(data []byte) error {
	if v == nil {
		return nil
	}

	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return nil
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	if v.data == nil {
		v.data = make(map[string][]any)
	}

	var m map[string][]any
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("unmarshal values: %w", err)
	}

	for k, value := range m {
		v.data[k] = value
	}

	return nil
}

func (v *PartitionKeys) Copy() PartitionKeys {
	return PartitionKeys{
		Token:  v.Token,
		Values: v.Values.Copy(),
	}
}

func (v *Values) Copy() *Values {
	if v == nil {
		return nil
	}

	v.mu.RLock()
	m := maps.Clone(v.data)
	v.mu.RUnlock()

	return &Values{data: m}
}

type StatementCacheType uint8

func (v ValueWithToken) MemoryFootprint() uint64 {
	return uint64(unsafe.Sizeof(v)) + v.Value.MemoryFootprint()
}
