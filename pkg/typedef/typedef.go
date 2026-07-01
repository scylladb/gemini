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
	"slices"
	"sort"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/google/uuid"
	"github.com/scylladb/gocqlx/v3/qb"

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
		DeleteBuckets      []time.Duration
		MaxDeletedHeapSize int
		RowTrackerCapacity int
		MaxBlobLength      int
		MinBlobLength      int
		MaxStringLength    int
		MinStringLength    int
		UseLWT             bool
	}

	ValueRangeConfig struct {
		MaxBlobLength   int
		MinBlobLength   int
		MaxStringLength int
		MinStringLength int
	}

	RangeConfig interface {
		GetMaxBlobLength() int
		GetMinBlobLength() int
		GetMaxStringLength() int
		GetMinStringLength() int
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
		Values      *Values
		Release     func() `json:"-"`
		ID          uuid.UUID
		DeletedAtNS uint64
	}

	Stmt struct {
		PartitionKeys []PartitionKeys
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
	case DeleteSingleRowType:
		return "DeleteSingleRow"
	case DeleteClusteringSubsetType:
		return "DeleteClusteringSubset"
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
		DeleteClusteringSubsetType, DeleteSingleRowType:
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
		DeleteClusteringSubsetType, DeleteSingleRowType:
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

// Values holds the bind values for a set of named columns. It uses parallel
// name/value slices rather than a map: the column sets are tiny (a handful of
// partition keys), so a linear scan beats map hashing while avoiding the map
// allocation that dominated the partition hot path. names[i] owns vals[i].
//
// Values is NOT safe for concurrent mutation. By construction it never needs to
// be: a Values is built (NewPartitionValues / NewValuesFromMap / Merge into a
// goroutine-local target) and thereafter only read. The one Values that is
// shared across goroutines — the old partition keys handed from Replace to the
// deleted-partitions heap and later to a validation worker — is read-only on
// every holder, and concurrent reads of immutable data need no lock. Do not
// introduce a path that mutates (Merge/UnmarshalJSON) a Values after it has
// escaped to another goroutine.
type Values struct {
	names []string
	vals  [][]any
}

// index returns the position of name, or -1.
func (v *Values) index(name string) int {
	for i, n := range v.names {
		if n == name {
			return i
		}
	}
	return -1
}

type ValidationData struct {
	FirstSuccessNS atomic.Uint64
	LastSuccessNS  atomic.Uint64
	LastFailureNS  atomic.Uint64
	Recent         [5]atomic.Uint64
	RecentIdx      atomic.Uint64
	SuccessCount   atomic.Uint64
}

func NewValues(initial int) *Values {
	return &Values{
		names: make([]string, 0, initial),
		vals:  make([][]any, 0, initial),
	}
}

func NewValuesFromMap(m map[string][]any) *Values {
	v := &Values{
		names: make([]string, 0, len(m)),
		vals:  make([][]any, 0, len(m)),
	}
	for k, val := range m {
		v.names = append(v.names, k)
		v.vals = append(v.vals, val)
	}
	return v
}

// NewPartitionValues builds a Values directly from a contiguous flat value slice
// and a column layout, skipping the intermediate map the partition hot path used
// to allocate per Get. Columns are laid out consecutively in src in the same
// order GenValueOut emits them, so each column's values are the next
// col.Type.LenValue() entries — a capped sub-slice of src (no copy). src must
// remain immutable for the lifetime of the returned Values; the cap forces any
// later append (e.g. via Merge) to reallocate rather than clobber the shared
// backing array.
func NewPartitionValues(cols Columns, src []any) *Values {
	names := make([]string, len(cols))
	vals := make([][]any, len(cols))
	off := 0
	for i, col := range cols {
		end := off + col.Type.LenValue()
		names[i] = col.Name
		vals[i] = src[off:end:end]
		off = end
	}
	return &Values{names: names, vals: vals}
}

func (v *Values) Get(name string) []any {
	if i := v.index(name); i >= 0 {
		return v.vals[i]
	}

	return nil
}

func (v *Values) Keys() []string {
	keys := make([]string, len(v.names))
	copy(keys, v.names)

	sort.Strings(keys)
	return keys
}

func (v *Values) Len() int {
	return len(v.names)
}

func (v *Values) ToCQLValues(pks Columns) []any {
	if v == nil {
		return []any{}
	}

	n := 0
	if len(pks) > 0 {
		if i := v.index(pks[0].Name); i >= 0 {
			n = len(v.names) * len(v.vals[i])
		}
	}

	values := make([]any, 0, n)
	for _, pk := range pks {
		if i := v.index(pk.Name); i >= 0 {
			values = append(values, v.vals[i]...)
		}
	}

	return values
}

// AppendCQLValues appends this Values' CQL bind values for the given partition
// keys to dst (in pks order) and returns the extended slice. It is the
// allocation-free counterpart to ToCQLValues: callers that already hold a
// reusable/pooled buffer (e.g. the statement-logger committer) avoid the
// throwaway intermediate slice ToCQLValues would otherwise allocate per call.
func (v *Values) AppendCQLValues(dst []any, pks Columns) []any {
	if v == nil {
		return dst
	}

	for _, pk := range pks {
		if i := v.index(pk.Name); i >= 0 {
			dst = append(dst, v.vals[i]...)
		}
	}

	return dst
}

// Merge appends every entry from values into v. v must be owned by the calling
// goroutine (see the Values doc comment); neither v nor values may be mutated
// concurrently.
func (v *Values) Merge(values *Values) {
	for i, name := range values.names {
		if j := v.index(name); j >= 0 {
			v.vals[j] = append(v.vals[j], values.vals[i]...)
		} else {
			v.names = append(v.names, name)
			// Copy rather than alias the source slice, matching the previous
			// map-based behaviour where a new key did append(nil, value...).
			v.vals = append(v.vals, append([]any(nil), values.vals[i]...))
		}
	}
}

func (v *Values) ToMap() map[string][]any {
	m := make(map[string][]any, len(v.names))
	for i, name := range v.names {
		m[name] = v.vals[i]
	}
	return m
}

func (v *Values) MemoryFootprint() uint64 {
	return 0
}

func (v *Values) MarshalJSON() ([]byte, error) {
	if v == nil {
		return []byte("null"), nil
	}

	m := make(map[string][]any, len(v.names))
	for i, name := range v.names {
		m[name] = v.vals[i]
	}

	return json.Marshal(m)
}

func (v *Values) UnmarshalJSON(data []byte) error {
	if v == nil {
		return nil
	}

	if len(data) == 0 || bytes.Equal(data, []byte("null")) {
		return nil
	}

	var m map[string][]any
	if err := json.Unmarshal(data, &m); err != nil {
		return fmt.Errorf("unmarshal values: %w", err)
	}

	for k, value := range m {
		if i := v.index(k); i >= 0 {
			v.vals[i] = value
		} else {
			v.names = append(v.names, k)
			v.vals = append(v.vals, value)
		}
	}

	return nil
}

func (v *PartitionKeys) Copy() PartitionKeys {
	return PartitionKeys{
		Values:      v.Values.Copy(),
		ID:          v.ID,
		DeletedAtNS: v.DeletedAtNS,
	}
}

func (v *Values) Copy() *Values {
	if v == nil {
		return nil
	}

	// Shallow clone: the name/value-header slices are copied, the inner []any
	// backing arrays are shared (matching the previous maps.Clone behaviour).
	return &Values{
		names: slices.Clone(v.names),
		vals:  slices.Clone(v.vals),
	}
}

func (v *Values) Data() []any {
	// Emit values in column-name order for deterministic output.
	order := make([]int, len(v.names))
	for i := range order {
		order[i] = i
	}
	sort.Slice(order, func(a, b int) bool { return v.names[order[a]] < v.names[order[b]] })

	values := make([]any, 0, len(v.names))
	for _, i := range order {
		values = append(values, v.vals[i]...)
	}

	return values
}

type StatementCacheType uint8

func (v ValueWithToken) MemoryFootprint() uint64 {
	return uint64(unsafe.Sizeof(v)) + v.Value.MemoryFootprint()
}

func (p PartitionRangeConfig) GetMaxBlobLength() int {
	return p.MaxBlobLength
}

func (p PartitionRangeConfig) GetMinBlobLength() int {
	return p.MinBlobLength
}

func (p PartitionRangeConfig) GetMaxStringLength() int {
	return p.MaxStringLength
}

func (p PartitionRangeConfig) GetMinStringLength() int {
	return p.MinStringLength
}

func (p ValueRangeConfig) GetMaxBlobLength() int {
	return p.MaxBlobLength
}

func (p ValueRangeConfig) GetMinBlobLength() int {
	return p.MinBlobLength
}

func (p ValueRangeConfig) GetMaxStringLength() int {
	return p.MaxStringLength
}

func (p ValueRangeConfig) GetMinStringLength() int {
	return p.MinStringLength
}
