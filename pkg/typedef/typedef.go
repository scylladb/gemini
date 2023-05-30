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
	"github.com/scylladb/gocqlx/v2/qb"

	"github.com/scylladb/gemini/pkg/replication"
)

type (
	ValueWithToken struct {
		Value Values
		Token uint64
	}
	Keyspace struct {
		Replication       *replication.Replication `json:"replication"`
		OracleReplication *replication.Replication `json:"oracle_replication"`
		Name              string                   `json:"name"`
	}

	IndexDef struct {
		Name      string `json:"name"`
		Column    string `json:"column"`
		ColumnIdx int    `json:"column_idx"`
	}

	PartitionRangeConfig struct {
		MaxBlobLength   int
		MinBlobLength   int
		MaxStringLength int
		MinStringLength int
		UseLWT          bool
	}

	CQLFeature int
)

type Stmts struct {
	PostStmtHook func()
	List         []*Stmt
}

type Stmt struct {
	ValuesWithToken *ValueWithToken
	Query           qb.Builder
	Values          Values
	Types           []Type
	QueryType       StatementType
}

func (s *Stmt) PrettyCQL() string {
	var replaced int
	query, _ := s.Query.ToCql()
	values := s.Values.Copy()
	if len(values) == 0 {
		return query
	}
	for _, typ := range s.Types {
		query, replaced = typ.CQLPretty(query, values)
		if len(values) >= replaced {
			values = values[replaced:]
		} else {
			break
		}
	}
	return query
}

type StatementType uint8

func (st StatementType) PossibleAsyncOperation() bool {
	return st == SelectByIndexStatementType || st == SelectFromMaterializedViewStatementType
}

type Values []interface{}

func (v Values) Copy() Values {
	values := make(Values, len(v))
	copy(values, v)
	return values
}
