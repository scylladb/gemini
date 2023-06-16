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
	"encoding/json"
	"fmt"

	"golang.org/x/exp/rand"

	"github.com/scylladb/gocqlx/v2/qb"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/typedef"
)

func GenMutateStmt(s *typedef.Schema, t *typedef.Table, g generators.GeneratorInterface, r *rand.Rand, p *typedef.PartitionRangeConfig, deletes bool) (*typedef.Stmt, error) {
	t.RLock()
	defer t.RUnlock()

	valuesWithToken := g.Get()
	if valuesWithToken == nil {
		return nil, nil
	}
	useLWT := false
	if p.UseLWT && r.Uint32()%10 == 0 {
		useLWT = true
	}

	if !deletes {
		return genInsertOrUpdateStmt(s, t, valuesWithToken, r, p, useLWT)
	}
	switch n := rand.Intn(1000); n {
	case 10, 100:
		return genDeleteRows(s, t, valuesWithToken, r, p)
	default:
		switch rand.Intn(2) {
		case 0:
			if t.KnownIssues[typedef.KnownIssuesJSONWithTuples] {
				return genInsertOrUpdateStmt(s, t, valuesWithToken, r, p, useLWT)
			}
			return genInsertJSONStmt(s, t, valuesWithToken, r, p)
		default:
			return genInsertOrUpdateStmt(s, t, valuesWithToken, r, p, useLWT)
		}
	}
}

func genInsertOrUpdateStmt(
	s *typedef.Schema,
	t *typedef.Table,
	valuesWithToken *typedef.ValueWithToken,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	useLWT bool,
) (*typedef.Stmt, error) {
	if t.IsCounterTable() {
		return genUpdateStmt(s, t, valuesWithToken, r, p)
	}
	return genInsertStmt(s, t, valuesWithToken, r, p, useLWT)
}

func genUpdateStmt(_ *typedef.Schema, t *typedef.Table, valuesWithToken *typedef.ValueWithToken, r *rand.Rand, p *typedef.PartitionRangeConfig) (*typedef.Stmt, error) {
	stmtCache := t.GetQueryCache(typedef.CacheUpdate)
	nonCounters := t.Columns.NonCounters()
	values := make(typedef.Values, 0, t.PartitionKeys.LenValues()+t.ClusteringKeys.LenValues()+nonCounters.LenValues())
	for _, cdef := range nonCounters {
		values = appendValue(cdef.Type, r, p, values)
	}
	values = values.CopyFrom(valuesWithToken.Value)
	for _, ck := range t.ClusteringKeys {
		values = appendValue(ck.Type, r, p, values)
	}
	return &typedef.Stmt{
		StmtCache:       stmtCache,
		ValuesWithToken: valuesWithToken,
		Values:          values,
	}, nil
}

func genInsertStmt(
	_ *typedef.Schema,
	t *typedef.Table,
	valuesWithToken *typedef.ValueWithToken,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	useLWT bool,
) (*typedef.Stmt, error) {
	values := make(typedef.Values, 0, t.PartitionKeys.LenValues()+t.ClusteringKeys.LenValues()+t.Columns.LenValues())
	values = values.CopyFrom(valuesWithToken.Value)
	for _, ck := range t.ClusteringKeys {
		values = append(values, ck.Type.GenValue(r, p)...)
	}
	for _, col := range t.Columns {
		values = append(values, col.Type.GenValue(r, p)...)
	}
	cacheType := typedef.CacheInsert
	if useLWT {
		cacheType = typedef.CacheInsertIfNotExists
	}
	stmtCache := t.GetQueryCache(cacheType)
	return &typedef.Stmt{
		StmtCache:       stmtCache,
		ValuesWithToken: valuesWithToken,
		Values:          values,
	}, nil
}

func genInsertJSONStmt(
	s *typedef.Schema,
	table *typedef.Table,
	valuesWithToken *typedef.ValueWithToken,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
) (*typedef.Stmt, error) {
	var v string
	var ok bool
	if table.IsCounterTable() {
		return nil, nil
	}
	vs := valuesWithToken.Value.Copy()
	values := make(map[string]interface{})
	for i, pk := range table.PartitionKeys {
		switch t := pk.Type.(type) {
		case typedef.SimpleType:
			if t != typedef.TYPE_BLOB {
				values[pk.Name] = vs[i]
				continue
			}
			v, ok = vs[i].(string)
			if ok {
				values[pk.Name] = "0x" + v
			}
		case *typedef.TupleType:
			tupVals := make([]interface{}, len(t.ValueTypes))
			for j := 0; j < len(t.ValueTypes); j++ {
				if t.ValueTypes[j] == typedef.TYPE_BLOB {
					v, ok = vs[i+j].(string)
					if ok {
						v = "0x" + v
					}
					vs[i+j] = v
				}
				tupVals[i] = vs[i+j]
				i++
			}
			values[pk.Name] = tupVals
		default:
			panic(fmt.Sprintf("unknown type: %s", t.Name()))
		}
	}
	values = table.ClusteringKeys.ToJSONMap(values, r, p)
	values = table.Columns.ToJSONMap(values, r, p)

	jsonString, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}

	builder := qb.Insert(s.Keyspace.Name + "." + table.Name).Json()
	return &typedef.Stmt{
		StmtCache: &typedef.StmtCache{
			Query:     builder,
			Types:     []typedef.Type{typedef.TYPE_TEXT},
			QueryType: typedef.InsertJSONStatementType,
		},
		ValuesWithToken: valuesWithToken,
		Values:          []interface{}{string(jsonString)},
	}, nil
}

func genDeleteRows(_ *typedef.Schema, t *typedef.Table, valuesWithToken *typedef.ValueWithToken, r *rand.Rand, p *typedef.PartitionRangeConfig) (*typedef.Stmt, error) {
	stmtCache := t.GetQueryCache(typedef.CacheDelete)
	values := valuesWithToken.Value.Copy()
	if len(t.ClusteringKeys) > 0 {
		ck := t.ClusteringKeys[0]
		values = appendValue(ck.Type, r, p, values)
		values = appendValue(ck.Type, r, p, values)
	}
	return &typedef.Stmt{
		StmtCache:       stmtCache,
		ValuesWithToken: valuesWithToken,
		Values:          values,
	}, nil
}
