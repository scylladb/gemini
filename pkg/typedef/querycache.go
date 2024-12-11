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
	"fmt"
	"sync"

	"github.com/scylladb/gocqlx/v2/qb"
)

type QueryCache [CacheArrayLen]*StmtCache

func (c QueryCache) Reset() {
	for id := range c {
		c[id] = nil
	}
}

type Cache struct {
	schema *Schema
	table  *Table
	cache  QueryCache
	mu     sync.RWMutex
}

func New(s *Schema) *Cache {
	return &Cache{
		schema: s,
	}
}

func (c *Cache) BindToTable(t *Table) {
	c.table = t
}

func (c *Cache) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache.Reset()
}

func (c *Cache) getQuery(qct StatementCacheType) *StmtCache {
	c.mu.RLock()
	defer c.mu.RUnlock()
	rec := c.cache[qct]
	return rec
}

func (c *Cache) GetQuery(qct StatementCacheType) *StmtCache {
	rec := c.getQuery(qct)
	if rec != nil {
		return rec
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	rec = CacheBuilders[qct](c.schema, c.table)
	c.cache[qct] = rec
	return rec
}

type CacheBuilderFn func(s *Schema, t *Table) *StmtCache

type CacheBuilderFnMap map[StatementCacheType]CacheBuilderFn

func (m CacheBuilderFnMap) ToList() [CacheArrayLen]CacheBuilderFn {
	out := [CacheArrayLen]CacheBuilderFn{}
	for idx, builderFn := range m {
		out[idx] = builderFn
	}
	for idx := range out {
		if out[idx] == nil {
			panic(fmt.Sprintf("no builder for %s", StatementCacheType(idx).ToString()))
		}
	}
	return out
}

var CacheBuilders = CacheBuilderFnMap{
	CacheInsert:            genInsertStmtCache,
	CacheInsertIfNotExists: genInsertIfNotExistsStmtCache,
	CacheDelete:            genDeleteStmtCache,
	CacheUpdate:            genUpdateStmtCache,
}.ToList()

func genInsertStmtCache(
	s *Schema,
	t *Table,
) *StmtCache {
	allTypes := make([]Type, 0, t.PartitionKeys.Len()+t.ClusteringKeys.Len()+t.Columns.Len())
	builder := qb.Insert(s.Keyspace.Name + "." + t.Name)
	for _, pk := range t.PartitionKeys {
		builder = builder.Columns(pk.Name)
		allTypes = append(allTypes, pk.Type)
	}
	for _, ck := range t.ClusteringKeys {
		builder = builder.Columns(ck.Name)
		allTypes = append(allTypes, ck.Type)
	}
	for _, col := range t.Columns {
		switch colType := col.Type.(type) {
		case *TupleType:
			builder = builder.TupleColumn(col.Name, len(colType.ValueTypes))
		default:
			builder = builder.Columns(col.Name)
		}
		allTypes = append(allTypes, col.Type)
	}
	return &StmtCache{
		Query:     builder,
		Types:     allTypes,
		QueryType: InsertStatementType,
	}
}

func genInsertIfNotExistsStmtCache(
	s *Schema,
	t *Table,
) *StmtCache {
	out := genInsertStmtCache(s, t)
	out.Query = out.Query.(*qb.InsertBuilder).Unique()
	return out
}

func genUpdateStmtCache(s *Schema, t *Table) *StmtCache {
	var allTypes []Type
	builder := qb.Update(s.Keyspace.Name + "." + t.Name)

	for _, cdef := range t.Columns {
		switch t := cdef.Type.(type) {
		case *TupleType:
			builder = builder.SetTuple(cdef.Name, len(t.ValueTypes))
		case *CounterType:
			builder = builder.SetLit(cdef.Name, cdef.Name+"+1")
			continue
		default:
			builder = builder.Set(cdef.Name)
		}
		allTypes = append(allTypes, cdef.Type)
	}

	for _, pk := range t.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		allTypes = append(allTypes, pk.Type)
	}
	for _, ck := range t.ClusteringKeys {
		builder = builder.Where(qb.Eq(ck.Name))
		allTypes = append(allTypes, ck.Type)
	}
	return &StmtCache{
		Query:     builder,
		Types:     allTypes,
		QueryType: UpdateStatementType,
	}
}

func genDeleteStmtCache(s *Schema, t *Table) *StmtCache {
	var allTypes []Type
	builder := qb.Delete(s.Keyspace.Name + "." + t.Name)
	for _, pk := range t.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
		allTypes = append(allTypes, pk.Type)
	}

	if len(t.ClusteringKeys) > 0 {
		ck := t.ClusteringKeys[0]
		builder = builder.Where(qb.GtOrEq(ck.Name)).Where(qb.LtOrEq(ck.Name))
		allTypes = append(allTypes, ck.Type, ck.Type)
	}

	return &StmtCache{
		Query:     builder,
		Types:     allTypes,
		QueryType: DeleteStatementType,
	}
}
