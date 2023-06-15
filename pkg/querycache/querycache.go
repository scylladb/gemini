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

package querycache

import (
	"fmt"
	"sync"

	"github.com/scylladb/gocqlx/v2/qb"

	"github.com/scylladb/gemini/pkg/typedef"
)

type QueryCache [typedef.CacheArrayLen]*typedef.StmtCache

func (c QueryCache) Reset() {
	for id := range c {
		c[id] = nil
	}
}

type Cache struct {
	schema *typedef.Schema
	table  *typedef.Table
	cache  QueryCache
	mu     sync.RWMutex
}

func New(s *typedef.Schema) *Cache {
	return &Cache{
		schema: s,
	}
}

func (c *Cache) BindToTable(t *typedef.Table) {
	c.table = t
}

func (c *Cache) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cache.Reset()
}

func (c *Cache) getQuery(qct typedef.StatementCacheType) *typedef.StmtCache {
	c.mu.RLock()
	defer c.mu.RUnlock()
	rec := c.cache[qct]
	return rec
}

func (c *Cache) GetQuery(qct typedef.StatementCacheType) *typedef.StmtCache {
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

type CacheBuilderFn func(s *typedef.Schema, t *typedef.Table) *typedef.StmtCache

type CacheBuilderFnMap map[typedef.StatementCacheType]CacheBuilderFn

func (m CacheBuilderFnMap) ToList() [typedef.CacheArrayLen]CacheBuilderFn {
	out := [typedef.CacheArrayLen]CacheBuilderFn{}
	for idx, builderFn := range m {
		out[idx] = builderFn
	}
	for idx := range out {
		if out[idx] == nil {
			panic(fmt.Sprintf("no builder for %s", typedef.StatementCacheType(idx).ToString()))
		}
	}
	return out
}

var CacheBuilders = CacheBuilderFnMap{
	typedef.CacheInsert:            genInsertStmtCache,
	typedef.CacheInsertIfNotExists: genInsertIfNotExistsStmtCache,
	typedef.CacheDelete:            genDeleteStmtCache,
	typedef.CacheUpdate:            genUpdateStmtCache,
}.ToList()

func genInsertStmtCache(
	s *typedef.Schema,
	t *typedef.Table,
) *typedef.StmtCache {
	allTypes := make([]typedef.Type, 0, t.PartitionKeys.Len()+t.ClusteringKeys.Len()+t.Columns.Len())
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
		case *typedef.TupleType:
			builder = builder.TupleColumn(col.Name, len(colType.Types))
		default:
			builder = builder.Columns(col.Name)
		}
		allTypes = append(allTypes, col.Type)
	}
	return &typedef.StmtCache{
		Query:     builder,
		Types:     allTypes,
		QueryType: typedef.InsertStatementType,
	}
}

func genInsertIfNotExistsStmtCache(
	s *typedef.Schema,
	t *typedef.Table,
) *typedef.StmtCache {
	out := genInsertStmtCache(s, t)
	out.Query = out.Query.(*qb.InsertBuilder).Unique()
	return out
}

func genUpdateStmtCache(s *typedef.Schema, t *typedef.Table) *typedef.StmtCache {
	var allTypes []typedef.Type
	builder := qb.Update(s.Keyspace.Name + "." + t.Name)

	for _, cdef := range t.Columns {
		switch t := cdef.Type.(type) {
		case *typedef.TupleType:
			builder = builder.SetTuple(cdef.Name, len(t.Types))
		case *typedef.CounterType:
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
	return &typedef.StmtCache{
		Query:     builder,
		Types:     allTypes,
		QueryType: typedef.UpdateStatementType,
	}
}

func genDeleteStmtCache(s *typedef.Schema, t *typedef.Table) *typedef.StmtCache {
	var allTypes []typedef.Type
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
	return &typedef.StmtCache{
		Query:     builder,
		Types:     allTypes,
		QueryType: typedef.DeleteStatementType,
	}
}
