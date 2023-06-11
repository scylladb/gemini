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

package builders

import (
	"github.com/scylladb/gemini/pkg/querycache"
	"github.com/scylladb/gemini/pkg/typedef"
)

type SchemaBuilder interface {
	Keyspace(typedef.Keyspace) SchemaBuilder
	Table(*typedef.Table) SchemaBuilder
	Build() *typedef.Schema
}

type AlterTableBuilder struct {
	Stmt string
}

func (atb *AlterTableBuilder) ToCql() (string, []string) {
	return atb.Stmt, nil
}

type schemaBuilder struct {
	keyspace typedef.Keyspace
	tables   []*typedef.Table
}

func (s *schemaBuilder) Keyspace(keyspace typedef.Keyspace) SchemaBuilder {
	s.keyspace = keyspace
	return s
}

func (s *schemaBuilder) Table(table *typedef.Table) SchemaBuilder {
	s.tables = append(s.tables, table)
	return s
}

func (s *schemaBuilder) Build() *typedef.Schema {
	out := &typedef.Schema{Keyspace: s.keyspace, Tables: s.tables}
	for id := range s.tables {
		s.tables[id].Init(out, querycache.New(out))
	}
	return out
}

func NewSchemaBuilder() SchemaBuilder {
	return &schemaBuilder{}
}
