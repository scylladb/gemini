// Copyright 2025 ScyllaDB
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

package scylla

import (
	"testing"

	"github.com/samber/mo"

	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/typedef"
)

func TestBuildCreateTableQuery(t *testing.T) {
	t.Parallel()

	tests := []struct {
		replication   replication.Replication
		name          string
		keyspace      string
		table         string
		wantKeyspace  string
		wantTable     string
		partitionKeys typedef.Columns
	}{
		{
			name:     "single partition key",
			keyspace: "test_logs",
			table:    "test_statements",
			partitionKeys: typedef.Columns{
				{
					Name: "pk0",
					Type: typedef.TypeText,
				},
			},
			replication:  replication.NewSimpleStrategy(),
			wantKeyspace: "CREATE KEYSPACE IF NOT EXISTS test_logs WITH replication={'class':'SimpleStrategy','replication_factor':1} AND durable_writes = true;",
			wantTable: "CREATE TABLE IF NOT EXISTS test_logs.test_statements(pk0 text," +
				"ts timestamp, ty text, statement text, values frozen<list<text>>, host text, attempt smallint, " +
				"gemini_attempt smallint, error text, dur duration, PRIMARY KEY ((pk0, ty), ts, attempt, gemini_attempt)) " +
				"WITH caching={'enabled':'true'} AND compression={'sstable_compression':'ZstdCompressor'} " +
				"AND tombstone_gc={'mode':'immediate'} AND comment='Table to store logs from Oracle and Test statements';",
		},
		{
			name:     "multiple partition keys",
			keyspace: "test_logs",
			table:    "test_statements",
			partitionKeys: typedef.Columns{
				{
					Name: "pk0",
					Type: typedef.TypeText,
				},
				{
					Name: "pk1",
					Type: typedef.TypeInt,
				},
			},
			replication:  replication.NewSimpleStrategy(),
			wantKeyspace: "CREATE KEYSPACE IF NOT EXISTS test_logs WITH replication={'class':'SimpleStrategy','replication_factor':1} AND durable_writes = true;",
			wantTable: "CREATE TABLE IF NOT EXISTS test_logs.test_statements(pk0 text,pk1 int," +
				"ts timestamp, ty text, statement text, values frozen<list<text>>, host text, attempt smallint, " +
				"gemini_attempt smallint, error text, dur duration, PRIMARY KEY ((pk0,pk1, ty), ts, attempt, gemini_attempt)) " +
				"WITH caching={'enabled':'true'} AND compression={'sstable_compression':'ZstdCompressor'} " +
				"AND tombstone_gc={'mode':'immediate'} AND comment='Table to store logs from Oracle and Test statements';",
		},
		{
			name:     "uuid partition key",
			keyspace: "ks_logs",
			table:    "tbl_statements",
			partitionKeys: typedef.Columns{
				{
					Name: "id",
					Type: typedef.TypeUuid,
				},
			},
			replication:  replication.NewSimpleStrategy(),
			wantKeyspace: "CREATE KEYSPACE IF NOT EXISTS ks_logs WITH replication={'class':'SimpleStrategy','replication_factor':1} AND durable_writes = true;",
			wantTable: "CREATE TABLE IF NOT EXISTS ks_logs.tbl_statements(id uuid," +
				"ts timestamp, ty text, statement text, values frozen<list<text>>, host text, attempt smallint, " +
				"gemini_attempt smallint, error text, dur duration, PRIMARY KEY ((id, ty), ts, attempt, gemini_attempt)) " +
				"WITH caching={'enabled':'true'} AND compression={'sstable_compression':'ZstdCompressor'} " +
				"AND tombstone_gc={'mode':'immediate'} AND comment='Table to store logs from Oracle and Test statements';",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gotKeyspace, gotTable := buildCreateTableQuery(tt.keyspace, tt.table, tt.partitionKeys, tt.replication)

			if gotKeyspace != tt.wantKeyspace {
				t.Errorf("buildCreateTableQuery() keyspace mismatch\ngot:  %q\nwant: %q", gotKeyspace, tt.wantKeyspace)
			}

			if gotTable != tt.wantTable {
				t.Errorf("buildCreateTableQuery() table mismatch\ngot:  %q\nwant: %q", gotTable, tt.wantTable)
			}
		})
	}
}

func TestPrepareValuesOptimized(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		values mo.Either[[]any, []byte]
		want   []string
	}{
		{
			name:   "nil left values",
			values: mo.Left[[]any, []byte](nil),
			want:   nil,
		},
		{
			name:   "empty slice",
			values: mo.Left[[]any, []byte]([]any{}),
			want:   []string{},
		},
		{
			name:   "single string value",
			values: mo.Left[[]any, []byte]([]any{"test"}),
			want:   []string{`"test"`},
		},
		{
			name:   "multiple values",
			values: mo.Left[[]any, []byte]([]any{"test", 123, true}),
			want:   []string{`"test"`, `123`, `true`},
		},
		{
			name:   "right byte value",
			values: mo.Right[[]any, []byte]([]byte("test data")),
			want:   []string{"test data"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := prepareValuesOptimized(tt.values)

			if len(got) != len(tt.want) {
				t.Errorf("prepareValuesOptimized() length mismatch, got %d, want %d", len(got), len(tt.want))
				return
			}

			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("prepareValuesOptimized()[%d] = %q, want %q", i, got[i], tt.want[i])
				}
			}
		})
	}
}

func TestAdditionalColumns(t *testing.T) {
	t.Parallel()

	expected := []string{"ts", "ty", "statement", "values", "host", "attempt", "gemini_attempt", "error", "dur"}

	if len(additionalColumnsArr) != len(expected) {
		t.Errorf("additionalColumnsArr length = %d, want %d", len(additionalColumnsArr), len(expected))
	}

	for i, col := range additionalColumnsArr {
		if col != expected[i] {
			t.Errorf("additionalColumnsArr[%d] = %q, want %q", i, col, expected[i])
		}
	}
}
