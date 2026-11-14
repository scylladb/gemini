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
	"slices"
	"testing"

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
			replication:  replication.NewNetworkTopologyStrategy(),
			wantKeyspace: "CREATE KEYSPACE IF NOT EXISTS test_logs WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1} AND durable_writes = true;",
			wantTable: "CREATE TABLE IF NOT EXISTS test_logs.test_statements(pk0 text," +
				"ts bigint, attempt smallint, gemini_attempt smallint, seq bigint, data text, PRIMARY KEY ((pk0), ts, attempt, gemini_attempt, seq))" +
				" WITH caching={'enabled':'false'} AND compression={'sstable_compression':'LZ4Compressor'} " +
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
			replication:  replication.NewNetworkTopologyStrategy(),
			wantKeyspace: "CREATE KEYSPACE IF NOT EXISTS test_logs WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1} AND durable_writes = true;",
			wantTable: "CREATE TABLE IF NOT EXISTS test_logs.test_statements(pk0 text,pk1 int," +
				"ts bigint, attempt smallint, gemini_attempt smallint, seq bigint, data text, PRIMARY KEY ((pk0,pk1), ts, attempt, gemini_attempt, seq))" +
				" WITH caching={'enabled':'false'} AND compression={'sstable_compression':'LZ4Compressor'} " +
				"AND tombstone_gc={'mode':'immediate'} AND comment='Table to store logs from Oracle and Test statements';",
		},
		{
			name:     "uuid partition key",
			keyspace: "ks_logs",
			table:    "tbl_statements",
			partitionKeys: typedef.Columns{
				{
					Name: "id",
					Type: typedef.TypeUUID,
				},
			},
			replication:  replication.NewNetworkTopologyStrategy(),
			wantKeyspace: "CREATE KEYSPACE IF NOT EXISTS ks_logs WITH replication={'class':'NetworkTopologyStrategy','replication_factor':1} AND durable_writes = true;",
			wantTable: "CREATE TABLE IF NOT EXISTS ks_logs.tbl_statements(id uuid," +
				"ts bigint, attempt smallint, gemini_attempt smallint, seq bigint, data text, PRIMARY KEY ((id), ts, attempt, gemini_attempt, seq))" +
				" WITH caching={'enabled':'false'} AND compression={'sstable_compression':'LZ4Compressor'} " +
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

// TestLogColumns pins the column order that fillPairArgs binds after the
// partition key.
func TestLogColumns(t *testing.T) {
	t.Parallel()

	expected := []string{"ts", "attempt", "gemini_attempt", "seq", "data"}

	if !slices.Equal(logColumns, expected) {
		t.Errorf("logColumns = %v, want %v", logColumns, expected)
	}
}

func TestBuildCreateQueriesTableQuery(t *testing.T) {
	t.Parallel()

	want := "CREATE TABLE IF NOT EXISTS ks_logs.queries (id bigint PRIMARY KEY, statement text)" +
		" WITH caching={'enabled':'false'} AND compression={'sstable_compression':'LZ4Compressor'}" +
		" AND tombstone_gc={'mode':'immediate'}" +
		" AND comment='CQL text of the query ids in the statement log tables';"

	if got := buildCreateQueriesTableQuery("ks_logs"); got != want {
		t.Errorf("buildCreateQueriesTableQuery()\ngot:  %q\nwant: %q", got, want)
	}
}
