// Copyright 2025 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package schema_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/schema"
	"github.com/scylladb/gemini/pkg/typedef"
)

func TestGetReplicationStrategy_Network(t *testing.T) {
	t.Parallel()
	logger := zap.NewNop()
	fallback := replication.NewSimpleStrategy()

	got := schema.GetReplicationStrategy("network", fallback, logger)

	if got["class"] != "NetworkTopologyStrategy" {
		t.Errorf("class = %v; want NetworkTopologyStrategy", got["class"])
	}
}

func TestGetReplicationStrategy_Simple(t *testing.T) {
	t.Parallel()
	logger := zap.NewNop()
	fallback := replication.NewNetworkTopologyStrategy()

	got := schema.GetReplicationStrategy("simple", fallback, logger)

	if got["class"] != "SimpleStrategy" {
		t.Errorf("class = %v; want SimpleStrategy", got["class"])
	}
}

func TestGetReplicationStrategy_ValidJSON(t *testing.T) {
	t.Parallel()
	logger := zap.NewNop()
	fallback := replication.NewSimpleStrategy()

	// Use single-quoted JSON (the replacer converts ' -> ") to simulate real usage.
	raw := `{'class': 'NetworkTopologyStrategy', 'dc1': 3}`
	got := schema.GetReplicationStrategy(raw, fallback, logger)

	if got["class"] != "NetworkTopologyStrategy" {
		t.Errorf("class = %v; want NetworkTopologyStrategy", got["class"])
	}
}

func TestGetReplicationStrategy_InvalidJSON_ReturnsFallback(t *testing.T) {
	t.Parallel()
	logger := zap.NewNop()
	fallback := replication.NewSimpleStrategy()

	got := schema.GetReplicationStrategy("not-valid-json-{{{", fallback, logger)

	// Must return fallback unchanged.
	wantClass := fallback["class"]
	if got["class"] != wantClass {
		t.Errorf("class = %v; want fallback class %v", got["class"], wantClass)
	}
}

func TestRead_ValidSchemaFile(t *testing.T) {
	t.Parallel()

	// Build a minimal valid schema JSON and write it to a temp file.
	raw := typedef.Schema{
		Keyspace: typedef.Keyspace{
			Name:              "test_ks",
			Replication:       replication.NewSimpleStrategy(),
			OracleReplication: replication.NewSimpleStrategy(),
		},
		Tables: []*typedef.Table{
			{
				Name: "t1",
				PartitionKeys: typedef.Columns{
					{Name: "pk1", Type: typedef.TypeInt},
				},
				ClusteringKeys: typedef.Columns{},
				Columns:        typedef.Columns{{Name: "v1", Type: typedef.TypeText}},
			},
		},
	}

	data, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal schema: %v", err)
	}

	dir := t.TempDir()
	schemaFile := filepath.Join(dir, "schema.json")
	if err = os.WriteFile(schemaFile, data, 0o600); err != nil {
		t.Fatalf("write schema file: %v", err)
	}

	cfg := typedef.SchemaConfig{
		MaxPartitionKeys:  2,
		MinPartitionKeys:  1,
		MaxClusteringKeys: 2,
		MinClusteringKeys: 0,
		MaxColumns:        5,
		MinColumns:        1,
	}

	got, err := schema.Read(schemaFile, cfg)
	if err != nil {
		t.Fatalf("Read() error: %v", err)
	}
	if got == nil {
		t.Fatal("Read() returned nil schema")
	}
	if got.Keyspace.Name != "test_ks" {
		t.Errorf("keyspace name = %q; want %q", got.Keyspace.Name, "test_ks")
	}
	if len(got.Tables) != 1 {
		t.Errorf("tables count = %d; want 1", len(got.Tables))
	}
}

func TestRead_MissingFile_ReturnsError(t *testing.T) {
	t.Parallel()
	cfg := typedef.SchemaConfig{}

	_, err := schema.Read("/nonexistent/path/schema.json", cfg)
	if err == nil {
		t.Error("expected error for missing file, got nil")
	}
}

func TestRead_InvalidJSON_ReturnsError(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	badFile := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(badFile, []byte("{not valid json"), 0o600); err != nil {
		t.Fatalf("write bad file: %v", err)
	}

	_, err := schema.Read(badFile, typedef.SchemaConfig{})
	if err == nil {
		t.Error("expected error for invalid JSON, got nil")
	}
}

func TestGet_WithSchemaFile(t *testing.T) {
	t.Parallel()

	raw := typedef.Schema{
		Keyspace: typedef.Keyspace{
			Name:              "ks_get",
			Replication:       replication.NewSimpleStrategy(),
			OracleReplication: replication.NewSimpleStrategy(),
		},
		Tables: []*typedef.Table{
			{
				Name:           "t1",
				PartitionKeys:  typedef.Columns{{Name: "pk", Type: typedef.TypeInt}},
				ClusteringKeys: typedef.Columns{},
				Columns:        typedef.Columns{{Name: "v", Type: typedef.TypeText}},
			},
		},
	}

	data, err := json.Marshal(raw)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	dir := t.TempDir()
	schemaFile := filepath.Join(dir, "schema.json")
	if err = os.WriteFile(schemaFile, data, 0o600); err != nil {
		t.Fatalf("write file: %v", err)
	}

	cfg := typedef.SchemaConfig{
		MaxPartitionKeys:  2,
		MinPartitionKeys:  1,
		MaxClusteringKeys: 2,
		MinClusteringKeys: 0,
		MaxColumns:        5,
		MinColumns:        1,
	}

	got, err := schema.Get(cfg, "", schemaFile)
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if got == nil {
		t.Fatal("Get() returned nil")
	}
	if got.Keyspace.Name != "ks_get" {
		t.Errorf("keyspace = %q; want ks_get", got.Keyspace.Name)
	}
}

func TestGet_WithSeed_GeneratesSchema(t *testing.T) {
	t.Parallel()

	cfg := typedef.SchemaConfig{
		MaxPartitionKeys:  2,
		MinPartitionKeys:  1,
		MaxClusteringKeys: 2,
		MinClusteringKeys: 0,
		MaxColumns:        5,
		MinColumns:        1,
		MaxTables:         1,
		MaxTupleParts:     3,
		MaxUDTParts:       3,
		MaxBlobLength:     256,
		MaxStringLength:   128,
		CQLFeature:        typedef.CQLFeatureBasic,
	}

	got, err := schema.Get(cfg, "deterministic-seed-42", "")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}
	if got == nil {
		t.Fatal("Get() returned nil")
	}
	if len(got.Tables) == 0 {
		t.Error("Get() with seed produced no tables")
	}
}

func TestGet_InvalidConfig_ReturnsError(t *testing.T) {
	t.Parallel()

	// MaxPartitionKeys <= MinPartitionKeys triggers validation error.
	cfg := typedef.SchemaConfig{
		MaxPartitionKeys:  1,
		MinPartitionKeys:  1,
		MaxClusteringKeys: 2,
		MinClusteringKeys: 0,
		MaxColumns:        5,
		MinColumns:        1,
	}

	_, err := schema.Get(cfg, "seed", "")
	if err == nil {
		t.Error("expected error for invalid schema config, got nil")
	}
}
