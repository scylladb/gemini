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

//go:build testing

package jobs

import (
	"context"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/statements"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/workpool"
)

func TestDeletedPartitions(t *testing.T) {
	t.Parallel()

	containers := testutils.TestContainers(t)
	assert := require.New(t)
	keyspace := testutils.GenerateUniqueKeyspaceName(t)

	// Create keyspace and table in both test and oracle clusters
	assert.NoError(containers.Test.Query(
		fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace),
	).Exec())
	assert.NoError(containers.Oracle.Query(
		fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace),
	).Exec())

	tableName := "test_table"
	createTableQuery := fmt.Sprintf(
		"CREATE TABLE %s.%s (pk1 int, pk2 text, ck1 int, col1 text, PRIMARY KEY ((pk1, pk2), ck1))",
		keyspace, tableName,
	)
	assert.NoError(containers.Test.Query(createTableQuery).Exec())
	assert.NoError(containers.Oracle.Query(createTableQuery).Exec())

	// Define the schema
	schema := &typedef.Schema{
		Keyspace: typedef.Keyspace{
			Name:              keyspace,
			Replication:       replication.NewSimpleStrategy(),
			OracleReplication: replication.NewSimpleStrategy(),
		},
		Config: typedef.SchemaConfig{
			ReplicationStrategy:              replication.NewSimpleStrategy(),
			OracleReplicationStrategy:        replication.NewSimpleStrategy(),
			AsyncObjectStabilizationAttempts: 5,
			AsyncObjectStabilizationDelay:    100 * time.Millisecond,
		},
		Tables: []*typedef.Table{{
			Name: tableName,
			PartitionKeys: typedef.Columns{
				{Name: "pk1", Type: typedef.TypeInt},
				{Name: "pk2", Type: typedef.TypeText},
			},
			ClusteringKeys: typedef.Columns{
				{Name: "ck1", Type: typedef.TypeInt},
			},
			Columns: typedef.Columns{
				{Name: "col1", Type: typedef.TypeText},
			},
		}},
	}

	table := schema.Tables[0]

	// Create store configuration
	storeConfig := store.Config{
		OracleClusterConfig: &store.ScyllaClusterConfig{
			Name:                    stmtlogger.TypeOracle,
			HostSelectionPolicy:     store.HostSelectionTokenAware,
			Consistency:             gocql.Quorum.String(),
			Hosts:                   containers.OracleHosts,
			RequestTimeout:          10 * time.Second,
			ConnectTimeout:          10 * time.Second,
			UseServerSideTimestamps: true,
		},
		TestClusterConfig: store.ScyllaClusterConfig{
			Name:                    stmtlogger.TypeTest,
			HostSelectionPolicy:     store.HostSelectionTokenAware,
			Consistency:             gocql.Quorum.String(),
			Hosts:                   containers.TestHosts,
			RequestTimeout:          10 * time.Second,
			ConnectTimeout:          10 * time.Second,
			UseServerSideTimestamps: false,
		},
		MaxRetriesMutate:                 5,
		MaxRetriesMutateSleep:            100 * time.Millisecond,
		AsyncObjectStabilizationAttempts: 5,
		AsyncObjectStabilizationDelay:    100 * time.Millisecond,
		Compression:                      stmtlogger.CompressionNone,
		UseServerSideTimestamps:          true,
	}

	// Create the store
	workers := workpool.New(10)
	defer workers.Close()

	st, err := store.New(
		typedef.PartitionKeys{},
		workers,
		schema,
		storeConfig,
		zap.NewNop(),
		nil,
	)
	assert.NoError(err)
	defer st.Close()

	// Create partition configuration with short delete buckets for fast testing
	partitionConfig := typedef.PartitionRangeConfig{
		DeleteBuckets:   []time.Duration{100 * time.Millisecond, 200 * time.Millisecond, 300 * time.Millisecond},
		MaxStringLength: 20,
		MinStringLength: 5,
	}

	// Create partitions manager
	seed := rand.NewChaCha8([32]byte{1, 2, 3, 4, 5, 6, 7, 8})
	_, distFunc := distributions.New(distributions.Uniform, 10, 12345, 0, 0)
	parts := partitions.New(
		t.Context(),
		seed,
		distFunc,
		table,
		partitionConfig,
		10, // Start with 10 partitions
	)
	defer parts.Close()

	// Create global status and stop flag
	globalStatus := status.NewGlobalStatus(10)
	stopFlag := stop.NewFlag("test")

	// Create validation job
	ratioRandom := rand.New(rand.NewPCG(12345, 67890))
	ratioController, err := statements.NewRatioController(statements.Ratios{}, ratioRandom)
	assert.NoError(err)
	validation := NewValidation(
		schema,
		table,
		parts,
		globalStatus,
		ratioController,
		stopFlag,
		st,
		[32]byte{1, 2, 3, 4, 5, 6, 7, 8},
	)

	t.Log("=== Phase 1: Test successful validation when partition is actually deleted ===")

	// Insert data into both clusters for partition index 0
	pk1Value := int32(100)
	pk2Value := "test_key_1"
	ck1Value := int32(1)
	col1Value := "test_data_1"

	insertQuery := fmt.Sprintf(
		"INSERT INTO %s.%s (pk1, pk2, ck1, col1) VALUES (?, ?, ?, ?)",
		keyspace, tableName,
	)
	assert.NoError(containers.Test.Query(insertQuery, pk1Value, pk2Value, ck1Value, col1Value).Exec())
	assert.NoError(containers.Oracle.Query(insertQuery, pk1Value, pk2Value, ck1Value, col1Value).Exec())

	// Update partition 0 to have these specific values
	parts.ReplaceWithoutOld(0)

	// Verify the data exists before deletion
	var count int
	selectQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s WHERE pk1 = ? AND pk2 = ?", keyspace, tableName)
	assert.NoError(containers.Test.Query(selectQuery, pk1Value, pk2Value).Scan(&count))
	assert.Equal(1, count, "Data should exist before deletion")

	// Delete the partition from both clusters
	deleteQuery := fmt.Sprintf("DELETE FROM %s.%s WHERE pk1 = ? AND pk2 = ?", keyspace, tableName)
	assert.NoError(containers.Test.Query(deleteQuery, pk1Value, pk2Value).Exec())
	assert.NoError(containers.Oracle.Query(deleteQuery, pk1Value, pk2Value).Exec())

	// Verify deletion
	assert.NoError(containers.Test.Query(selectQuery, pk1Value, pk2Value).Scan(&count))
	assert.Equal(0, count, "Data should be deleted")

	// Now replace the partition - this will mark the old partition as deleted
	oldValues := parts.Replace(0)
	t.Logf("Replaced partition 0, old values marked as deleted: %v", oldValues)

	// Wait for the deleted partition to appear on the channel and be validated
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	validationSucceeded := make(chan bool, 1)
	go func() {
		validationSucceeded <- validation.validateDeletedPartition(ctx, oldValues) == nil
	}()

	select {
	case success := <-validationSucceeded:
		assert.True(success, "Validation should succeed when partition is actually deleted")
		t.Log("✓ Phase 1 passed: Validation succeeded for deleted partition")
	case <-ctx.Done():
		t.Fatal("Timeout waiting for validation")
	}

	t.Log("=== Phase 2: Test failed validation when partition still exists ===")

	// Insert another partition (index 1)
	pk1Value2 := int32(200)
	pk2Value2 := "test_key_2"
	ck1Value2 := int32(2)
	col1Value2 := "test_data_2"

	assert.NoError(containers.Test.Query(insertQuery, pk1Value2, pk2Value2, ck1Value2, col1Value2).Exec())
	assert.NoError(containers.Oracle.Query(insertQuery, pk1Value2, pk2Value2, ck1Value2, col1Value2).Exec())

	// Update partition 1 to have these values
	parts.ReplaceWithoutOld(1)

	// Verify the data exists
	assert.NoError(containers.Test.Query(selectQuery, pk1Value2, pk2Value2).Scan(&count))
	assert.Equal(1, count, "Data should exist")

	// Replace the partition WITHOUT deleting from database - this should cause validation to fail
	oldValues2 := parts.Replace(1)
	t.Logf("Replaced partition 1, old values marked as deleted (but NOT deleted from DB): %v", oldValues2)

	// Try to validate - this should fail because the partition still exists
	ctx2, cancel2 := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel2()

	validationFailed := make(chan error, 1)
	go func() {
		validationFailed <- validation.validateDeletedPartition(ctx2, oldValues2)
	}()

	select {
	case err = <-validationFailed:
		assert.Error(err, "Validation should fail when partition still exists")
		t.Logf("✓ Phase 2 passed: Validation correctly failed with error: %v", err)
	case <-ctx2.Done():
		t.Fatal("Timeout waiting for validation failure")
	}

	t.Log("=== Integration test completed successfully ===")
	t.Logf("Read operations: %d", globalStatus.ReadOps.Load())
	t.Logf("Read errors: %d", globalStatus.ReadErrors.Load())
}
