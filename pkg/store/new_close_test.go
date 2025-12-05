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

//go:build testing

package store

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
)

func TestNew_BasicConfiguration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	scyllaContainer := testutils.TestContainers(t)
	keyspace := testutils.GenerateUniqueKeyspaceName(t)
	table := "test_table"

	// Create keyspace on both clusters
	require.NoError(t, scyllaContainer.Test.Query(
		fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace),
	).Exec())
	require.NoError(t, scyllaContainer.Oracle.Query(
		fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace),
	).Exec())

	schema := &typedef.Schema{
		Keyspace: typedef.Keyspace{Name: keyspace},
		Config: typedef.SchemaConfig{
			ReplicationStrategy:       replication.NewSimpleStrategy(),
			OracleReplicationStrategy: replication.NewSimpleStrategy(),
		},
		Tables: []*typedef.Table{{
			Name: table,
			Columns: typedef.Columns{
				{Name: "id", Type: typedef.TypeInt},
				{Name: "name", Type: typedef.TypeText},
			},
			PartitionKeys: typedef.Columns{
				{Name: "id", Type: typedef.TypeInt},
			},
		}},
	}

	cfg := Config{
		TestClusterConfig: ScyllaClusterConfig{
			Hosts:               scyllaContainer.TestHosts,
			Consistency:         "QUORUM",
			HostSelectionPolicy: HostSelectionRoundRobin,
		},
		OracleClusterConfig: &ScyllaClusterConfig{
			Hosts:               scyllaContainer.OracleHosts,
			Consistency:         "QUORUM",
			HostSelectionPolicy: HostSelectionRoundRobin,
		},
		MaxRetriesMutate:                 10,
		MaxRetriesMutateSleep:            10 * time.Millisecond,
		AsyncObjectStabilizationAttempts: 10,
		AsyncObjectStabilizationDelay:    10 * time.Millisecond,
	}

	logger := zap.NewNop()
	errorList := joberror.NewErrorList(10)

	store, err := New(
		keyspace,
		table,
		schema.Tables[0].PartitionKeys,
		schema,
		cfg,
		logger,
		errorList,
	)

	require.NoError(t, err)
	require.NotNil(t, store)

	// Clean up
	require.NoError(t, store.Close())
}

func TestNew_WithoutOracleCluster(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	scyllaContainer := testutils.SingleScylla(t)
	keyspace := testutils.GenerateUniqueKeyspaceName(t)
	table := "test_table"

	// Create keyspace on test cluster
	require.NoError(t, scyllaContainer.Test.Query(
		fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace),
	).Exec())

	schema := &typedef.Schema{
		Keyspace: typedef.Keyspace{Name: keyspace},
		Config: typedef.SchemaConfig{
			ReplicationStrategy: replication.NewSimpleStrategy(),
		},
		Tables: []*typedef.Table{{
			Name: table,
			Columns: typedef.Columns{
				{Name: "id", Type: typedef.TypeInt},
				{Name: "name", Type: typedef.TypeText},
			},
			PartitionKeys: typedef.Columns{
				{Name: "id", Type: typedef.TypeInt},
			},
		}},
	}

	cfg := Config{
		TestClusterConfig: ScyllaClusterConfig{
			Hosts:               scyllaContainer.TestHosts,
			Consistency:         "QUORUM",
			HostSelectionPolicy: HostSelectionRoundRobin,
		},
		OracleClusterConfig: nil, // No oracle cluster
	}

	logger := zap.NewNop()
	errorList := joberror.NewErrorList(10)

	store, err := New(
		keyspace,
		table,
		schema.Tables[0].PartitionKeys,
		schema,
		cfg,
		logger,
		errorList,
	)

	require.NoError(t, err)
	require.NotNil(t, store)

	// Clean up
	require.NoError(t, store.Close())
}

func TestNew_DefaultConfiguration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	scyllaContainer := testutils.SingleScylla(t)
	keyspace := testutils.GenerateUniqueKeyspaceName(t)
	table := "test_table"

	require.NoError(t, scyllaContainer.Test.Query(
		fmt.Sprintf("CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}", keyspace),
	).Exec())

	schema := &typedef.Schema{
		Keyspace: typedef.Keyspace{Name: keyspace},
		Config: typedef.SchemaConfig{
			ReplicationStrategy: replication.NewSimpleStrategy(),
		},
		Tables: []*typedef.Table{{
			Name: table,
			Columns: typedef.Columns{
				{Name: "id", Type: typedef.TypeInt},
			},
			PartitionKeys: typedef.Columns{
				{Name: "id", Type: typedef.TypeInt},
			},
		}},
	}

	// Test with default/invalid values to trigger default configuration
	cfg := Config{
		TestClusterConfig: ScyllaClusterConfig{
			Hosts:               scyllaContainer.TestHosts,
			Consistency:         "QUORUM",
			HostSelectionPolicy: HostSelectionRoundRobin,
		},
		MaxRetriesMutate:                 -1, // Should default to 10
		MaxRetriesMutateSleep:            0,  // Should default to 10ms
		AsyncObjectStabilizationAttempts: 0,  // Should default to 10
		AsyncObjectStabilizationDelay:    0,  // Should default to 25ms
		MinimumDelay:                     0,  // Should default to 25ms
	}

	logger := zap.NewNop()
	errorList := joberror.NewErrorList(10)

	store, err := New(
		keyspace,
		table,
		schema.Tables[0].PartitionKeys,
		schema,
		cfg,
		logger,
		errorList,
	)

	require.NoError(t, err)
	require.NotNil(t, store)

	// Verify defaults were applied
	ds, ok := store.(*delegatingStore)
	require.True(t, ok)
	assert.Equal(t, 10, ds.mutationRetries)
	assert.Equal(t, 10*time.Millisecond, ds.mutationRetrySleep)
	assert.Equal(t, 10, ds.validationRetries)
	assert.Equal(t, 25*time.Millisecond, ds.validationRetrySleep)
	assert.Equal(t, 25*time.Millisecond, ds.minimumDelay)

	require.NoError(t, store.Close())
}

func TestNew_InvalidTestCluster(t *testing.T) {
	keyspace := "test_keyspace"
	table := "test_table"

	schema := &typedef.Schema{
		Keyspace: typedef.Keyspace{Name: keyspace},
		Config: typedef.SchemaConfig{
			ReplicationStrategy: replication.NewSimpleStrategy(),
		},
		Tables: []*typedef.Table{{
			Name: table,
			Columns: typedef.Columns{
				{Name: "id", Type: typedef.TypeInt},
			},
			PartitionKeys: typedef.Columns{
				{Name: "id", Type: typedef.TypeInt},
			},
		}},
	}

	cfg := Config{
		TestClusterConfig: ScyllaClusterConfig{
			Hosts:               []string{"invalid.host.doesnotexist:9042"},
			Consistency:         "QUORUM",
			HostSelectionPolicy: HostSelectionRoundRobin,
			ConnectTimeout:      100 * time.Millisecond,
		},
	}

	logger := zap.NewNop()
	errorList := joberror.NewErrorList(10)

	store, err := New(
		keyspace,
		table,
		schema.Tables[0].PartitionKeys,
		schema,
		cfg,
		logger,
		errorList,
	)

	assert.Error(t, err)
	assert.Nil(t, store)
}

func TestDelegatingStore_Close(t *testing.T) {
	t.Parallel()

	t.Run("close with nil logger", func(t *testing.T) {
		t.Parallel()

		testStore := &mockStoreLoader{}
		testStore.On("Close").Return(nil)

		ds := &delegatingStore{
			testStore:       testStore,
			logger:          nil, // Nil logger should not panic
			statementLogger: nil,
		}

		err := ds.Close()
		assert.NoError(t, err)
	})

	t.Run("close without statement logger", func(t *testing.T) {
		t.Parallel()

		testStore := &mockStoreLoader{}
		testStore.On("Close").Return(nil)

		ds := &delegatingStore{
			testStore:       testStore,
			logger:          zap.NewNop(),
			statementLogger: nil,
		}

		err := ds.Close()
		assert.NoError(t, err)
	})
}

func TestDelegatingStore_GetLogger(t *testing.T) {
	t.Parallel()

	t.Run("with logger", func(t *testing.T) {
		t.Parallel()

		logger := zap.NewNop()
		ds := delegatingStore{
			logger: logger,
		}

		result := ds.getLogger()
		assert.Equal(t, logger, result)
	})

	t.Run("without logger", func(t *testing.T) {
		t.Parallel()

		ds := delegatingStore{
			logger: nil,
		}

		result := ds.getLogger()
		assert.NotNil(t, result)
		// Should return a nop logger, not nil
	})
}
