//go:build testing

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

package typedef_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/testutils"
)

// TestComplexTypesAsPartitionKeys_Integration demonstrates that complex types (frozen collections, tuples, UDTs)
// can be safely used as partition keys in actual ScyllaDB/Cassandra databases.
// This integration test verifies that the schema generation and data operations work correctly.
func TestComplexTypesAsPartitionKeys_Integration(t *testing.T) {
	t.Parallel()

	containers := testutils.TestContainers(t)
	
	testCluster := gocql.NewCluster(containers.TestHosts[0])
	testCluster.Consistency = gocql.Quorum
	testCluster.Timeout = 30 * time.Second
	session, err := testCluster.CreateSession()
	require.NoError(t, err, "Failed to create session")
	defer session.Close()

	keyspace := testutils.GenerateUniqueKeyspaceName(t)

	// Create keyspace
	createKsErr := session.Query(fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
		keyspace,
	)).Exec()
	require.NoError(t, createKsErr, "Failed to create keyspace")

	// Test frozen list as partition key
	t.Run("FrozenListAsPartitionKey", func(t *testing.T) {
		tableName := "test_list_pk"
		createTableQuery := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (pk frozen<list<int>> PRIMARY KEY, v int)",
			keyspace, tableName,
		)
		err := session.Query(createTableQuery).Exec()
		require.NoError(t, err, "Failed to create table with frozen list partition key")

		// Insert data
		insertQuery := fmt.Sprintf("INSERT INTO %s.%s (pk, v) VALUES (?, ?)", keyspace, tableName)
		err = session.Query(insertQuery, []int{1, 2, 3}, 100).Exec()
		require.NoError(t, err, "Failed to insert data with list partition key")

		// Select data
		selectQuery := fmt.Sprintf("SELECT v FROM %s.%s WHERE pk = ?", keyspace, tableName)
		var value int
		err = session.Query(selectQuery, []int{1, 2, 3}).Scan(&value)
		require.NoError(t, err, "Failed to select data with list partition key")
		require.Equal(t, 100, value, "Retrieved value doesn't match")
	})

	// Test frozen set as partition key
	t.Run("FrozenSetAsPartitionKey", func(t *testing.T) {
		tableName := "test_set_pk"
		createTableQuery := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (pk frozen<set<text>> PRIMARY KEY, v int)",
			keyspace, tableName,
		)
		err := session.Query(createTableQuery).Exec()
		require.NoError(t, err, "Failed to create table with frozen set partition key")

		// Insert data
		insertQuery := fmt.Sprintf("INSERT INTO %s.%s (pk, v) VALUES (?, ?)", keyspace, tableName)
		testSet := map[string]struct{}{"a": {}, "b": {}, "c": {}}
		err = session.Query(insertQuery, testSet, 200).Exec()
		require.NoError(t, err, "Failed to insert data with set partition key")

		// Select data
		selectQuery := fmt.Sprintf("SELECT v FROM %s.%s WHERE pk = ?", keyspace, tableName)
		var value int
		err = session.Query(selectQuery, testSet).Scan(&value)
		require.NoError(t, err, "Failed to select data with set partition key")
		require.Equal(t, 200, value, "Retrieved value doesn't match")
	})

	// Test frozen map as partition key
	t.Run("FrozenMapAsPartitionKey", func(t *testing.T) {
		tableName := "test_map_pk"
		createTableQuery := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (pk frozen<map<text, int>> PRIMARY KEY, v int)",
			keyspace, tableName,
		)
		err := session.Query(createTableQuery).Exec()
		require.NoError(t, err, "Failed to create table with frozen map partition key")

		// Insert data
		insertQuery := fmt.Sprintf("INSERT INTO %s.%s (pk, v) VALUES (?, ?)", keyspace, tableName)
		testMap := map[string]int{"x": 1, "y": 2}
		err = session.Query(insertQuery, testMap, 300).Exec()
		require.NoError(t, err, "Failed to insert data with map partition key")

		// Select data
		selectQuery := fmt.Sprintf("SELECT v FROM %s.%s WHERE pk = ?", keyspace, tableName)
		var value int
		err = session.Query(selectQuery, testMap).Scan(&value)
		require.NoError(t, err, "Failed to select data with map partition key")
		require.Equal(t, 300, value, "Retrieved value doesn't match")
	})

	// Test frozen tuple as partition key
	t.Run("FrozenTupleAsPartitionKey", func(t *testing.T) {
		tableName := "test_tuple_pk"
		createTableQuery := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (pk frozen<tuple<int, text>> PRIMARY KEY, v int)",
			keyspace, tableName,
		)
		err := session.Query(createTableQuery).Exec()
		require.NoError(t, err, "Failed to create table with frozen tuple partition key")

		// Insert data using tuple as slice
		insertQuery := fmt.Sprintf("INSERT INTO %s.%s (pk, v) VALUES (?, ?)", keyspace, tableName)
		testTuple := []interface{}{1, "test"}
		err = session.Query(insertQuery, testTuple, 400).Exec()
		require.NoError(t, err, "Failed to insert data with tuple partition key")

		// Select data
		selectQuery := fmt.Sprintf("SELECT v FROM %s.%s WHERE pk = ?", keyspace, tableName)
		var value int
		err = session.Query(selectQuery, testTuple).Scan(&value)
		require.NoError(t, err, "Failed to select data with tuple partition key")
		require.Equal(t, 400, value, "Retrieved value doesn't match")
	})

	// Test UDT as partition key
	t.Run("FrozenUDTAsPartitionKey", func(t *testing.T) {
		tableName := "test_udt_pk"
		typeName := "test_address"

		// Create UDT
		createTypeQuery := fmt.Sprintf(
			"CREATE TYPE IF NOT EXISTS %s.%s (street text, city text, zip int)",
			keyspace, typeName,
		)
		err := session.Query(createTypeQuery).Exec()
		require.NoError(t, err, "Failed to create UDT")

		// Create table with frozen UDT as partition key
		createTableQuery := fmt.Sprintf(
			"CREATE TABLE IF NOT EXISTS %s.%s (pk frozen<%s> PRIMARY KEY, v int)",
			keyspace, tableName, typeName,
		)
		err = session.Query(createTableQuery).Exec()
		require.NoError(t, err, "Failed to create table with frozen UDT partition key")

		// Insert data
		insertQuery := fmt.Sprintf("INSERT INTO %s.%s (pk, v) VALUES (?, ?)", keyspace, tableName)
		testUDT := map[string]interface{}{
			"street": "123 Main St",
			"city":   "Anytown",
			"zip":    12345,
		}
		err = session.Query(insertQuery, testUDT, 500).Exec()
		require.NoError(t, err, "Failed to insert data with UDT partition key")

		// Select data
		selectQuery := fmt.Sprintf("SELECT v FROM %s.%s WHERE pk = ?", keyspace, tableName)
		var value int
		err = session.Query(selectQuery, testUDT).Scan(&value)
		require.NoError(t, err, "Failed to select data with UDT partition key")
		require.Equal(t, 500, value, "Retrieved value doesn't match")
	})

	// Clean up keyspace
	t.Cleanup(func() {
		_ = session.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", keyspace)).Exec()
	})
}
