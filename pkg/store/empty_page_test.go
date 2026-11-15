// Copyright 2026 ScyllaDB
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

package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
)

// TestLoadReadsPastEmptyFirstPage: a page can be empty while later pages hold
// rows. Scylla returns such a page when a filter rejects every row the page
// read, and when a page stops at the tombstone limit. load and loadIter must
// still return the rows of the later pages.
func TestLoadReadsPastEmptyFirstPage(t *testing.T) {
	t.Parallel()

	containers := testutils.TestContainers(t)
	keyspace := testutils.GenerateUniqueKeyspaceName(t)

	require.NoError(t, containers.Test.Query(fmt.Sprintf(
		"CREATE KEYSPACE %s WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}",
		keyspace)).Exec())
	t.Cleanup(func() { _ = containers.Test.Query("DROP KEYSPACE IF EXISTS " + keyspace).Exec() })

	require.NoError(t, containers.Test.Query(fmt.Sprintf(
		"CREATE TABLE %s.t (pk int, ck int, v text, PRIMARY KEY (pk, ck))", keyspace)).Exec())

	const rows = 10
	for ck := range rows {
		v := "miss"
		if ck == rows-1 {
			v = "hit"
		}
		require.NoError(t, containers.Test.Query(fmt.Sprintf(
			"INSERT INTO %s.t (pk, ck, v) VALUES (?, ?, ?)", keyspace), 1, ck, v).Exec())
	}

	cluster := *containers.TestCluster
	cluster.PageSize = 1

	session, err := cluster.CreateSession()
	require.NoError(t, err)
	t.Cleanup(session.Close)

	stmt := &typedef.Stmt{
		Query:     fmt.Sprintf("SELECT pk, ck, v FROM %s.t WHERE pk = ? AND v = ? ALLOW FILTERING", keyspace),
		Values:    []any{1, "hit"},
		QueryType: typedef.SelectStatementType,
	}

	// The case under test: the first page holds no row.
	probe := session.Query(stmt.Query, stmt.Values...)
	iter := probe.Iter()
	require.Zero(t, iter.NumRows(), "precondition: the first page must be empty")
	require.NoError(t, iter.Close())
	probe.Release()

	store := newCQLStoreWithSession(&cluster, nil, zap.NewNop(), "", "test")
	store.session = session

	loaded, err := store.load(t.Context(), stmt)
	require.NoError(t, err)
	require.Len(t, loaded, 1, "load must read past the empty first page")

	var yielded int
	for _, rowErr := range store.loadIter(t.Context(), stmt) {
		require.NoError(t, rowErr)
		yielded++
	}
	require.Equal(t, 1, yielded, "loadIter must read past the empty first page")
}
