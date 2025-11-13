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

package jobs

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/typedef"
)

// mockStore is a simple mock implementation of store.Store for testing
type mockStore struct {
	checkFunc func(context.Context, *typedef.Table, *typedef.Stmt, int) (int, error)
}

func (m *mockStore) Create(context.Context, *typedef.Stmt, *typedef.Stmt) error {
	return nil
}

func (m *mockStore) Mutate(context.Context, *typedef.Stmt) error {
	return nil
}

func (m *mockStore) Check(ctx context.Context, table *typedef.Table, stmt *typedef.Stmt, expectedRows int) (int, error) {
	if m.checkFunc != nil {
		return m.checkFunc(ctx, table, stmt, expectedRows)
	}
	return 0, nil
}

func (m *mockStore) Close() error {
	return nil
}

func TestCreateSelectStmtForPartitionKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		table              *typedef.Table
		keyspaceName       string
		partitionKeyValues *typedef.Values
		expectedQuery      string
	}{
		{
			name: "single partition key",
			table: &typedef.Table{
				Name: "test_table",
				PartitionKeys: typedef.Columns{
					{Name: "pk1", Type: typedef.TypeInt},
				},
			},
			keyspaceName: "test_keyspace",
			partitionKeyValues: typedef.NewValuesFromMap(map[string][]any{
				"pk1": {int32(123)},
			}),
			expectedQuery: "SELECT * FROM test_keyspace.test_table WHERE pk1=? ",
		},
		{
			name: "multiple partition keys",
			table: &typedef.Table{
				Name: "test_table",
				PartitionKeys: typedef.Columns{
					{Name: "pk1", Type: typedef.TypeInt},
					{Name: "pk2", Type: typedef.TypeText},
				},
			},
			keyspaceName: "test_keyspace",
			partitionKeyValues: typedef.NewValuesFromMap(map[string][]any{
				"pk1": {int32(123)},
				"pk2": {"test_value"},
			}),
			expectedQuery: "SELECT * FROM test_keyspace.test_table WHERE pk1=? AND pk2=? ",
		},
		{
			name: "uuid partition key",
			table: &typedef.Table{
				Name: "users",
				PartitionKeys: typedef.Columns{
					{Name: "user_id", Type: typedef.TypeUuid},
				},
			},
			keyspaceName: "my_keyspace",
			partitionKeyValues: typedef.NewValuesFromMap(map[string][]any{
				"user_id": {"550e8400-e29b-41d4-a716-446655440000"},
			}),
			expectedQuery: "SELECT * FROM my_keyspace.users WHERE user_id=? ",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			v := &Validation{
				table:        tt.table,
				keyspaceName: tt.keyspaceName,
			}

			stmt := v.createSelectStmtForPartitionKeys(tt.partitionKeyValues)

			require.NotNil(t, stmt)
			require.Equal(t, typedef.SelectStatementType, stmt.QueryType)
			require.Equal(t, tt.expectedQuery, stmt.Query)
			require.Equal(t, tt.partitionKeyValues, stmt.PartitionKeys.Values)
			require.Len(t, stmt.Values, len(tt.table.PartitionKeys))
		})
	}
}

func TestValidateDeletedPartition_Success(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name: "test_table",
		PartitionKeys: typedef.Columns{
			{Name: "pk1", Type: typedef.TypeInt},
		},
	}

	mockSt := &mockStore{
		checkFunc: func(_ context.Context, _ *typedef.Table, _ *typedef.Stmt, _ int) (int, error) {
			// Simulate partition is deleted (0 rows returned)
			return 0, nil
		},
	}

	v := &Validation{
		table:        table,
		keyspaceName: "test_keyspace",
		store:        mockSt,
	}

	partitionKeyValues := typedef.NewValuesFromMap(map[string][]any{
		"pk1": {int32(123)},
	})

	ctx := t.Context()
	err := v.validateDeletedPartition(ctx, partitionKeyValues)

	require.NoError(t, err)
}

func TestValidateDeletedPartition_StoreError(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name: "test_table",
		PartitionKeys: typedef.Columns{
			{Name: "pk1", Type: typedef.TypeInt},
		},
	}

	storeError := errors.New("connection timeout")

	mockSt := &mockStore{
		checkFunc: func(_ context.Context, _ *typedef.Table, _ *typedef.Stmt, _ int) (int, error) {
			return 0, storeError
		},
	}

	v := &Validation{
		table:        table,
		keyspaceName: "test_keyspace",
		store:        mockSt,
	}

	partitionKeyValues := typedef.NewValuesFromMap(map[string][]any{
		"pk1": {int32(123)},
	})

	ctx := t.Context()

	err := v.validateDeletedPartition(ctx, partitionKeyValues)

	require.Error(t, err)

	var jobErr *joberror.JobError
	require.True(t, errors.As(err, &jobErr))
	require.Contains(t, jobErr.Message, "Deleted partition validation failed")
	require.Equal(t, typedef.SelectStatementType, jobErr.StmtType)
}

func TestValidateDeletedPartition_ContextCanceled(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name: "test_table",
		PartitionKeys: typedef.Columns{
			{Name: "pk1", Type: typedef.TypeInt},
		},
	}

	mockSt := &mockStore{
		checkFunc: func(_ context.Context, _ *typedef.Table, _ *typedef.Stmt, _ int) (int, error) {
			return 0, context.Canceled
		},
	}

	v := &Validation{
		table:        table,
		keyspaceName: "test_keyspace",
		store:        mockSt,
	}

	partitionKeyValues := typedef.NewValuesFromMap(map[string][]any{
		"pk1": {int32(123)},
	})

	ctx := t.Context()

	err := v.validateDeletedPartition(ctx, partitionKeyValues)

	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled))
}

func TestValidateDeletedPartition_ContextDeadlineExceeded(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name: "test_table",
		PartitionKeys: typedef.Columns{
			{Name: "pk1", Type: typedef.TypeInt},
		},
	}

	mockSt := &mockStore{
		checkFunc: func(_ context.Context, _ *typedef.Table, _ *typedef.Stmt, _ int) (int, error) {
			return 0, context.DeadlineExceeded
		},
	}

	v := &Validation{
		table:        table,
		keyspaceName: "test_keyspace",
		store:        mockSt,
	}

	partitionKeyValues := typedef.NewValuesFromMap(map[string][]any{
		"pk1": {int32(123)},
	})

	ctx := t.Context()
	err := v.validateDeletedPartition(ctx, partitionKeyValues)

	require.Error(t, err)
	require.True(t, errors.Is(err, context.DeadlineExceeded))
}

func TestValidateDeletedPartition_MultiplePartitionKeys(t *testing.T) {
	t.Parallel()

	table := &typedef.Table{
		Name: "test_table",
		PartitionKeys: typedef.Columns{
			{Name: "pk1", Type: typedef.TypeInt},
			{Name: "pk2", Type: typedef.TypeText},
			{Name: "pk3", Type: typedef.TypeUuid},
		},
	}

	var capturedStmt *typedef.Stmt

	mockSt := &mockStore{
		checkFunc: func(_ context.Context, _ *typedef.Table, stmt *typedef.Stmt, _ int) (int, error) {
			capturedStmt = stmt
			// Partition is deleted (0 rows returned)
			return 0, nil
		},
	}

	v := &Validation{
		table:        table,
		keyspaceName: "test_keyspace",
		store:        mockSt,
	}

	partitionKeyValues := typedef.NewValuesFromMap(map[string][]any{
		"pk1": {int32(123)},
		"pk2": {"test_value"},
		"pk3": {"550e8400-e29b-41d4-a716-446655440000"},
	})

	ctx := t.Context()
	err := v.validateDeletedPartition(ctx, partitionKeyValues)

	// Should succeed - partition is deleted (0 rows)
	require.NoError(t, err)
	require.NotNil(t, capturedStmt)
	require.Len(t, capturedStmt.Values, 3)
	require.Contains(t, capturedStmt.Query, "pk1=?")
	require.Contains(t, capturedStmt.Query, "pk2=?")
	require.Contains(t, capturedStmt.Query, "pk3=?")
}
