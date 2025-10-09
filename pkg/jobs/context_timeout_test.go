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
	"math/rand/v2"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/generators/statements"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

// mockStore is a mock implementation of store.Store for testing
type mockStore struct {
	mutateDelay time.Duration
	checkDelay  time.Duration
}

func (m *mockStore) Mutate(_ context.Context, _ *typedef.Stmt) error {
	// Simulate a mutation operation that takes some time
	time.Sleep(m.mutateDelay)
	return nil
}

func (m *mockStore) Check(ctx context.Context, _ *typedef.Table, _ *typedef.Stmt, _ int) (int, error) {
	// Simulate a check operation that takes some time
	select {
	case <-time.After(m.checkDelay):
		return 1, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (m *mockStore) Create(_ context.Context, _, _ *typedef.Stmt) error {
	return nil
}

func (m *mockStore) Close() error {
	return nil
}

// mockGenerator is a mock implementation of generators.Interface for testing
type mockGenerator struct{}

func (m *mockGenerator) Get(_ context.Context) (typedef.PartitionKeys, error) {
	// Create Values with proper map structure matching the test schema
	// Schema has pk1 (bigint) as partition key
	values := typedef.NewValuesFromMap(map[string][]any{
		"pk1": {int64(1)},
	})

	return typedef.PartitionKeys{
		Values: values,
		Token:  1,
	}, nil
}

func (m *mockGenerator) GetOld(_ context.Context) (typedef.PartitionKeys, error) {
	// Create Values with proper map structure matching the test schema
	values := typedef.NewValuesFromMap(map[string][]any{
		"pk1": {int64(1)},
	})

	return typedef.PartitionKeys{
		Values: values,
		Token:  1,
	}, nil
}

func (m *mockGenerator) GiveOlds(_ context.Context, _ ...typedef.PartitionKeys) {}

// createTestSchema creates a simple test schema
func createTestSchema() *typedef.Schema {
	return &typedef.Schema{
		Keyspace: typedef.Keyspace{
			Name: "test_ks",
		},
		Tables: []*typedef.Table{
			{
				Name: "test_table",
				PartitionKeys: typedef.Columns{
					{Name: "pk1", Type: typedef.TypeBigint},
				},
				ClusteringKeys: typedef.Columns{
					{Name: "ck1", Type: typedef.TypeText},
				},
				Columns: typedef.Columns{
					{Name: "col1", Type: typedef.TypeText},
				},
			},
		},
		Config: typedef.SchemaConfig{
			AsyncObjectStabilizationAttempts: 1,
			AsyncObjectStabilizationDelay:    10 * time.Millisecond,
			MaxStringLength:                  10,
			MinStringLength:                  1,
			MaxBlobLength:                    10,
			MinBlobLength:                    1,
		},
	}
}

// TestMutationRespectsContextTimeout tests that Mutation.Do() respects context timeout
func TestMutationRespectsContextTimeout(t *testing.T) {
	// Initialize random string pool for string generation
	utils.PreallocateRandomString(rand.New(rand.NewChaCha8([32]byte{})), 1000)

	schema := createTestSchema()
	table := schema.Tables[0]

	// Create a mock store with very short delays to simulate fast operations
	store := &mockStore{
		mutateDelay: 1 * time.Millisecond,
	}

	generator := &mockGenerator{}
	globalStatus := status.NewGlobalStatus(10)
	stopFlag := stop.NewFlag("test")

	// Create statement ratio controller
	ratios := statements.DefaultStatementRatios()
	ratioController, err := statements.NewRatioController(ratios, rand.New(rand.NewChaCha8([32]byte{})))
	require.NoError(t, err)

	// Create mutation job
	mutation := NewMutation(
		schema,
		table,
		generator,
		globalStatus,
		ratioController,
		stopFlag,
		store,
		false,
		[32]byte{},
	)

	// Test with a short timeout
	timeout := 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	start := time.Now()
	err = mutation.Do(ctx)
	elapsed := time.Since(start)

	// The job should complete within a reasonable time after the timeout
	// Allow 50ms grace period for cleanup
	maxExpected := timeout + 50*time.Millisecond

	require.NoError(t, err, "Mutation.Do() should not return an error when context is cancelled")
	require.Less(t, elapsed, maxExpected,
		"Mutation.Do() should respect context timeout and exit within %v, but took %v", maxExpected, elapsed)

	t.Logf("Mutation job completed in %v (timeout was %v)", elapsed, timeout)
}

// TestValidationRespectsContextTimeout tests that Validation.Do() respects context timeout
func TestValidationRespectsContextTimeout(t *testing.T) {
	// Initialize random string pool for string generation
	utils.PreallocateRandomString(rand.New(rand.NewChaCha8([32]byte{})), 1000)

	schema := createTestSchema()
	table := schema.Tables[0]

	// Create a mock store with very short delays to simulate fast operations
	store := &mockStore{
		checkDelay: 1 * time.Millisecond,
	}

	generator := &mockGenerator{}
	globalStatus := status.NewGlobalStatus(10)
	stopFlag := stop.NewFlag("test")

	// Create statement ratio controller
	ratios := statements.DefaultStatementRatios()
	ratioController, err := statements.NewRatioController(ratios, rand.New(rand.NewChaCha8([32]byte{})))
	require.NoError(t, err)

	// Create validation job
	validation := NewValidation(
		schema,
		table,
		generator,
		globalStatus,
		ratioController,
		stopFlag,
		store,
		[32]byte{},
	)

	// Test with a short timeout
	timeout := 100 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	start := time.Now()
	err = validation.Do(ctx)
	elapsed := time.Since(start)

	// The job should complete within a reasonable time after the timeout
	// Allow 50ms grace period for cleanup
	maxExpected := timeout + 50*time.Millisecond

	require.NoError(t, err, "Validation.Do() should not return an error when context is cancelled")
	require.Less(t, elapsed, maxExpected,
		"Validation.Do() should respect context timeout and exit within %v, but took %v", maxExpected, elapsed)

	t.Logf("Validation job completed in %v (timeout was %v)", elapsed, timeout)
}

// TestMutationWithoutContextCheckStalls tests that without the context check, mutation stalls
// This test should FAIL without the fix and PASS with the fix
func TestMutationWithoutContextCheckWouldStall(t *testing.T) {
	// Initialize random string pool for string generation
	utils.PreallocateRandomString(rand.New(rand.NewChaCha8([32]byte{})), 1000)

	// This test documents the behavior we're fixing
	// With the fix (context check in the loop), the job exits quickly
	// Without the fix, it would stall indefinitely

	schema := createTestSchema()
	table := schema.Tables[0]

	// Create a mock store with operations that complete successfully
	// but the loop would continue indefinitely without context checks
	store := &mockStore{
		mutateDelay: 5 * time.Millisecond,
	}

	generator := &mockGenerator{}
	globalStatus := status.NewGlobalStatus(10)
	stopFlag := stop.NewFlag("test")

	ratios := statements.DefaultStatementRatios()
	ratioController, err := statements.NewRatioController(ratios, rand.New(rand.NewChaCha8([32]byte{})))
	require.NoError(t, err)

	mutation := NewMutation(
		schema,
		table,
		generator,
		globalStatus,
		ratioController,
		stopFlag,
		store,
		false,
		[32]byte{},
	)

	// Set a short timeout
	timeout := 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	// Channel to track if the job completes
	done := make(chan bool, 1)

	go func() {
		_ = mutation.Do(ctx)
		done <- true
	}()

	// Wait for either completion or a longer timeout
	stallDetectionTimeout := timeout + 200*time.Millisecond
	select {
	case <-done:
		// Job completed successfully - this is the expected behavior WITH the fix
		t.Log("✓ Mutation job exited promptly when context was cancelled (fix is working)")
	case <-time.After(stallDetectionTimeout):
		t.Fatal("Mutation job did not exit within expected time after context cancellation - " +
			"this indicates the context check is missing from the loop")
	}
}

// TestValidationWithoutContextCheckStalls tests that without the context check, validation stalls
// This test should FAIL without the fix and PASS with the fix
func TestValidationWithoutContextCheckWouldStall(t *testing.T) {
	// Initialize random string pool for string generation
	utils.PreallocateRandomString(rand.New(rand.NewChaCha8([32]byte{})), 1000)

	// This test documents the behavior we're fixing
	// With the fix (context check in the loop), the job exits quickly
	// Without the fix, it would stall indefinitely

	schema := createTestSchema()
	table := schema.Tables[0]

	// Create a mock store with operations that complete successfully
	store := &mockStore{
		checkDelay: 5 * time.Millisecond,
	}

	generator := &mockGenerator{}
	globalStatus := status.NewGlobalStatus(10)
	stopFlag := stop.NewFlag("test")

	ratios := statements.DefaultStatementRatios()
	ratioController, err := statements.NewRatioController(ratios, rand.New(rand.NewChaCha8([32]byte{})))
	require.NoError(t, err)

	validation := NewValidation(
		schema,
		table,
		generator,
		globalStatus,
		ratioController,
		stopFlag,
		store,
		[32]byte{},
	)

	// Set a short timeout
	timeout := 50 * time.Millisecond
	ctx, cancel := context.WithTimeout(t.Context(), timeout)
	defer cancel()

	// Channel to track if the job completes
	done := make(chan bool, 1)

	go func() {
		_ = validation.Do(ctx)
		done <- true
	}()

	// Wait for either completion or a longer timeout
	stallDetectionTimeout := timeout + 200*time.Millisecond
	select {
	case <-done:
		// Job completed successfully - this is the expected behavior WITH the fix
		t.Log("✓ Validation job exited promptly when context was cancelled (fix is working)")
	case <-time.After(stallDetectionTimeout):
		t.Fatal("Validation job did not exit within expected time after context cancellation - " +
			"this indicates the context check is missing from the loop")
	}
}
