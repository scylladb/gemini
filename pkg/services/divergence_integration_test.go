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

package services

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/testutils"
)

// TestGeminiDivergenceDetection provisions a single pair of ScyllaDB containers
// (oracle + SUT) shared across all subtests.  Each subtest gets its own
// isolated keyspace, so subtests can run in parallel without interfering with
// each other.
//
// The proven approach used here is a single MixedMode workload where mutations
// and validations share the same Partitions object.  Sabotage is injected
// mid-run via time.AfterFunc so the validation workers query the exact PKs that
// were just mutated and can detect the divergence in the next sweep.
func TestGeminiDivergenceDetection(t *testing.T) {
	t.Parallel()

	// One container pair shared by all subtests to avoid exhausting Docker
	// resources (aio-max-nr, network pool).
	containers := testutils.TestContainers(t)

	t.Run("RowsMissingFromSUT", func(t *testing.T) {
		t.Parallel()
		testGeminiDetectsRowsMissingFromSUT(t, containers)
	})

	t.Run("ExtraRowsInSUT", func(t *testing.T) {
		t.Parallel()
		testGeminiDetectsExtraRowsInSUT(t, containers)
	})

	t.Run("IdenticalClusters", func(t *testing.T) {
		t.Parallel()
		testGeminiAcceptsIdenticalClusters(t, containers)
	})
}

// testGeminiDetectsRowsMissingFromSUT verifies that Gemini detects a read error
// when the test cluster (SUT) is missing rows that the oracle has.
//
// Sequence:
//  1. Run a MixedMode workload that writes to both clusters and validates.
//  2. After ~15 s of warmup, TRUNCATE the SUT table while leaving the oracle
//     intact — the oracle now has more rows than the SUT.
//  3. The next validation sweep queries the same PKs and must detect the
//     discrepancy, returning a non-nil error.
func testGeminiDetectsRowsMissingFromSUT(t *testing.T, containers *testutils.ScyllaContainer) {
	t.Helper()

	const (
		partitionCount = 100
		seed           = 1001
		totalDuration  = 1 * time.Minute
		sabotageAfter  = 15 * time.Second
	)

	schema := divergenceSchema(t)
	storeConfig := getStoreConfig(t, containers.TestHosts, containers.OracleHosts)
	saboteur := newSaboteur(t, containers, schema)

	w, runErr := runDivergenceWorkload(
		t,
		mixedConfig(partitionCount, seed, totalDuration),
		storeConfig,
		schema,
		sabotageAfter,
		func() { saboteur.truncateSUT(t) },
	)

	require.Error(t, runErr, "validation must detect rows missing from SUT and return an error")

	status := w.GetGlobalStatus()
	require.GreaterOrEqual(t, status.ReadErrors.Load(), uint64(1), "at least one read error must be recorded")
	t.Logf("status: WriteOps=%d ReadOps=%d ValidatedRows=%d ReadErrors=%d",
		status.WriteOps.Load(), status.ReadOps.Load(),
		status.ValidatedRows.Load(), status.ReadErrors.Load())
}

// testGeminiDetectsExtraRowsInSUT verifies that Gemini detects a read error
// when the test cluster (SUT) has rows that the oracle does not.
//
// Sequence:
//  1. Run a MixedMode workload that writes to both clusters and validates.
//  2. After ~15 s of warmup, TRUNCATE the oracle table while leaving the SUT
//     intact — the SUT now has more rows than the oracle.
//  3. The next validation sweep queries the same PKs and must detect the
//     discrepancy, returning a non-nil error.
func testGeminiDetectsExtraRowsInSUT(t *testing.T, containers *testutils.ScyllaContainer) {
	t.Helper()

	const (
		partitionCount = 100
		seed           = 1002
		totalDuration  = 1 * time.Minute
		sabotageAfter  = 15 * time.Second
	)

	schema := divergenceSchema(t)
	storeConfig := getStoreConfig(t, containers.TestHosts, containers.OracleHosts)
	saboteur := newSaboteur(t, containers, schema)

	w, runErr := runDivergenceWorkload(
		t,
		mixedConfig(partitionCount, seed, totalDuration),
		storeConfig,
		schema,
		sabotageAfter,
		func() { saboteur.truncateOracle(t) },
	)

	require.Error(t, runErr, "validation must detect extra rows in SUT and return an error")

	status := w.GetGlobalStatus()
	require.GreaterOrEqual(t, status.ReadErrors.Load(), uint64(1), "at least one read error must be recorded")
	t.Logf("status: WriteOps=%d ReadOps=%d ValidatedRows=%d ReadErrors=%d",
		status.WriteOps.Load(), status.ReadOps.Load(),
		status.ValidatedRows.Load(), status.ReadErrors.Load())
}

// testGeminiAcceptsIdenticalClusters is the happy-path counterpart: when both
// clusters contain exactly the same data Gemini must report zero read errors
// and the workload must return nil.
func testGeminiAcceptsIdenticalClusters(t *testing.T, containers *testutils.ScyllaContainer) {
	t.Helper()

	const (
		partitionCount = 100
		seed           = 1003
		totalDuration  = 30 * time.Second
	)

	schema := divergenceSchema(t)
	storeConfig := getStoreConfig(t, containers.TestHosts, containers.OracleHosts)

	// No sabotage — both clusters stay in sync throughout.
	w, runErr := runDivergenceWorkload(
		t,
		mixedConfig(partitionCount, seed, totalDuration),
		storeConfig,
		schema,
		0,
		nil,
	)

	require.NoError(t, runErr, "validation must pass on identical clusters")

	status := w.GetGlobalStatus()
	require.Equal(t, uint64(0), status.ReadErrors.Load(), "no read errors expected")
	require.Equal(t, uint64(0), status.WriteErrors.Load(), "no write errors expected")
	require.Greater(t, status.WriteOps.Load(), uint64(0), "must have performed at least one write")
	require.Greater(t, status.ReadOps.Load(), uint64(0), "must have performed at least one read")
	t.Logf("status: WriteOps=%d ReadOps=%d ValidatedRows=%d",
		status.WriteOps.Load(), status.ReadOps.Load(), status.ValidatedRows.Load())
}
