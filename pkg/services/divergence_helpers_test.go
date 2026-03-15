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
	"crypto/sha256"
	"fmt"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/jobs"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/statements"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
)

// divergenceTable is a minimal, deterministic table used across all divergence
// tests.  One partition key, one clustering key and one plain column — enough
// to exercise INSERT / UPDATE / DELETE without noise from complex types.
var divergenceTable = &typedef.Table{
	Name: "events",
	PartitionKeys: typedef.Columns{
		{Name: "pk", Type: typedef.TypeInt},
	},
	ClusteringKeys: typedef.Columns{
		{Name: "ck", Type: typedef.TypeInt},
	},
	Columns: typedef.Columns{
		{Name: "payload", Type: typedef.TypeText},
	},
}

// divergenceSchema builds a [typedef.Schema] whose keyspace name is derived
// from the test name so each subtest gets its own isolated keyspace.
// The name is hashed to keep it well within CQL's 48-character limit; the
// statement logger appends "_logs", so the base must be ≤ 43 chars.
func divergenceSchema(tb testing.TB) *typedef.Schema {
	tb.Helper()

	// 6-byte SHA-256 prefix → 12 hex chars → "ks_" + 12 = 15 chars total.
	h := sha256.Sum256([]byte(tb.Name()))
	ksName := fmt.Sprintf("ks_%x", h[:6])

	return &typedef.Schema{
		Keyspace: typedef.Keyspace{
			Name:              ksName,
			Replication:       replication.NewSimpleStrategy(),
			OracleReplication: replication.NewSimpleStrategy(),
		},
		Tables: []*typedef.Table{divergenceTable},
		Config: typedef.SchemaConfig{
			ReplicationStrategy:              replication.NewSimpleStrategy(),
			OracleReplicationStrategy:        replication.NewSimpleStrategy(),
			MaxTables:                        1,
			MinPartitionKeys:                 1,
			MaxPartitionKeys:                 1,
			MinClusteringKeys:                1,
			MaxClusteringKeys:                1,
			MinColumns:                       1,
			MaxColumns:                       1,
			MaxStringLength:                  32,
			MinStringLength:                  1,
			MaxBlobLength:                    32,
			MinBlobLength:                    1,
			MaxPKStringLength:                8,
			MinPKStringLength:                1,
			MaxPKBlobLength:                  32,
			MinPKBlobLength:                  1,
			CQLFeature:                       typedef.CQLFeatureNormal,
			AsyncObjectStabilizationDelay:    25 * time.Millisecond,
			AsyncObjectStabilizationAttempts: 5,
			DeleteBuckets:                    []time.Duration{5 * time.Second},
		},
	}
}

// mixedConfig returns a [WorkloadConfig] that both writes and validates
// simultaneously.  Using MixedMode means mutations and validations share the
// same Partitions object, so the validation will always query the exact same
// PKs that the mutations are writing — making mid-run sabotage reliably
// detectable.
func mixedConfig(partitionCount, seed uint64, duration time.Duration) *WorkloadConfig {
	return &WorkloadConfig{
		RunningMode:           jobs.MixedMode,
		PartitionDistribution: distributions.Uniform,
		Seed:                  seed,
		IOWorkerPoolSize:      16,
		MaxErrorsToStore:      1,
		WarmupDuration:        10 * time.Second,
		Duration:              duration,
		PartitionCount:        partitionCount,
		MutationConcurrency:   1,
		ReadConcurrency:       4,
		DropSchema:            true,
		StatementRatios:       divergenceStatementRatios(),
	}
}

// divergenceStatementRatios returns a full [statements.Ratios] suitable for a
// divergence-detection workload: pure inserts during mutations, and
// single-partition selects during validation.
func divergenceStatementRatios() statements.Ratios {
	return statements.Ratios{
		MutationRatios: statements.MutationRatios{
			InsertRatio: 1.0,
			UpdateRatio: 0,
			DeleteRatio: 0,
			InsertSubtypeRatios: statements.InsertRatios{
				RegularInsertRatio: 1.0,
				JSONInsertRatio:    0,
			},
		},
		ValidationRatios: statements.ValidationRatios{
			SelectSubtypeRatios: statements.SelectRatios{
				SinglePartitionRatio: 1.0,
			},
		},
	}
}

// clusterSaboteur wraps raw sessions to the test and oracle clusters.  Each
// method deliberately introduces a specific kind of divergence that Gemini's
// validation should detect.
//
// All destructive methods operate only on the test cluster (SUT) so the oracle
// retains the original, correct state — unless stated otherwise.
type clusterSaboteur struct {
	test   *gocql.Session
	oracle *gocql.Session
	schema *typedef.Schema
}

func newSaboteur(tb testing.TB, containers *testutils.ScyllaContainer, schema *typedef.Schema) *clusterSaboteur {
	tb.Helper()
	return &clusterSaboteur{
		test:   containers.Test,
		oracle: containers.Oracle,
		schema: schema,
	}
}

// qualifiedTable returns the fully-qualified "keyspace.table" identifier.
func (s *clusterSaboteur) qualifiedTable() string {
	return fmt.Sprintf("%s.%s", s.schema.Keyspace.Name, divergenceTable.Name)
}

// truncateSUT wipes all data from the test cluster while leaving the oracle
// intact.  The next validation pass will find rows in the oracle that are
// absent from the SUT and flag them as divergence.
func (s *clusterSaboteur) truncateSUT(tb testing.TB) {
	tb.Helper()
	err := s.test.Query(fmt.Sprintf("TRUNCATE TABLE %s", s.qualifiedTable())).Exec()
	require.NoError(tb, err, "saboteur: truncate SUT")
	tb.Log("saboteur: SUT truncated — oracle still has data, divergence introduced")
}

// truncateOracle wipes all data from the oracle cluster while leaving the SUT
// intact.  The next validation pass will find rows in the SUT that are absent
// from the oracle and flag them as divergence.
func (s *clusterSaboteur) truncateOracle(tb testing.TB) {
	tb.Helper()
	err := s.oracle.Query(fmt.Sprintf("TRUNCATE TABLE %s", s.qualifiedTable())).Exec()
	require.NoError(tb, err, "saboteur: truncate oracle")
	tb.Log("saboteur: oracle truncated — SUT still has data, divergence introduced")
}

// runDivergenceWorkload creates a MixedMode [Workload], schedules the sabotage
// function after sabotageDelay, runs the workload to completion, and returns
// the workload (for status inspection) and any run error.
//
// The sabotage function is executed exactly once inside a time.AfterFunc
// goroutine.  It takes no arguments; callers should close over the test helper
// they need (e.g. via a closure that captures tb).
func runDivergenceWorkload(
	tb testing.TB,
	cfg *WorkloadConfig,
	storeConfig store.Config,
	schema *typedef.Schema,
	sabotageDelay time.Duration,
	sabotage func(),
) (*Workload, error) {
	tb.Helper()

	stopFlag := stop.NewFlag(tb.Name())
	tb.Cleanup(func() { stopFlag.SetHard(true) })

	w, err := NewWorkload(cfg, storeConfig, schema, getLogger(tb), stopFlag)
	require.NoError(tb, err, "workload creation must not fail")

	if sabotage != nil {
		time.AfterFunc(sabotageDelay, sabotage)
	}

	runErr := w.Run(tb.Context())
	require.NoError(tb, w.Close())
	return w, runErr
}
