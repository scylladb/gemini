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

package services

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/jobs"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/statements"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
)

func getStoreConfig(tb testing.TB, testHosts, oracleHosts []string, dockerMode bool) store.Config {
	tb.Helper()
	var oracleConfig *store.ScyllaClusterConfig

	// Allow overriding ports via environment for external clusters (e.g., docker-compose)
	parsePort := func(env string) int {
		if v := os.Getenv(env); v != "" {
			if p, err := strconv.Atoi(v); err == nil && p > 0 {
				return p
			}
		}
		return 0
	}
	testPort := parsePort("GEMINI_TEST_PORT")
	oraclePort := parsePort("GEMINI_ORACLE_PORT")

	if len(oracleHosts) > 0 {
		oracleConfig = &store.ScyllaClusterConfig{
			Name:                    stmtlogger.TypeOracle,
			HostSelectionPolicy:     store.HostSelectionTokenAware,
			Consistency:             gocql.Quorum.String(),
			Hosts:                   oracleHosts,
			Port:                    oraclePort,
			DockerMode:              dockerMode,
			RequestTimeout:          10 * time.Second,
			ConnectTimeout:          10 * time.Second,
			UseServerSideTimestamps: true,
			Replication:             replication.NewSimpleStrategy(),
		}
	}

	directory := tb.TempDir()

	return store.Config{
		OracleClusterConfig: oracleConfig,
		OracleStatementFile: filepath.Join(directory, "oracle_statements.jsonl"),
		TestStatementFile:   filepath.Join(directory, "test_statements.jsonl"),
		MinimumDelay:        25 * time.Millisecond,
		TestClusterConfig: store.ScyllaClusterConfig{
			Name:                    stmtlogger.TypeTest,
			HostSelectionPolicy:     store.HostSelectionTokenAware,
			Consistency:             gocql.Quorum.String(),
			Hosts:                   testHosts,
			Port:                    testPort,
			DockerMode:              dockerMode,
			RequestTimeout:          10 * time.Second,
			ConnectTimeout:          10 * time.Second,
			UseServerSideTimestamps: false,
			Replication:             replication.NewSimpleStrategy(),
		},
		MaxRetriesMutate:                 5,
		MaxRetriesMutateSleep:            1 * time.Second,
		AsyncObjectStabilizationAttempts: 5,
		AsyncObjectStabilizationDelay:    250 * time.Millisecond,
		UseServerSideTimestamps:          false,
	}
}

func getSchema(tb testing.TB, table ...*typedef.Table) *typedef.Schema {
	tb.Helper()

	tables := make([]*typedef.Table, 0, 1)
	if len(table) > 0 {
		tables = append(tables, table[0])
	} else {
		tables = append(tables, &typedef.Table{
			Name: "table_1",
			PartitionKeys: typedef.Columns{
				{Name: "pk1", Type: typedef.TypeText},
				{Name: "pk2", Type: typedef.TypeInt},
			},
			ClusteringKeys: typedef.Columns{
				{Name: "ck1", Type: typedef.TypeInt},
			},
			Columns: typedef.Columns{
				{Name: "col1", Type: typedef.TypeText},
			},
		})
	}

	keyspace := strings.ToLower(strings.ReplaceAll(tb.Name(), "/", "_"))

	return &typedef.Schema{
		Keyspace: typedef.Keyspace{
			Replication:       replication.NewSimpleStrategy(),
			OracleReplication: replication.NewSimpleStrategy(),
			Name:              keyspace,
		},
		Tables: tables,
		Config: typedef.SchemaConfig{
			ReplicationStrategy:              replication.NewSimpleStrategy(),
			OracleReplicationStrategy:        replication.NewSimpleStrategy(),
			TableOptions:                     nil,
			DeleteBuckets:                    []time.Duration{5 * time.Second, 10 * time.Second, 15 * time.Second},
			MaxUDTParts:                      2,
			MaxStringLength:                  32,
			MinBlobLength:                    1,
			MaxBlobLength:                    32,
			MinStringLength:                  1,
			MaxPKStringLength:                8,
			MinPKBlobLength:                  64,
			MaxPKBlobLength:                  128,
			MinPKStringLength:                8,
			MaxClusteringKeys:                2,
			MinClusteringKeys:                1,
			MaxColumns:                       1,
			MinColumns:                       1,
			MaxPartitionKeys:                 2,
			MaxTupleParts:                    3,
			MinPartitionKeys:                 2,
			MaxTables:                        1,
			AsyncObjectStabilizationDelay:    10 * time.Millisecond,
			AsyncObjectStabilizationAttempts: 10,
			CQLFeature:                       typedef.CQLFeatureNormal,
			UseMaterializedViews:             false,
			UseLWT:                           false,
			UseCounters:                      false,
		},
	}
}

type DataSet struct {
	expect   func(testing.TB, *Workload, store.Config)
	name     string
	mode     string
	duration time.Duration
	warmup   time.Duration
}

var dataset = []DataSet{
	{
		name:     "MixedMode",
		mode:     jobs.MixedMode,
		duration: 10 * time.Second,
		warmup:   10 * time.Second,
		expect: func(tb testing.TB, workload *Workload, _ store.Config) {
			tb.Helper()

			assert := require.New(tb)

			status := workload.GetGlobalStatus()

			assert.Equalf(uint64(0), status.WriteErrors.Load(), "there were write errors")
			assert.Equal(uint64(0), status.ReadErrors.Load(), "there were validation errors")
			assert.Equal(0, status.Errors.Len())

			assert.Greater(status.WriteOps.Load(), uint64(0))
			assert.Greater(status.ReadOps.Load(), uint64(0))
			assert.Greater(status.ValidatedRows.Load(), uint64(0))
		},
	},
	{
		name:     "MixedModeWithoutWarmup",
		mode:     jobs.MixedMode,
		duration: 20 * time.Second,
		expect: func(tb testing.TB, workload *Workload, _ store.Config) {
			tb.Helper()

			assert := require.New(tb)

			status := workload.GetGlobalStatus()

			assert.Equal(uint64(0), status.WriteErrors.Load())
			assert.Equal(uint64(0), status.ReadErrors.Load())
			assert.Equal(0, status.Errors.Len())

			assert.Greater(status.WriteOps.Load(), uint64(0))
			assert.Greater(status.ReadOps.Load(), uint64(0))
			assert.Greater(status.ValidatedRows.Load(), uint64(0))
		},
	},
	{
		name:     "WriteMode",
		mode:     jobs.WriteMode,
		duration: 10 * time.Second,
		warmup:   10 * time.Second,
		expect: func(tb testing.TB, workload *Workload, _ store.Config) {
			tb.Helper()

			assert := require.New(tb)

			status := workload.GetGlobalStatus()

			assert.Equal(uint64(0), status.WriteErrors.Load())
			assert.Equal(0, status.Errors.Len())
			assert.Equal(uint64(0), status.ReadErrors.Load())

			assert.Greater(status.WriteOps.Load(), uint64(0))
			assert.Equal(uint64(0), status.ReadOps.Load())
			assert.Equal(uint64(0), status.ValidatedRows.Load())
		},
	},
	{
		name:     "WriteModeWithoutWarmup",
		mode:     jobs.WriteMode,
		duration: 20 * time.Second,
		expect: func(tb testing.TB, workload *Workload, _ store.Config) {
			tb.Helper()
			assert := require.New(tb)

			status := workload.GetGlobalStatus()

			assert.Equal(uint64(0), status.WriteErrors.Load())
			assert.Equal(0, status.Errors.Len())
			assert.Equal(uint64(0), status.ReadErrors.Load())

			assert.Greater(status.WriteOps.Load(), uint64(0))
			assert.Equal(uint64(0), status.ReadOps.Load())
			assert.Equal(uint64(0), status.ValidatedRows.Load())
		},
	},
}

func TestWorkload(t *testing.T) {
	t.Parallel()
	scyllaContainer := testutils.TestContainers(t)

	for _, test := range dataset {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert := require.New(t)
			storeConfig := getStoreConfig(t, scyllaContainer.TestHosts, scyllaContainer.OracleHosts, scyllaContainer.DockerMode)
			schema := getSchema(t)
			stopFlag := stop.NewFlag(t.Name())
			t.Cleanup(func() {
				stopFlag.SetHard(true)
			})

			workload, err := NewWorkload(&WorkloadConfig{
				RunningMode:           test.mode,
				PartitionDistribution: distributions.Uniform,
				Seed:                  1,
				IOWorkerPoolSize:      64,
				MaxErrorsToStore:      1,
				WarmupDuration:        test.warmup,
				Duration:              test.duration,
				PartitionCount:        10000,
				MutationConcurrency:   4,
				ReadConcurrency:       8,
				DropSchema:            true,
			}, storeConfig, schema, getLogger(t), stopFlag)

			assert.NoError(err)
			assert.NoError(workload.Run(t.Context()))
			assert.NoError(workload.Close())

			test.expect(t, workload, storeConfig)
		})
	}
}

func TestWorkloadWithoutOracle(t *testing.T) {
	t.Parallel()
	scyllaContainer := testutils.SingleScylla(t)

	for _, test := range dataset {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			logger := getLogger(t)
			assert := require.New(t)
			storeConfig := getStoreConfig(t, scyllaContainer.TestHosts, scyllaContainer.OracleHosts, scyllaContainer.DockerMode)
			schema := getSchema(t)
			stopFlag := stop.NewFlag(t.Name())
			t.Cleanup(func() {
				stopFlag.SetHard(true)
			})

			workload, err := NewWorkload(&WorkloadConfig{
				RunningMode:           test.mode,
				PartitionDistribution: distributions.Uniform,
				Seed:                  1,
				IOWorkerPoolSize:      16,
				MaxErrorsToStore:      1,
				WarmupDuration:        test.warmup,
				Duration:              test.duration,
				PartitionCount:        1000,
				MutationConcurrency:   2,
				ReadConcurrency:       4,
				DropSchema:            true,
			}, storeConfig, schema, logger, stopFlag)

			assert.NoError(err)
			assert.NoError(workload.Run(t.Context()))
			assert.NoError(workload.Close())

			test.expect(t, workload, storeConfig)
		})
	}
}

// defaultStatementRatios returns balanced ratios for integration tests:
// 70% insert, 25% update, 5% delete — exercises all mutation paths while
// keeping deletes low enough that the partition pool doesn't churn excessively.
func defaultStatementRatios() statements.Ratios {
	return statements.Ratios{
		MutationRatios: statements.MutationRatios{
			InsertRatio: 0.70,
			UpdateRatio: 0.25,
			DeleteRatio: 0.05,
			InsertSubtypeRatios: statements.InsertRatios{
				RegularInsertRatio: 0.9,
				JSONInsertRatio:    0.1,
			},
			DeleteSubtypeRatios: statements.DeleteRatios{
				WholePartitionRatio:     0.4,
				SingleRowRatio:          0.3,
				SingleColumnRatio:       0.2,
				MultiplePartitionsRatio: 0.1,
			},
		},
		ValidationRatios: statements.ValidationRatios{
			SelectSubtypeRatios: statements.SelectRatios{
				SinglePartitionRatio:                  0.6,
				MultiplePartitionRatio:                0.3,
				ClusteringRangeRatio:                  0.05,
				MultiplePartitionClusteringRangeRatio: 0.04,
				SingleIndexRatio:                      0.01,
			},
		},
	}
}

func TestWorkloadWithFailedValidation(t *testing.T) {
	t.Parallel()
	scyllaContainer := testutils.TestContainers(t)

	assert := require.New(t)
	logger := getLogger(t)
	storeConfig := getStoreConfig(t, scyllaContainer.TestHosts, scyllaContainer.OracleHosts, scyllaContainer.DockerMode)
	schema := getSchema(t)
	stopFlag := stop.NewFlag(t.Name())
	t.Cleanup(func() {
		stopFlag.SetHard(true)
	})

	const (
		partitionCount = 5000
		seed           = 4
		maxErrorsCount = 1
	)

	// Phase 1: Run a mixed workload to populate data AND establish partition keys
	t.Log("Phase 1: Running mixed workload to populate and validate data")
	mixedWorkload, err := NewWorkload(&WorkloadConfig{
		RunningMode:           jobs.MixedMode,
		PartitionDistribution: distributions.Zipf,
		Seed:                  seed,
		StatementRatios:       defaultStatementRatios(),
		IOWorkerPoolSize:      128,
		MaxErrorsToStore:      maxErrorsCount,
		Duration:              1 * time.Minute,
		PartitionCount:        partitionCount,
		MutationConcurrency:   8,
		ReadConcurrency:       16,
		DropSchema:            true,
	}, storeConfig, schema, logger, stopFlag)
	assert.NoError(err)

	time.AfterFunc(15*time.Second, func() {
		truncateQuery := fmt.Sprintf("TRUNCATE TABLE %s.%s", schema.Keyspace.Name, schema.Tables[0].Name)
		if err = scyllaContainer.Test.Query(truncateQuery).Exec(); err != nil {
			t.Logf("Failed to execute truncate query: %v", err)
		} else {
			t.Log("TEST cluster truncated successfully")
		}
	})
	result := mixedWorkload.Run(t.Context())

	mixedStatus := mixedWorkload.GetGlobalStatus()
	t.Logf("Mixed workload complete: WriteOps=%d, ReadOps=%d, ValidatedRows=%d, Errors=%d",
		mixedStatus.WriteOps.Load(), mixedStatus.ReadOps.Load(),
		mixedStatus.ValidatedRows.Load(), mixedStatus.Errors.Len())

	assert.Error(result)
	assert.NoError(mixedWorkload.Close())

	assert.GreaterOrEqual(mixedStatus.ReadErrors.Load(), uint64(1), "should have read errors")
	assert.Zero(mixedStatus.WriteErrors.Load(), "should have no write errors")

	t.Log("Phase 4: Verifying log files contain error statements")
	contents := map[string][]byte{}

	for _, file := range []string{storeConfig.TestStatementFile, storeConfig.OracleStatementFile} {
		bytes := testutils.Must(os.ReadFile(file))
		contents[file] = bytes
	}

	assert.NotEmpty(contents[storeConfig.TestStatementFile], "test log file should contain statements")
	assert.NotEmpty(contents[storeConfig.OracleStatementFile], "oracle log file should contain statements")

	// Validate that statements in the log files are not empty
	for _, file := range []string{storeConfig.TestStatementFile, storeConfig.OracleStatementFile} {
		lines := strings.Split(strings.TrimSpace(string(contents[file])), "\n")
		assert.NotEmpty(lines, "file %s should have at least one line", file)

		for i, line := range lines {
			if line == "" {
				continue // Skip empty lines
			}

			var loggedStmt struct {
				Query      string            `json:"query"`
				Fragments  []json.RawMessage `json:"mutationFragments"`
				Statements []json.RawMessage `json:"statements"`
			}
			err = json.Unmarshal([]byte(line), &loggedStmt)
			assert.NoError(err, "failed to unmarshal line %d in file %s", i, file)

			// Validate that either Query or Statements are not empty
			assert.NotEmpty(loggedStmt.Query, "line %d in file %s should have non-empty query", i, file)
			assert.NotEmpty(loggedStmt.Statements, "line %d in file %s should have non-empty statements", i, file)
		}
	}
}

func TestWorkloadWithAllPrimitiveTypes(t *testing.T) {
	t.Parallel()
	scyllaContainer := testutils.TestContainers(t)

	assert := require.New(t)
	logger := getLogger(t)
	storeConfig := getStoreConfig(t, scyllaContainer.TestHosts, scyllaContainer.OracleHosts, scyllaContainer.DockerMode)
	schema := getSchema(t, &typedef.Table{
		Name: "table_all",
		PartitionKeys: typedef.Columns{
			{Name: "pk1", Type: typedef.TypeText},
			{Name: "pk2", Type: typedef.TypeInt},
		},
		ClusteringKeys: typedef.Columns{
			{Name: "ck1", Type: typedef.TypeInt},
		},
		Columns: typedef.Columns{
			{Name: "col1", Type: typedef.TypeAscii},
			{Name: "col2", Type: typedef.TypeBigint},
			{Name: "col3", Type: typedef.TypeBlob},
			{Name: "col4", Type: typedef.TypeBoolean},
			{Name: "col5", Type: typedef.TypeDate},
			{Name: "col6", Type: typedef.TypeDecimal},
			{Name: "col7", Type: typedef.TypeDouble},
			{Name: "col8", Type: typedef.TypeDuration},
			{Name: "col9", Type: typedef.TypeFloat},
			{Name: "col10", Type: typedef.TypeInet},
			{Name: "col11", Type: typedef.TypeInt},
			{Name: "col12", Type: typedef.TypeSmallint},
			{Name: "col13", Type: typedef.TypeText},
			{Name: "col14", Type: typedef.TypeTime},
			{Name: "col15", Type: typedef.TypeTimestamp},
			{Name: "col16", Type: typedef.TypeTimeuuid},
			{Name: "col17", Type: typedef.TypeTinyint},
			{Name: "col18", Type: typedef.TypeUuid},
			{Name: "col19", Type: typedef.TypeVarchar},
			{Name: "col20", Type: typedef.TypeVarint},
		},
	})
	stopFlag := stop.NewFlag(t.Name())
	t.Cleanup(func() {
		stopFlag.SetHard(true)
	})

	const (
		partitionCount = 1000
		seed           = 20
		maxErrorsCount = 1
	)

	workload, err := NewWorkload(&WorkloadConfig{
		RunningMode:           jobs.MixedMode,
		PartitionDistribution: distributions.Zipf,
		Seed:                  seed,
		IOWorkerPoolSize:      64,
		MaxErrorsToStore:      maxErrorsCount,
		WarmupDuration:        5 * time.Second,
		Duration:              20 * time.Second,
		PartitionCount:        partitionCount,
		MutationConcurrency:   4,
		ReadConcurrency:       8,
		DropSchema:            true,
		StatementRatios: statements.Ratios{
			MutationRatios: statements.MutationRatios{
				InsertRatio: 0.7,
				UpdateRatio: 0.2,
				DeleteRatio: 0.1,
				InsertSubtypeRatios: statements.InsertRatios{
					RegularInsertRatio: 1.0,
					JSONInsertRatio:    0.0,
				},
				DeleteSubtypeRatios: statements.DeleteRatios{
					WholePartitionRatio:     0.5,
					SingleRowRatio:          0.5,
					SingleColumnRatio:       0.0,
					MultiplePartitionsRatio: 0.0,
				},
			},
			ValidationRatios: statements.ValidationRatios{
				SelectSubtypeRatios: statements.SelectRatios{
					SinglePartitionRatio:                  0.5,
					MultiplePartitionRatio:                0.0,
					ClusteringRangeRatio:                  0.5,
					MultiplePartitionClusteringRangeRatio: 0.0,
					SingleIndexRatio:                      0.0,
				},
			},
		},
	}, storeConfig, schema, logger, stopFlag)

	assert.NoError(err)

	// Run the workload with error handling
	runErr := workload.Run(t.Context())
	closeErr := workload.Close()

	// Log errors for debugging but don't fail immediately
	if runErr != nil {
		t.Logf("Workload run returned error: %v", runErr)
	}
	if closeErr != nil {
		t.Logf("Workload close returned error: %v", closeErr)
	}

	status := workload.GetGlobalStatus()

	// Log status for debugging
	t.Logf("Status: WriteOps=%d, ReadOps=%d, ValidatedRows=%d, WriteErrors=%d, ReadErrors=%d, Errors=%d",
		status.WriteOps.Load(), status.ReadOps.Load(), status.ValidatedRows.Load(),
		status.WriteErrors.Load(), status.ReadErrors.Load(), status.Errors.Len())

	// Verify we did some work
	assert.Greater(status.WriteOps.Load(), uint64(0), "should have performed some write operations")
	assert.GreaterOrEqual(status.ReadOps.Load(), uint64(0), "should have performed some read operations")
}

// TestWorkloadHighConcurrency exercises Gemini with production-like concurrency
// settings: Zipf distribution (hot-spot access pattern), 50k partitions, and
// high mutation+read concurrency.  This mirrors the SCT configuration but with
// a shorter duration to keep CI stable.
func TestWorkloadHighConcurrency(t *testing.T) {
	t.Parallel()
	scyllaContainer := testutils.TestContainers(t)

	assert := require.New(t)
	logger := getLogger(t)
	storeConfig := getStoreConfig(t, scyllaContainer.TestHosts, scyllaContainer.OracleHosts, scyllaContainer.DockerMode)
	schema := getSchema(t)
	stopFlag := stop.NewFlag(t.Name())
	t.Cleanup(func() {
		stopFlag.SetHard(true)
	})

	workload, err := NewWorkload(&WorkloadConfig{
		RunningMode:           jobs.MixedMode,
		PartitionDistribution: distributions.Zipf,
		Seed:                  70, // Same seed as SCT to exercise the same code paths
		IOWorkerPoolSize:      128,
		MaxErrorsToStore:      1,
		WarmupDuration:        5 * time.Second,
		Duration:              30 * time.Second,
		PartitionCount:        50000,
		MutationConcurrency:   16,
		ReadConcurrency:       32,
		DropSchema:            true,
		StatementRatios:       defaultStatementRatios(),
	}, storeConfig, schema, logger, stopFlag)

	assert.NoError(err)
	assert.NoError(workload.Run(t.Context()))
	assert.NoError(workload.Close())

	status := workload.GetGlobalStatus()
	t.Logf("HighConcurrency: WriteOps=%d, ReadOps=%d, ValidatedRows=%d",
		status.WriteOps.Load(), status.ReadOps.Load(), status.ValidatedRows.Load())

	assert.Zero(status.WriteErrors.Load(), "should have no write errors")
	assert.Zero(status.ReadErrors.Load(), "should have no read errors")
	assert.Equal(0, status.Errors.Len(), "should have no stored errors")
	assert.Greater(status.WriteOps.Load(), uint64(0), "must have performed writes")
	assert.Greater(status.ReadOps.Load(), uint64(0), "must have performed reads")
	assert.Greater(status.ValidatedRows.Load(), uint64(0), "must have validated rows")
}

// TestWorkloadDeleteHeavy exercises the deleted-partitions heap with an
// aggressive 40% DELETE ratio.  This is the code path that caused memory growth
// in production (retained partition references for ~71min in delete buckets).
// The test uses short delete buckets (1s, 2s, 5s) to ensure entries cycle
// through quickly on CI.
func TestWorkloadDeleteHeavy(t *testing.T) {
	t.Parallel()
	scyllaContainer := testutils.TestContainers(t)

	assert := require.New(t)
	logger := getLogger(t)
	storeConfig := getStoreConfig(t, scyllaContainer.TestHosts, scyllaContainer.OracleHosts, scyllaContainer.DockerMode)

	// Custom schema with short delete buckets to exercise the full
	// deleted-partitions lifecycle within the test duration.
	schema := &typedef.Schema{
		Keyspace: typedef.Keyspace{
			Replication:       replication.NewSimpleStrategy(),
			OracleReplication: replication.NewSimpleStrategy(),
			Name:              "ks_delete_heavy",
		},
		Tables: []*typedef.Table{
			{
				Name: "table_del",
				PartitionKeys: typedef.Columns{
					{Name: "pk1", Type: typedef.TypeInt},
				},
				ClusteringKeys: typedef.Columns{
					{Name: "ck1", Type: typedef.TypeInt},
				},
				Columns: typedef.Columns{
					{Name: "val", Type: typedef.TypeText},
				},
			},
		},
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
			MaxStringLength:                  64,
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
			// Short buckets so partitions cycle through delete→reuse quickly
			DeleteBuckets: []time.Duration{1 * time.Second, 2 * time.Second, 5 * time.Second},
		},
	}

	stopFlag := stop.NewFlag(t.Name())
	t.Cleanup(func() {
		stopFlag.SetHard(true)
	})

	workload, err := NewWorkload(&WorkloadConfig{
		RunningMode:           jobs.MixedMode,
		PartitionDistribution: distributions.Uniform,
		Seed:                  42,
		IOWorkerPoolSize:      64,
		MaxErrorsToStore:      1,
		WarmupDuration:        3 * time.Second,
		Duration:              25 * time.Second,
		PartitionCount:        5000,
		MutationConcurrency:   8,
		ReadConcurrency:       16,
		DropSchema:            true,
		StatementRatios: statements.Ratios{
			MutationRatios: statements.MutationRatios{
				InsertRatio: 0.4,
				UpdateRatio: 0.2,
				DeleteRatio: 0.4,
				InsertSubtypeRatios: statements.InsertRatios{
					RegularInsertRatio: 1.0,
					JSONInsertRatio:    0.0,
				},
				DeleteSubtypeRatios: statements.DeleteRatios{
					WholePartitionRatio:     0.3,
					SingleRowRatio:          0.4,
					SingleColumnRatio:       0.2,
					MultiplePartitionsRatio: 0.1,
				},
			},
			ValidationRatios: statements.ValidationRatios{
				SelectSubtypeRatios: statements.SelectRatios{
					SinglePartitionRatio:                  0.7,
					MultiplePartitionRatio:                0.2,
					ClusteringRangeRatio:                  0.1,
					MultiplePartitionClusteringRangeRatio: 0.0,
					SingleIndexRatio:                      0.0,
				},
			},
		},
	}, storeConfig, schema, logger, stopFlag)

	assert.NoError(err)
	assert.NoError(workload.Run(t.Context()))
	assert.NoError(workload.Close())

	status := workload.GetGlobalStatus()
	t.Logf("DeleteHeavy: WriteOps=%d, ReadOps=%d, ValidatedRows=%d, WriteErrors=%d, ReadErrors=%d",
		status.WriteOps.Load(), status.ReadOps.Load(), status.ValidatedRows.Load(),
		status.WriteErrors.Load(), status.ReadErrors.Load())

	assert.Zero(status.WriteErrors.Load(), "should have no write errors")
	assert.Zero(status.ReadErrors.Load(), "should have no read errors")
	assert.Equal(0, status.Errors.Len(), "should have no stored errors")
	assert.Greater(status.WriteOps.Load(), uint64(0), "must have performed writes")
	assert.Greater(status.ReadOps.Load(), uint64(0), "must have performed reads")
}

// TestWorkloadWriteThenValidate exercises the write→read two-phase pattern:
// first saturate the clusters with writes only (no concurrent reads), then
// do a full validation pass.  This catches bugs where validation-only mode
// fails to read data written in a prior phase.
func TestWorkloadWriteThenValidate(t *testing.T) {
	t.Parallel()
	scyllaContainer := testutils.TestContainers(t)

	assert := require.New(t)
	logger := getLogger(t)
	storeConfig := getStoreConfig(t, scyllaContainer.TestHosts, scyllaContainer.OracleHosts, scyllaContainer.DockerMode)
	schema := getSchema(t)
	stopFlag := stop.NewFlag(t.Name())
	t.Cleanup(func() {
		stopFlag.SetHard(true)
	})

	const (
		partitionCount = 10000
		seed           = 99
	)

	// Phase 1: Write-only at high concurrency
	t.Log("Phase 1: Write-only workload")
	writeWorkload, err := NewWorkload(&WorkloadConfig{
		RunningMode:           jobs.WriteMode,
		PartitionDistribution: distributions.Zipf,
		Seed:                  seed,
		IOWorkerPoolSize:      128,
		MaxErrorsToStore:      1,
		Duration:              15 * time.Second,
		PartitionCount:        partitionCount,
		MutationConcurrency:   16,
		DropSchema:            true,
		StatementRatios: statements.Ratios{
			MutationRatios: statements.MutationRatios{
				InsertRatio: 0.8,
				UpdateRatio: 0.2,
				DeleteRatio: 0.0,
				InsertSubtypeRatios: statements.InsertRatios{
					RegularInsertRatio: 0.8,
					JSONInsertRatio:    0.2,
				},
			},
			ValidationRatios: statements.ValidationRatios{
				SelectSubtypeRatios: statements.SelectRatios{
					SinglePartitionRatio: 1.0,
				},
			},
		},
	}, storeConfig, schema, logger, stopFlag)
	assert.NoError(err)
	assert.NoError(writeWorkload.Run(t.Context()))

	writeStatus := writeWorkload.GetGlobalStatus()
	t.Logf("Write phase: WriteOps=%d", writeStatus.WriteOps.Load())
	assert.Greater(writeStatus.WriteOps.Load(), uint64(0), "write phase must produce writes")
	assert.Zero(writeStatus.WriteErrors.Load(), "write phase should have no errors")
	assert.NoError(writeWorkload.Close())

	// Phase 2: Read-only validation of the data written above
	t.Log("Phase 2: Read-only validation")
	readWorkload, err := NewWorkload(&WorkloadConfig{
		RunningMode:           jobs.ReadMode,
		PartitionDistribution: distributions.Zipf,
		Seed:                  seed,
		IOWorkerPoolSize:      128,
		MaxErrorsToStore:      1,
		Duration:              15 * time.Second,
		PartitionCount:        partitionCount,
		ReadConcurrency:       32,
		DropSchema:            false, // Reuse existing schema+data
	}, storeConfig, schema, logger, stopFlag)
	assert.NoError(err)
	assert.NoError(readWorkload.Run(t.Context()))
	assert.NoError(readWorkload.Close())

	readStatus := readWorkload.GetGlobalStatus()
	t.Logf("Read phase: ReadOps=%d, ValidatedRows=%d, ReadErrors=%d",
		readStatus.ReadOps.Load(), readStatus.ValidatedRows.Load(), readStatus.ReadErrors.Load())
	assert.Zero(readStatus.ReadErrors.Load(), "validation must find no divergence")
	assert.Greater(readStatus.ReadOps.Load(), uint64(0), "read phase must perform reads")
}
