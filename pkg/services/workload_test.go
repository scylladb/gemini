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
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/jobs"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
)

func getStoreConfig(tb testing.TB, testHosts, oracleHosts []string) store.Config {
	tb.Helper()
	var oracleConfig *store.ScyllaClusterConfig

	if len(oracleHosts) > 0 {
		oracleConfig = &store.ScyllaClusterConfig{
			Name:                    stmtlogger.TypeOracle,
			HostSelectionPolicy:     store.HostSelectionTokenAware,
			Consistency:             gocql.Quorum.String(),
			Hosts:                   oracleHosts,
			RequestTimeout:          10 * time.Second,
			ConnectTimeout:          10 * time.Second,
			UseServerSideTimestamps: true,
		}
	}

	directory := tb.TempDir()

	return store.Config{
		OracleClusterConfig: oracleConfig,
		OracleStatementFile: filepath.Join(directory, "oracle_statements.jsonl"),
		TestStatementFile:   filepath.Join(directory, "test_statements.jsonl"),
		TestClusterConfig: store.ScyllaClusterConfig{
			Name:                    stmtlogger.TypeTest,
			HostSelectionPolicy:     store.HostSelectionTokenAware,
			Consistency:             gocql.Quorum.String(),
			Hosts:                   testHosts,
			RequestTimeout:          10 * time.Second,
			ConnectTimeout:          10 * time.Second,
			UseServerSideTimestamps: false,
		},
		MaxRetriesMutate:        1,
		MaxRetriesMutateSleep:   10 * time.Second,
		Compression:             stmtlogger.CompressionNone,
		UseServerSideTimestamps: true,
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
			MaxTables:                        1,
			MaxPartitionKeys:                 2,
			MinPartitionKeys:                 2,
			MaxClusteringKeys:                0,
			MinClusteringKeys:                1,
			MaxColumns:                       1,
			MinColumns:                       1,
			MaxBlobLength:                    32,
			MaxStringLength:                  32,
			CQLFeature:                       typedef.CQLFeatureNormal,
			AsyncObjectStabilizationAttempts: 10,
			AsyncObjectStabilizationDelay:    10 * time.Millisecond,
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
		expect: func(tb testing.TB, workload *Workload, storeConfig store.Config) {
			tb.Helper()

			assert := require.New(tb)

			assert.NoFileExists(storeConfig.OracleStatementFile)
			assert.NoFileExists(storeConfig.TestStatementFile)

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
		expect: func(tb testing.TB, workload *Workload, config store.Config) {
			tb.Helper()

			assert := require.New(tb)

			assert.NoFileExists(config.OracleStatementFile)
			assert.NoFileExists(config.TestStatementFile)

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
		expect: func(tb testing.TB, workload *Workload, config store.Config) {
			tb.Helper()

			assert := require.New(tb)

			assert.NoFileExists(config.OracleStatementFile)
			assert.NoFileExists(config.TestStatementFile)

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
		expect: func(tb testing.TB, workload *Workload, config store.Config) {
			tb.Helper()
			assert := require.New(tb)

			assert.NoFileExists(config.OracleStatementFile)
			assert.NoFileExists(config.TestStatementFile)

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
	logger := getLogger(t)
	scyllaContainer := testutils.TestContainers(t)

	for _, test := range dataset {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert := require.New(t)
			storeConfig := getStoreConfig(t, scyllaContainer.TestHosts, scyllaContainer.OracleHosts)
			schema := getSchema(t)
			stopFlag := stop.NewFlag(t.Name())
			t.Cleanup(func() {
				stopFlag.SetHard(true)
			})

			workload, err := NewWorkload(&WorkloadConfig{
				RunningMode:           test.mode,
				PartitionDistribution: distributions.DistributionUniform,
				Seed:                  1,
				PartitionBufferSize:   10,
				IOWorkerPoolSize:      2,
				MaxErrorsToStore:      1,
				WarmupDuration:        test.warmup,
				Duration:              test.duration,
				PartitionCount:        100,
				MutationConcurrency:   1,
				ReadConcurrency:       1,
				DropSchema:            true,
			}, storeConfig, schema, logger, stopFlag)

			assert.NoError(err)
			assert.NoError(workload.Run(t.Context()))
			assert.NoError(workload.Close())

			test.expect(t, workload, storeConfig)
		})
	}
}

func TestWorkloadWithoutOracle(t *testing.T) {
	t.Parallel()
	logger := getLogger(t)
	scyllaContainer := testutils.SingleScylla(t)

	for _, test := range dataset {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			assert := require.New(t)
			storeConfig := getStoreConfig(t, scyllaContainer.TestHosts, scyllaContainer.OracleHosts)
			schema := getSchema(t)
			stopFlag := stop.NewFlag(t.Name())
			t.Cleanup(func() {
				stopFlag.SetHard(true)
			})

			workload, err := NewWorkload(&WorkloadConfig{
				RunningMode:           test.mode,
				PartitionDistribution: distributions.DistributionUniform,
				Seed:                  1,
				PartitionBufferSize:   10,
				IOWorkerPoolSize:      2,
				MaxErrorsToStore:      1,
				WarmupDuration:        test.warmup,
				Duration:              test.duration,
				PartitionCount:        100,
				MutationConcurrency:   1,
				ReadConcurrency:       1,
				DropSchema:            true,
			}, storeConfig, schema, logger, stopFlag)

			assert.NoError(err)
			assert.NoError(workload.Run(t.Context()))
			assert.NoError(workload.Close())

			test.expect(t, workload, storeConfig)
		})
	}
}

func TestWorkloadWithFailedValidation(t *testing.T) {
	t.Parallel()
	scyllaContainer := testutils.TestContainers(t)

	assert := require.New(t)
	logger := getLogger(t)
	storeConfig := getStoreConfig(t, scyllaContainer.TestHosts, scyllaContainer.OracleHosts)
	schema := getSchema(t)
	stopFlag := stop.NewFlag(t.Name())
	t.Cleanup(func() {
		stopFlag.SetHard(true)
	})

	const (
		partitionCount      = 100
		partitionBufferSize = 10
		seed                = 1
		maxErrorsCount      = 4
	)

	random, distFunc := distributions.New(distributions.DistributionUniform, partitionCount, seed, 0, 0)
	gens := generators.New(schema, distFunc, seed, partitionCount, partitionBufferSize, logger, random)

	values := make([]typedef.PartitionKeys, 0, maxErrorsCount+1)

	for range maxErrorsCount + 1 {
		pk, err := gens.Get(schema.Tables[0]).Get(t.Context())
		assert.NoError(err)
		values = append(values, pk)
	}

	// Delete some data to cause validation errors from the TEST cluster
	time.AfterFunc(4*time.Second, func() {
		for _, value := range values {
			assert.NoError(scyllaContainer.Test.Query(
				fmt.Sprintf(
					"DELETE FROM %s.%s WHERE pk1 = ? AND pk2 = ?",
					schema.Keyspace.Name,
					schema.Tables[0].Name,
				),
				value.Values.ToCQLValues(schema.Tables[0].PartitionKeys)...,
			).Exec())
		}
	})

	workload, err := NewWorkload(&WorkloadConfig{
		RunningMode:           jobs.MixedMode,
		PartitionDistribution: distributions.DistributionUniform,
		Seed:                  seed,
		PartitionBufferSize:   partitionBufferSize,
		IOWorkerPoolSize:      2,
		MaxErrorsToStore:      maxErrorsCount,
		WarmupDuration:        5 * time.Second,
		Duration:              10 * time.Second,
		PartitionCount:        partitionCount,
		MutationConcurrency:   1,
		ReadConcurrency:       1,
		DropSchema:            true,
	}, storeConfig, schema, logger, stopFlag)

	assert.NoError(err)
	assert.Error(workload.Run(t.Context()))
	assert.NoError(workload.Close())

	assert.FileExists(storeConfig.OracleStatementFile)
	assert.FileExists(storeConfig.TestStatementFile)

	status := workload.GetGlobalStatus()

	assert.Equal(maxErrorsCount, status.Errors.Len())
	assert.Equal(uint64(maxErrorsCount), status.ReadErrors.Load(), "there were validation errors")

	assert.Equalf(uint64(0), status.WriteErrors.Load(), "there were write errors")

	assert.Greater(status.WriteOps.Load(), uint64(0))
	assert.Greater(status.ReadOps.Load(), uint64(0))
	assert.Greater(status.ValidatedRows.Load(), uint64(0))

	contents := map[string][]stmtlogger.Item{}

	for _, file := range []string{storeConfig.TestStatementFile, storeConfig.OracleStatementFile} {
		var handle *os.File
		handle, err = os.Open(file)
		assert.NoError(err)

		data := make([]stmtlogger.Item, 0, 1000)

		var stats os.FileInfo
		stats, err = handle.Stat()
		assert.NoError(err)
		assert.Greater(stats.Size(), int64(1))

		decoder := json.NewDecoder(handle)

		var item stmtlogger.Item
		for err = decoder.Decode(&item); err == nil; err = decoder.Decode(&item) {
			data = append(data, item)
		}

		slices.SortStableFunc(data, func(a, b stmtlogger.Item) int {
			return a.Start.Time.Compare(b.Start.Time)
		})

		contents[file] = data
	}

	assert.Equal(len(contents[storeConfig.TestStatementFile]), len(contents[storeConfig.OracleStatementFile]), "test and oracle files should have the same number of items")
}

func TestWorkloadWithAllSchemaTypes(t *testing.T) {
	t.Parallel()
	scyllaContainer := testutils.TestContainers(t)

	assert := require.New(t)
	logger := getLogger(t)
	storeConfig := getStoreConfig(t, scyllaContainer.TestHosts, scyllaContainer.OracleHosts)
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
		partitionCount      = 100
		partitionBufferSize = 10
		seed                = 1
		maxErrorsCount      = 4
	)

	workload, err := NewWorkload(&WorkloadConfig{
		RunningMode:           jobs.MixedMode,
		PartitionDistribution: distributions.DistributionUniform,
		Seed:                  seed,
		PartitionBufferSize:   partitionBufferSize,
		IOWorkerPoolSize:      2,
		MaxErrorsToStore:      maxErrorsCount,
		WarmupDuration:        5 * time.Second,
		Duration:              10 * time.Second,
		PartitionCount:        partitionCount,
		MutationConcurrency:   1,
		ReadConcurrency:       1,
		DropSchema:            true,
	}, storeConfig, schema, logger, stopFlag)

	assert.NoError(err)
	assert.NoError(workload.Run(t.Context()))
	assert.NoError(workload.Close())

	assert.NoFileExists(storeConfig.OracleStatementFile)
	assert.NoFileExists(storeConfig.TestStatementFile)

	status := workload.GetGlobalStatus()

	assert.Equal(uint64(0), status.WriteErrors.Load())
	assert.Equal(0, status.Errors.Len())
	assert.Equal(uint64(0), status.ReadErrors.Load())

	assert.Greater(status.WriteOps.Load(), uint64(0))
	assert.Greater(status.ReadOps.Load(), uint64(0))
	assert.Greater(status.ValidatedRows.Load(), uint64(0))
}
