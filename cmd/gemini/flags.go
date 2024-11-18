// Copyright 2019 ScyllaDB
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

package main

import (
	"time"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/jobs"
)

var (
	testClusterHost                  []string
	testClusterUsername              string
	testClusterPassword              string
	oracleClusterHost                []string
	oracleClusterUsername            string
	oracleClusterPassword            string
	schemaFile                       string
	outFileArg                       string
	concurrency                      uint64
	seed                             string
	schemaSeed                       string
	dropSchema                       bool
	verbose                          bool
	mode                             string
	failFast                         bool
	nonInteractive                   bool
	duration                         time.Duration
	bind                             string
	warmup                           time.Duration
	replicationStrategy              string
	tableOptions                     []string
	oracleReplicationStrategy        string
	consistency                      string
	maxTables                        int
	maxPartitionKeys                 int
	minPartitionKeys                 int
	maxClusteringKeys                int
	minClusteringKeys                int
	maxColumns                       int
	minColumns                       int
	datasetSize                      string
	cqlFeatures                      string
	useMaterializedViews             bool
	level                            string
	maxRetriesMutate                 int
	maxRetriesMutateSleep            time.Duration
	maxErrorsToStore                 int
	pkBufferReuseSize                uint64
	partitionCount                   uint64
	partitionKeyDistribution         string
	normalDistMean                   float64
	normalDistSigma                  float64
	tracingOutFile                   string
	useCounters                      bool
	asyncObjectStabilizationAttempts int
	asyncObjectStabilizationDelay    time.Duration
	useLWT                           bool
	testClusterHostSelectionPolicy   string
	oracleClusterHostSelectionPolicy string
	useServerSideTimestamps          bool
	requestTimeout                   time.Duration
	connectTimeout                   time.Duration
	profilingPort                    int
	testStatementLogFile             string
	oracleStatementLogFile           string
)

func init() {
	rootCmd.Version = version + ", commit " + commit + ", date " + date
	rootCmd.Flags().StringSliceVarP(&testClusterHost, "test-cluster", "t", []string{}, "Host names or IPs of the test cluster that is system under test")
	_ = rootCmd.MarkFlagRequired("test-cluster")
	rootCmd.Flags().StringVarP(&testClusterUsername, "test-username", "", "", "Username for the test cluster")
	rootCmd.Flags().StringVarP(&testClusterPassword, "test-password", "", "", "Password for the test cluster")
	rootCmd.Flags().StringSliceVarP(
		&oracleClusterHost, "oracle-cluster", "o", []string{},
		"Host names or IPs of the oracle cluster that provides correct answers. If omitted no oracle will be used")
	rootCmd.Flags().StringVarP(&oracleClusterUsername, "oracle-username", "", "", "Username for the oracle cluster")
	rootCmd.Flags().StringVarP(&oracleClusterPassword, "oracle-password", "", "", "Password for the oracle cluster")
	rootCmd.Flags().StringVarP(&schemaFile, "schema", "", "", "Schema JSON config file")
	rootCmd.Flags().StringVarP(&mode, "mode", "m", jobs.MixedMode, "Query operation mode. Mode options: write, read, mixed (default)")
	rootCmd.Flags().Uint64VarP(&concurrency, "concurrency", "c", 10, "Number of threads per table to run concurrently")
	rootCmd.Flags().StringVarP(&seed, "seed", "s", "random", "Statement seed value")
	rootCmd.Flags().StringVarP(&schemaSeed, "schema-seed", "", "random", "Schema seed value")
	rootCmd.Flags().BoolVarP(&dropSchema, "drop-schema", "d", true, "Drop schema before starting tests run")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output during test run")
	rootCmd.Flags().BoolVarP(&failFast, "fail-fast", "f", false, "Stop on the first failure")
	rootCmd.Flags().BoolVarP(&nonInteractive, "non-interactive", "", false, "Run in non-interactive mode (disable progress indicator)")
	rootCmd.Flags().DurationVarP(&duration, "duration", "", 30*time.Second, "")
	rootCmd.Flags().StringVarP(&outFileArg, "outfile", "", "", "Specify the name of the file where the results should go")
	rootCmd.Flags().StringVarP(&bind, "bind", "b", ":2112", "Specify the interface and port which to bind prometheus metrics on. Default is ':2112'")
	rootCmd.Flags().DurationVarP(&warmup, "warmup", "", 30*time.Second, "Specify the warmup perid as a duration for example 30s or 10h")
	rootCmd.Flags().StringVarP(
		&replicationStrategy, "replication-strategy", "", "simple",
		"Specify the desired replication strategy as either the coded short hand simple|network to get the default for each type or provide "+
			"the entire specification in the form {'class':'....'}")
	rootCmd.Flags().StringVarP(
		&oracleReplicationStrategy, "oracle-replication-strategy", "", "simple",
		"Specify the desired replication strategy of the oracle cluster as either the coded short hand simple|network to get the default for each "+
			"type or provide the entire specification in the form {'class':'....'}")
	rootCmd.Flags().StringArrayVarP(&tableOptions, "table-options", "", []string{}, "Repeatable argument to set table options to be added to the created tables")
	rootCmd.Flags().StringVarP(&consistency, "consistency", "", "LOCAL_QUORUM", "Specify the desired consistency as ANY|ONE|TWO|THREE|QUORUM|LOCAL_QUORUM|EACH_QUORUM|LOCAL_ONE")
	rootCmd.Flags().IntVarP(&maxTables, "max-tables", "", 1, "Maximum number of generated tables")
	rootCmd.Flags().IntVarP(&maxPartitionKeys, "max-partition-keys", "", 6, "Maximum number of generated partition keys")
	rootCmd.Flags().IntVarP(&minPartitionKeys, "min-partition-keys", "", 2, "Minimum number of generated partition keys")
	rootCmd.Flags().IntVarP(&maxClusteringKeys, "max-clustering-keys", "", 4, "Maximum number of generated clustering keys")
	rootCmd.Flags().IntVarP(&minClusteringKeys, "min-clustering-keys", "", 2, "Minimum number of generated clustering keys")
	rootCmd.Flags().IntVarP(&maxColumns, "max-columns", "", 16, "Maximum number of generated columns")
	rootCmd.Flags().IntVarP(&minColumns, "min-columns", "", 8, "Minimum number of generated columns")
	rootCmd.Flags().StringVarP(&datasetSize, "dataset-size", "", "large", "Specify the type of dataset size to use, small|large")
	rootCmd.Flags().StringVarP(&cqlFeatures, "cql-features", "", "basic", "Specify the type of cql features to use, basic|normal|all")
	rootCmd.Flags().BoolVarP(&useMaterializedViews, "materialized-views", "", false, "Run gemini with materialized views support")
	rootCmd.Flags().StringVarP(&level, "level", "", "info", "Specify the logging level, debug|info|warn|error|dpanic|panic|fatal")
	rootCmd.Flags().IntVarP(&maxRetriesMutate, "max-mutation-retries", "", 10, "Maximum number of attempts to apply a mutation")
	rootCmd.Flags().DurationVarP(
		&maxRetriesMutateSleep, "max-mutation-retries-backoff", "", 10*time.Millisecond,
		"Duration between attempts to apply a mutation for example 10ms or 1s")
	rootCmd.Flags().Uint64VarP(&pkBufferReuseSize, "partition-key-buffer-reuse-size", "", 30000, "Number of reused buffered partition keys")
	rootCmd.Flags().Uint64VarP(&partitionCount, "token-range-slices", "", 10000, "Number of slices to divide the token space into")
	rootCmd.Flags().StringVarP(
		&partitionKeyDistribution, "partition-key-distribution", "", "uniform",
		"Specify the distribution from which to draw partition keys, supported values are currently uniform|normal|zipf")
	rootCmd.Flags().Float64VarP(&normalDistMean, "normal-dist-mean", "", generators.StdDistMean, "Mean of the normal distribution")
	rootCmd.Flags().Float64VarP(&normalDistSigma, "normal-dist-sigma", "", generators.OneStdDev, "Sigma of the normal distribution, defaults to one standard deviation ~0.341")
	rootCmd.Flags().StringVarP(
		&tracingOutFile, "tracing-outfile", "", "",
		"Specify the file to which tracing information gets written. Two magic names are available, 'stdout' and 'stderr'. By default tracing is disabled.")
	rootCmd.Flags().BoolVarP(&useCounters, "use-counters", "", false, "Ensure that at least one table is a counter table")
	rootCmd.Flags().IntVarP(
		&asyncObjectStabilizationAttempts, "async-objects-stabilization-attempts", "", 10,
		"Maximum number of attempts to validate result sets from MV and SI")
	rootCmd.Flags().DurationVarP(
		&asyncObjectStabilizationDelay, "async-objects-stabilization-backoff", "", 10*time.Millisecond,
		"Duration between attempts to validate result sets from MV and SI for example 10ms or 1s")
	rootCmd.Flags().BoolVarP(&useLWT, "use-lwt", "", false, "Emit LWT based updates")
	rootCmd.Flags().StringVarP(
		&oracleClusterHostSelectionPolicy, "oracle-host-selection-policy", "", "token-aware",
		"Host selection policy used by the driver for the oracle cluster: round-robin|host-pool|token-aware")
	rootCmd.Flags().StringVarP(
		&testClusterHostSelectionPolicy, "test-host-selection-policy", "", "token-aware",
		"Host selection policy used by the driver for the test cluster: round-robin|host-pool|token-aware")
	rootCmd.Flags().BoolVarP(&useServerSideTimestamps, "use-server-timestamps", "", false, "Use server-side generated timestamps for writes")
	rootCmd.Flags().DurationVarP(&requestTimeout, "request-timeout", "", 30*time.Second, "Duration of waiting request execution")
	rootCmd.Flags().DurationVarP(&connectTimeout, "connect-timeout", "", 30*time.Second, "Duration of waiting connection established")
	rootCmd.Flags().IntVarP(&profilingPort, "profiling-port", "", 0, "If non-zero starts pprof profiler on given port at 'http://0.0.0.0:<port>/profile'")
	rootCmd.Flags().IntVarP(&maxErrorsToStore, "max-errors-to-store", "", 1000, "Maximum number of errors to store and output at the end")
	rootCmd.Flags().StringVarP(&testStatementLogFile, "test-statement-log-file", "", "", "File to write statements flow to")
	rootCmd.Flags().StringVarP(&oracleStatementLogFile, "oracle-statement-log-file", "", "", "File to write statements flow to")
}
