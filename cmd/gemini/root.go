// Copyright 2019 ScyllaDB
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

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand/v2"
	"net/http"
	"net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/net/context"

	"github.com/scylladb/gemini/pkg/builders"
	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/jobs"
	"github.com/scylladb/gemini/pkg/realrandom"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/store/drivers"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

var (
	level         string
	profilingPort int
	mode          string
	warmup        time.Duration
	duration      time.Duration
	verbose       bool
	failFast      bool
	concurrency   uint64

	oracleConfig                drivers.CQLConfig
	testConfig                  drivers.CQLConfig
	statementLogFileCompression string
	testReplicationStrategy     string
	oracleReplicationStrategy   string

	schemaFile                       string
	outFileArg                       string
	seed                             string
	schemaSeed                       string
	dropSchema                       bool
	tableOptions                     []string
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
	maxErrorsToStore                 int
	pkBufferReuseSize                uint64
	partitionCount                   uint64
	partitionKeyDistribution         string
	normalDistMean                   float64
	normalDistSigma                  float64
	useCounters                      bool
	asyncObjectStabilizationAttempts int
	asyncObjectStabilizationDelay    time.Duration
	useLWT                           bool
)

func readSchema(confFile string, schemaConfig typedef.SchemaConfig) (*typedef.Schema, error) {
	byteValue, err := os.ReadFile(confFile)
	if err != nil {
		return nil, err
	}

	var shm typedef.Schema

	err = json.Unmarshal(byteValue, &shm)
	if err != nil {
		return nil, err
	}

	schemaBuilder := builders.NewSchemaBuilder()
	schemaBuilder.Keyspace(shm.Keyspace).Config(schemaConfig)
	for t, tbl := range shm.Tables {
		shm.Tables[t].LinkIndexAndColumns()
		schemaBuilder.Table(tbl)
	}
	return schemaBuilder.Build(), nil
}

func run(cmd *cobra.Command, _ []string) error {
	baseCtx, cancel := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGABRT)
	defer cancel()

	logger := createLogger(level)
	globalStatus := status.NewGlobalStatus(1000)
	defer utils.IgnoreError(logger.Sync)

	if err := validateSeed(seed); err != nil {
		return errors.Wrapf(err, "failed to parse --seed argument")
	}
	if err := validateSeed(schemaSeed); err != nil {
		return errors.Wrapf(err, "failed to parse --schema-seed argument")
	}

	intSeed := seedFromString(seed)
	intSchemaSeed := seedFromString(schemaSeed)

	if profilingPort != 0 {
		go func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/profile", pprof.Profile)
			log.Fatal(http.ListenAndServe(":"+strconv.Itoa(profilingPort), mux))
		}()
	}

	outFile, err := utils.CreateFile(outFileArg, os.Stdout)
	if err != nil {
		return err
	}

	schemaConfig := createSchemaConfig(logger)
	if err = schemaConfig.Valid(); err != nil {
		return errors.Wrap(err, "invalid schema configuration")
	}
	var schema *typedef.Schema
	if len(schemaFile) > 0 {
		schema, err = readSchema(schemaFile, schemaConfig)
		if err != nil {
			return errors.Wrap(err, "cannot create schema")
		}
	} else {
		schema, intSchemaSeed, err = generateSchema(logger, schemaConfig, schemaSeed)
		if err != nil {
			return errors.Wrapf(err, "failed to create schema for seed %s", schemaSeed)
		}
	}

	jsonSchema, _ := json.MarshalIndent(schema, "", "    ")

	printSetup(intSeed, intSchemaSeed)
	fmt.Printf("Schema: %v\n", string(jsonSchema))

	var oracle store.Driver
	if len(oracleConfig.Hosts) > 0 {
		oracle, err = drivers.NewCQL(baseCtx, "oracle", schema, oracleConfig, logger.Named("oracle_store"), statementLogFileCompression)
		if err != nil {
			return errors.Wrap(err, "failed to create oracle store")
		}

		defer func() {
			if closer, ok := oracle.(io.Closer); ok {
				utils.IgnoreError(closer.Close)
			}
		}()

	} else {
		oracle = drivers.NewNop()
	}

	test, err := drivers.NewCQL(baseCtx, "test", schema, testConfig, logger.Named("test_store"), statementLogFileCompression)
	if err != nil {
		return errors.Wrap(err, "failed to create oracle store")
	}
	defer utils.IgnoreError(test.Close)

	st, err := store.New(logger, test, oracle)
	if err != nil {
		return errors.Wrap(err, "failed to create store")
	}

	if dropSchema && mode != jobs.ReadMode {
		for _, stmt := range generators.GetDropKeyspace(schema) {
			logger.Debug(stmt)
			if err = st.Mutate(baseCtx, typedef.SimpleStmt(stmt, typedef.DropKeyspaceStatementType)); err != nil {
				return errors.Wrap(err, "unable to drop schema")
			}
		}
	}

	testKeyspace, oracleKeyspace := generators.GetCreateKeyspaces(schema)
	if err = st.Create(
		baseCtx,
		typedef.SimpleStmt(testKeyspace, typedef.CreateKeyspaceStatementType),
		typedef.SimpleStmt(oracleKeyspace, typedef.CreateKeyspaceStatementType)); err != nil {
		return errors.Wrap(err, "unable to create keyspace")
	}

	for _, stmt := range generators.GetCreateSchema(schema) {
		logger.Debug(stmt)
		if err = st.Mutate(baseCtx, typedef.SimpleStmt(stmt, typedef.CreateSchemaStatementType)); err != nil {
			return errors.Wrap(err, "unable to create schema")
		}
	}

	baseCtx, done := context.WithTimeout(baseCtx, duration+warmup+time.Second*2)
	defer done()
	stopFlag := stop.NewFlag("main")
	warmupStopFlag := stop.NewFlag("warmup")
	stop.StartOsSignalsTransmitter(logger, stopFlag, warmupStopFlag)
	// pump := jobs.NewPump(stopFlag, logger)

	distFunc, err := distributions.New(partitionKeyDistribution, partitionCount, intSeed, stdDistMean, oneStdDev)
	if err != nil {
		return errors.Wrapf(err, "Faile to create distribution function: %s", partitionKeyDistribution)
	}

	gens := generators.New(baseCtx, schema, distFunc, intSeed, partitionCount, pkBufferReuseSize, logger)
	defer utils.IgnoreError(gens.Close)

	// if warmup > 0 && !stopFlag.IsHardOrSoft() {
	//	jobsList := jobs.ListFromMode(jobs.WarmupMode, warmup, concurrency)
	//	if err = jobsList.Run(baseCtx, schema, schemaConfig, st, pump, gens, globalStatus, logger, intSeed, warmupStopFlag, failFast, verbose); err != nil {
	//		logger.Error("warmup encountered an error", zap.Error(err))
	//		stopFlag.SetHard(true)
	//	}
	//}

	// if !stopFlag.IsHardOrSoft() {
	//	jobsList := jobs.ListFromMode(mode, duration, concurrency)
	//	if err = jobsList.Run(baseCtx, schema, schemaConfig, st, pump, gens, globalStatus, logger, intSeed, stopFlag.CreateChild("workload"), failFast, verbose); err != nil {
	//		logger.Debug("error detected", zap.Error(err))
	//	}
	//}
	logger.Info("test finished")
	globalStatus.PrintResult(outFile, schema, version)
	if globalStatus.HasErrors() {
		return errors.Errorf("gemini encountered errors, exiting with non zero status")
	}
	return nil
}

const (
	stdDistMean = math.MaxUint64 / 2
	oneStdDev   = 0.341 * math.MaxUint64
)

func createLogger(level string) *zap.Logger {
	lvl := zap.NewAtomicLevel()
	if err := lvl.UnmarshalText([]byte(level)); err != nil {
		lvl.SetLevel(zap.InfoLevel)
	}
	encoderCfg := zap.NewDevelopmentEncoderConfig()
	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.Lock(os.Stdout),
		lvl,
	))
	return logger
}

func getReplicationStrategy(rs string, fallback replication.Replication, logger *zap.Logger) replication.Replication {
	switch rs {
	case "network":
		return replication.NewNetworkTopologyStrategy()
	case "simple":
		return replication.NewSimpleStrategy()
	default:
		rf := make(replication.Replication)
		if err := json.Unmarshal([]byte(strings.ReplaceAll(rs, "'", "\"")), &rf); err != nil {
			logger.Error("unable to parse replication strategy", zap.String("strategy", rs), zap.Error(err))
			return fallback
		}
		return rf
	}
}

func getCQLFeature(feature string) typedef.CQLFeature {
	switch strings.ToLower(feature) {
	case "all":
		return typedef.CQL_FEATURE_ALL
	case "normal":
		return typedef.CQL_FEATURE_NORMAL
	default:
		return typedef.CQL_FEATURE_BASIC
	}
}

var rootCmd = &cobra.Command{
	Use:          "gemini",
	Short:        "Gemini is an automatic random testing tool for Scylla.",
	RunE:         run,
	SilenceUsage: true,
}

func init() {
	rootCmd.Version = version + ", commit " + commit + ", date " + date

	rootCmd.Flags().IntVarP(&profilingPort, "profiling-port", "", 0, "If non-zero starts pprof profiler on given port at 'http://0.0.0.0:<port>/profile'")
	rootCmd.Flags().StringVarP(&mode, "mode", "m", jobs.MixedMode, "Query operation mode. Mode options: write, read, mixed (default)")
	rootCmd.Flags().Uint64VarP(&concurrency, "concurrency", "c", 10, "Number of threads per table to run concurrently")
	rootCmd.Flags().StringVarP(&seed, "seed", "s", "random", "Statement seed value")
	rootCmd.Flags().StringVarP(&schemaSeed, "schema-seed", "", "random", "Schema seed value")
	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "Verbose output during test run")
	rootCmd.Flags().BoolVarP(&failFast, "fail-fast", "f", false, "Stop on the first failure")
	rootCmd.Flags().StringVarP(&level, "level", "", "info", "Specify the logging level, debug|info|warn|error|dpanic|panic|fatal")

	rootCmd.Flags().StringSliceVarP(&testConfig.Hosts, "test-cluster", "t", []string{}, "Host names or IPs of the test cluster that is system under test")
	//nolint:lll
	rootCmd.Flags().StringVarP(&testConfig.Trace, "test-tracing-outfile", "", "", "Specify the file to which tracing information gets written. Two magic names are available, 'stdout' and 'stderr'. By default tracing is disabled.")
	//nolint:lll
	rootCmd.Flags().StringVarP(&testConfig.Consistency, "test-consistency", "", "LOCAL_QUORUM", "Specify the desired consistency as ANY|ONE|TWO|THREE|QUORUM|LOCAL_QUORUM|EACH_QUORUM|LOCAL_ONE")
	rootCmd.Flags().StringVarP(&testConfig.DC, "test-dc", "", "", "Datacenter name for the test cluster")
	//nolint:lll
	rootCmd.Flags().StringVarP(&testConfig.HostSelectionPolicy, "test-host-selection-policy", "", "token-aware", "Host selection policy used by the driver for the test cluster: round-robin|host-pool|token-aware")
	rootCmd.Flags().StringVarP(&testConfig.Username, "test-username", "", "", "Username for the test cluster")
	rootCmd.Flags().StringVarP(&testConfig.Password, "test-password", "", "", "Password for the test cluster")
	rootCmd.Flags().StringVarP(&testConfig.StatementLog, "test-statement-log-file", "", "", "File to write statements flow to")
	rootCmd.Flags().DurationVarP(&testConfig.RequestTimeout, "test-request-timeout", "", 30*time.Second, "Duration of waiting request execution")
	rootCmd.Flags().DurationVarP(&testConfig.ConnectTimeout, "test-connect-timeout", "", 30*time.Second, "Duration of waiting connection established")
	//nolint:lll
	rootCmd.Flags().DurationVarP(&testConfig.MaxRetriesMutateSleep, "test-max-mutation-retries-backoff", "", 10*time.Millisecond, "Duration between attempts to apply a mutation for example 10ms or 1s")
	rootCmd.Flags().IntVarP(&testConfig.MaxRetriesMutate, "test-max-mutation-retries", "", 2, "Maximum number of attempts to apply a mutation")
	rootCmd.Flags().BoolVarP(&testConfig.UseServerSideTimestamps, "test-use-server-timestamps", "", false, "Use server-side generated timestamps for writes")
	//nolint:lll
	rootCmd.Flags().StringVarP(&testReplicationStrategy, "test-replication-strategy", "", "simple", "Specify the desired replication strategy as either the coded short hand simple|network to get the default for each type or provide the entire specification in the form {'class':'....'}")
	//nolint:lll
	rootCmd.Flags().StringSliceVarP(&oracleConfig.Hosts, "oracle-cluster", "o", []string{}, "Host names or IPs of the oracle cluster that provides correct answers. If omitted no oracle will be used")
	//nolint:lll
	rootCmd.Flags().StringVarP(&oracleConfig.Trace, "oracle-tracing-outfile", "", "", "Specify the file to which tracing information gets written. Two magic names are available, 'stdout' and 'stderr'. By default tracing is disabled.")

	//nolint:lll
	rootCmd.Flags().StringVarP(&oracleConfig.Consistency, "oracle-consistency", "", "LOCAL_QUORUM", "Specify the desired consistency as ANY|ONE|TWO|THREE|QUORUM|LOCAL_QUORUM|EACH_QUORUM|LOCAL_ONE")
	rootCmd.Flags().StringVarP(&oracleConfig.DC, "oracle-dc", "", "", "Datacenter name for the oracle cluster")
	//nolint:lll
	rootCmd.Flags().StringVarP(&oracleConfig.HostSelectionPolicy, "oracle-host-selection-policy", "", "token-aware", "Host selection policy used by the driver for the oracle cluster: round-robin|host-pool|token-aware")
	rootCmd.Flags().StringVarP(&oracleConfig.Username, "oracle-username", "", "", "Username for the oracle cluster")
	rootCmd.Flags().StringVarP(&oracleConfig.Password, "oracle-password", "", "", "Password for the oracle cluster")
	rootCmd.Flags().StringVarP(&oracleConfig.StatementLog, "oracle-statement-log-file", "", "", "File to write statements flow to")
	rootCmd.Flags().DurationVarP(&oracleConfig.RequestTimeout, "oracle-request-timeout", "", 30*time.Second, "Duration of waiting request execution")
	//nolint:lll
	rootCmd.Flags().DurationVarP(&oracleConfig.ConnectTimeout, "oracle-connect-timeout", "", 30*time.Second, "Duration of waiting connection established")
	//nolint:lll
	rootCmd.Flags().DurationVarP(&oracleConfig.MaxRetriesMutateSleep, "oracle-max-mutation-retries-backoff", "", 10*time.Millisecond, "Duration between attempts to apply a mutation for example 10ms or 1s")
	rootCmd.Flags().IntVarP(&oracleConfig.MaxRetriesMutate, "oracle-max-mutation-retries", "", 2, "Maximum number of attempts to apply a mutation")
	rootCmd.Flags().BoolVarP(&oracleConfig.UseServerSideTimestamps, "oracle-use-server-timestamps", "", false, "Use server-side generated timestamps for writes")
	//nolint:lll
	rootCmd.Flags().StringVarP(&oracleReplicationStrategy, "oracle-replication-strategy", "", "simple", "Specify the desired replication strategy of the oracle cluster as either the coded short hand simple|network to get the default for each type or provide the entire specification in the form {'class':'....'}")

	rootCmd.Flags().StringVarP(&schemaFile, "schema", "", "", "Schema JSON config file")
	rootCmd.Flags().BoolVarP(&dropSchema, "drop-schema", "d", false, "Drop schema before starting tests run")
	rootCmd.Flags().DurationVarP(&duration, "duration", "", 30*time.Second, "")
	rootCmd.Flags().StringVarP(&outFileArg, "outfile", "", "", "Specify the name of the file where the results should go")
	rootCmd.Flags().DurationVarP(&warmup, "warmup", "", 30*time.Second, "Specify the warmup perid as a duration for example 30s or 10h")
	rootCmd.Flags().StringArrayVarP(&tableOptions, "table-options", "", []string{}, "Repeatable argument to set table options to be added to the created tables")
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
	rootCmd.Flags().Uint64VarP(&pkBufferReuseSize, "partition-key-buffer-reuse-size", "", 100, "Number of reused buffered partition keys")
	//nolint:lll
	rootCmd.Flags().Uint64VarP(&partitionCount, "token-range-slices", "", 10000, "Number of slices to divide the token space into")
	//nolint:lll
	rootCmd.Flags().StringVarP(&partitionKeyDistribution, "partition-key-distribution", "", "uniform", "Specify the distribution from which to draw partition keys, supported values are currently uniform|normal|zipf")
	rootCmd.Flags().Float64VarP(&normalDistMean, "normal-dist-mean", "", stdDistMean, "Mean of the normal distribution")
	rootCmd.Flags().Float64VarP(&normalDistSigma, "normal-dist-sigma", "", oneStdDev, "Sigma of the normal distribution, defaults to one standard deviation ~0.341")
	rootCmd.Flags().BoolVarP(&useCounters, "use-counters", "", false, "Ensure that at least one table is a counter table")
	rootCmd.Flags().IntVarP(&asyncObjectStabilizationAttempts, "async-objects-stabilization-attempts", "", 10, "Maximum number of attempts to validate result sets from MV and SI")
	//nolint:lll
	rootCmd.Flags().DurationVarP(&asyncObjectStabilizationDelay, "async-objects-stabilization-backoff", "", 10*time.Millisecond, "Duration between attempts to validate result sets from MV and SI for example 10ms or 1s")
	rootCmd.Flags().BoolVarP(&useLWT, "use-lwt", "", false, "Emit LWT based updates")
	rootCmd.Flags().IntVarP(&maxErrorsToStore, "max-errors-to-store", "", 1000, "Maximum number of errors to store and output at the end")
	rootCmd.Flags().StringVarP(&statementLogFileCompression, "statement-log-file-compression", "", "zstd", "Compression algorithm to use for statement log files")

	_ = rootCmd.MarkFlagRequired("test-cluster")
}

func printSetup(seed, schemaSeed uint64) {
	tw := new(tabwriter.Writer)
	tw.Init(os.Stdout, 0, 8, 2, '\t', tabwriter.AlignRight)
	fmt.Fprintf(tw, "Seed:\t%d\n", seed)
	fmt.Fprintf(tw, "Schema seed:\t%d\n", schemaSeed)
	fmt.Fprintf(tw, "Maximum duration:\t%s\n", duration)
	fmt.Fprintf(tw, "Warmup duration:\t%s\n", warmup)
	fmt.Fprintf(tw, "Concurrency:\t%d\n", concurrency)
	fmt.Fprintf(tw, "Test cluster:\t%s\n", strings.Join(testConfig.Hosts, ", "))
	fmt.Fprintf(tw, "Oracle cluster:\t%s\n", strings.Join(oracleConfig.Hosts, ", "))
	if outFileArg == "" {
		fmt.Fprintf(tw, "Output file:\t%s\n", "<stdout>")
	} else {
		fmt.Fprintf(tw, "Output file:\t%s\n", outFileArg)
	}
	tw.Flush()
}

func RealRandom() uint64 {
	return rand.New(realrandom.Source).Uint64()
}

func validateSeed(seed string) error {
	if seed == "random" {
		return nil
	}
	_, err := strconv.ParseUint(seed, 10, 64)
	return err
}

func seedFromString(seed string) uint64 {
	if seed == "random" {
		return RealRandom()
	}
	val, _ := strconv.ParseUint(seed, 10, 64)
	return val
}

// generateSchema generates schema, if schema seed is random and schema did not pass validation it regenerates it
func generateSchema(logger *zap.Logger, sc typedef.SchemaConfig, schemaSeed string) (schema *typedef.Schema, intSchemaSeed uint64, err error) {
	intSchemaSeed = seedFromString(schemaSeed)
	schema = generators.GenSchema(sc, intSchemaSeed)
	err = schema.Validate(partitionCount)
	if err == nil {
		return schema, intSchemaSeed, nil
	}
	if schemaSeed != "random" {
		// If user provided schema, allow to run it, but log warning
		logger.Warn(errors.Wrap(err, "validation failed, running this test could end up in error or stale gemini").Error())
		return schema, intSchemaSeed, nil
	}

	for err != nil {
		intSchemaSeed = seedFromString(schemaSeed)
		schema = generators.GenSchema(sc, intSchemaSeed)
		err = schema.Validate(partitionCount)
	}

	return schema, intSchemaSeed, nil
}
