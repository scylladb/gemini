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
	"log"
	"math"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/scylladb/gemini/pkg/auth"
	"github.com/scylladb/gemini/pkg/builders"
	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/jobs"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/tableopts"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"

	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"

	"github.com/gocql/gocql"
	"github.com/hailocab/go-hostpool"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/rand"
	"golang.org/x/net/context"
	"gonum.org/v1/gonum/stat/distuv"
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
	seed                             uint64
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
)

func interactive() bool {
	return !nonInteractive
}

func readSchema(confFile string) (*typedef.Schema, error) {
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
	schemaBuilder.Keyspace(shm.Keyspace)
	for _, tbl := range shm.Tables {
		schemaBuilder.Table(tbl)
	}
	return schemaBuilder.Build(), nil
}

type createBuilder struct {
	stmt string
}

func (cb createBuilder) ToCql() (stmt string, names []string) {
	return cb.stmt, nil
}

func run(_ *cobra.Command, _ []string) error {
	logger := createLogger(level)
	globalStatus := status.NewGlobalStatus(1000)
	defer utils.IgnoreError(logger.Sync)

	cons, err := gocql.ParseConsistencyWrapper(consistency)
	if err != nil {
		logger.Error("Unable parse consistency, error=%s. Falling back on Quorum", zap.Error(err))
		cons = gocql.Quorum
	}

	testHostSelectionPolicy, err := getHostSelectionPolicy(testClusterHostSelectionPolicy, testClusterHost)
	if err != nil {
		return err
	}
	oracleHostSelectionPolicy, err := getHostSelectionPolicy(oracleClusterHostSelectionPolicy, oracleClusterHost)
	if err != nil {
		return err
	}

	go func() {
		http.Handle("/metrics", promhttp.Handler())
		_ = http.ListenAndServe(bind, nil)
	}()

	if profilingPort != 0 {
		go func() {
			mux := http.NewServeMux()
			mux.HandleFunc("/profile", pprof.Profile)
			log.Fatal(http.ListenAndServe(":"+strconv.Itoa(profilingPort), mux))
		}()
	}

	if err = printSetup(); err != nil {
		return errors.Wrapf(err, "unable to print setup")
	}
	distFunc, err := createDistributionFunc(partitionKeyDistribution, math.MaxUint64, seed, stdDistMean, oneStdDev)
	if err != nil {
		return err
	}

	outFile, err := createFile(outFileArg, os.Stdout)
	if err != nil {
		return err
	}
	defer utils.IgnoreError(outFile.Sync)

	schemaConfig := createSchemaConfig(logger)
	if err = schemaConfig.Valid(); err != nil {
		return errors.Wrap(err, "invalid schema configuration")
	}
	var schema *typedef.Schema
	if len(schemaFile) > 0 {
		schema, err = readSchema(schemaFile)
		if err != nil {
			return errors.Wrap(err, "cannot create schema")
		}
	} else {
		schema = generators.GenSchema(schemaConfig)
	}

	jsonSchema, _ := json.MarshalIndent(schema, "", "    ")
	fmt.Printf("Schema: %v\n", string(jsonSchema))

	testCluster, oracleCluster := createClusters(cons, testHostSelectionPolicy, oracleHostSelectionPolicy, logger)
	storeConfig := store.Config{
		MaxRetriesMutate:        maxRetriesMutate,
		MaxRetriesMutateSleep:   maxRetriesMutateSleep,
		UseServerSideTimestamps: useServerSideTimestamps,
	}
	var tracingFile *os.File
	if tracingOutFile != "" {
		switch tracingOutFile {
		case "stderr":
			tracingFile = os.Stderr
		case "stdout":
			tracingFile = os.Stdout
		default:
			tf, ioErr := createFile(tracingOutFile, os.Stdout)
			if ioErr != nil {
				return ioErr
			}
			tracingFile = tf
			defer utils.IgnoreError(tracingFile.Sync)
		}
	}
	st := store.New(schema, testCluster, oracleCluster, storeConfig, tracingFile, logger)
	defer utils.IgnoreError(st.Close)

	if dropSchema && mode != jobs.ReadMode {
		for _, stmt := range generators.GetDropSchema(schema) {
			logger.Debug(stmt)
			if err = st.Mutate(context.Background(), createBuilder{stmt: stmt}); err != nil {
				return errors.Wrap(err, "unable to drop schema")
			}
		}
	}

	testKeyspace, oracleKeyspace := generators.GetCreateKeyspaces(schema)
	if err = st.Create(context.Background(), createBuilder{stmt: testKeyspace}, createBuilder{stmt: oracleKeyspace}); err != nil {
		return errors.Wrap(err, "unable to create keyspace")
	}

	for _, stmt := range generators.GetCreateSchema(schema) {
		logger.Debug(stmt)
		if err = st.Mutate(context.Background(), createBuilder{stmt: stmt}); err != nil {
			return errors.Wrap(err, "unable to create schema")
		}
	}

	ctx, done := context.WithTimeout(context.Background(), duration+warmup+time.Second*2)
	warmupStopFlag := stop.NewFlag()
	workStopFlag := stop.NewFlag()
	stop.StartOsSignalsTransmitter(logger, &warmupStopFlag, &workStopFlag)
	pump := jobs.NewPump(ctx, logger)

	generators := createGenerators(ctx, schema, schemaConfig, distFunc, concurrency, partitionCount, logger)

	if !nonInteractive {
		sp := createSpinner(interactive())
		ticker := time.NewTicker(time.Second)
		go func() {
			defer done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					sp.Set(" Running Gemini... %v", globalStatus)
				}
			}
		}()
	}

	if warmup > 0 && !warmupStopFlag.IsHardOrSoft() {
		jobsList := jobs.ListFromMode(jobs.WarmupMode, warmup, concurrency)
		if err = jobsList.Run(ctx, schema, schemaConfig, st, pump, generators, globalStatus, logger, seed, &warmupStopFlag, failFast, verbose); err != nil {
			logger.Error("warmup encountered an error", zap.Error(err))
		}
	}

	select {
	case <-ctx.Done():
	default:
		if workStopFlag.IsHardOrSoft() {
			break
		}
		jobsList := jobs.ListFromMode(mode, duration, concurrency)
		if err = jobsList.Run(ctx, schema, schemaConfig, st, pump, generators, globalStatus, logger, seed, &workStopFlag, failFast, verbose); err != nil {
			logger.Debug("error detected", zap.Error(err))
		}

	}
	logger.Info("test finished")
	globalStatus.PrintResult(outFile, schema, version)
	if globalStatus.HasErrors() {
		return errors.Errorf("gemini encountered errors, exiting with non zero status")
	}
	return nil
}

func createFile(fname string, def *os.File) (*os.File, error) {
	if fname != "" {
		f, err := os.Create(fname)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to open output file %s", fname)
		}
		return f, nil
	}
	return def, nil
}

const (
	stdDistMean = math.MaxUint64 / 2
	oneStdDev   = 0.341 * math.MaxUint64
)

func createDistributionFunc(distribution string, size, seed uint64, mu, sigma float64) (generators.DistributionFunc, error) {
	switch strings.ToLower(distribution) {
	case "zipf":
		dist := rand.NewZipf(rand.New(rand.NewSource(seed)), 1.1, 1.1, size)
		return func() generators.TokenIndex {
			return generators.TokenIndex(dist.Uint64())
		}, nil
	case "normal":
		dist := distuv.Normal{
			Src:   rand.NewSource(seed),
			Mu:    mu,
			Sigma: sigma,
		}
		return func() generators.TokenIndex {
			return generators.TokenIndex(dist.Rand())
		}, nil
	case "uniform":
		rnd := rand.New(rand.NewSource(seed))
		return func() generators.TokenIndex {
			return generators.TokenIndex(rnd.Uint64n(size))
		}, nil
	default:
		return nil, errors.Errorf("unsupported distribution: %s", distribution)
	}
}

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

func createClusters(
	consistency gocql.Consistency,
	testHostSelectionPolicy, oracleHostSelectionPolicy gocql.HostSelectionPolicy,
	logger *zap.Logger,
) (*gocql.ClusterConfig, *gocql.ClusterConfig) {
	retryPolicy := &gocql.ExponentialBackoffRetryPolicy{
		Min:        time.Second,
		Max:        60 * time.Second,
		NumRetries: 5,
	}
	testCluster := gocql.NewCluster(testClusterHost...)
	testCluster.Timeout = requestTimeout
	testCluster.ConnectTimeout = connectTimeout
	testCluster.RetryPolicy = retryPolicy
	testCluster.Consistency = consistency
	testCluster.PoolConfig.HostSelectionPolicy = testHostSelectionPolicy
	testAuthenticator, testAuthErr := auth.BuildAuthenticator(testClusterUsername, testClusterPassword)
	if testAuthErr != nil {
		logger.Warn("%s for test cluster", zap.Error(testAuthErr))
	}
	testCluster.Authenticator = testAuthenticator
	if len(oracleClusterHost) == 0 {
		return testCluster, nil
	}
	oracleCluster := gocql.NewCluster(oracleClusterHost...)
	testCluster.Timeout = requestTimeout
	testCluster.ConnectTimeout = connectTimeout
	oracleCluster.RetryPolicy = retryPolicy
	oracleCluster.Consistency = consistency
	oracleCluster.PoolConfig.HostSelectionPolicy = oracleHostSelectionPolicy
	oracleAuthenticator, oracleAuthErr := auth.BuildAuthenticator(oracleClusterUsername, oracleClusterPassword)
	if oracleAuthErr != nil {
		logger.Warn("%s for oracle cluster", zap.Error(oracleAuthErr))
	}
	oracleCluster.Authenticator = oracleAuthenticator
	return testCluster, oracleCluster
}

func createTableOptions(tableOptionStrings []string, logger *zap.Logger) []tableopts.Option {
	var tableOptions []tableopts.Option
	for _, optionString := range tableOptionStrings {
		o, err := tableopts.FromCQL(optionString)
		if err != nil {
			logger.Warn("invalid table option", zap.String("option", optionString), zap.Error(err))
			continue
		}
		tableOptions = append(tableOptions, o)
	}
	return tableOptions
}

func getReplicationStrategy(rs string, fallback *replication.Replication, logger *zap.Logger) *replication.Replication {
	switch rs {
	case "network":
		return replication.NewNetworkTopologyStrategy()
	case "simple":
		return replication.NewSimpleStrategy()
	default:
		replicationStrategy := &replication.Replication{}
		if err := json.Unmarshal([]byte(strings.ReplaceAll(rs, "'", "\"")), replicationStrategy); err != nil {
			logger.Error("unable to parse replication strategy", zap.String("strategy", rs), zap.Error(err))
			return fallback
		}
		return replicationStrategy
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

func getHostSelectionPolicy(policy string, hosts []string) (gocql.HostSelectionPolicy, error) {
	switch policy {
	case "round-robin":
		return gocql.RoundRobinHostPolicy(), nil
	case "host-pool":
		return gocql.HostPoolHostPolicy(hostpool.New(hosts)), nil
	case "token-aware":
		return gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy()), nil
	default:
		return nil, fmt.Errorf("unknown host selection policy \"%s\"", policy)
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
	rootCmd.Flags().Uint64VarP(&seed, "seed", "s", 1, "PRNG seed value")
	rootCmd.Flags().BoolVarP(&dropSchema, "drop-schema", "d", false, "Drop schema before starting tests run")
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
	rootCmd.Flags().StringVarP(&consistency, "consistency", "", "QUORUM", "Specify the desired consistency as ANY|ONE|TWO|THREE|QUORUM|LOCAL_QUORUM|EACH_QUORUM|LOCAL_ONE")
	rootCmd.Flags().IntVarP(&maxTables, "max-tables", "", 1, "Maximum number of generated tables")
	rootCmd.Flags().IntVarP(&maxPartitionKeys, "max-partition-keys", "", 6, "Maximum number of generated partition keys")
	rootCmd.Flags().IntVarP(&minPartitionKeys, "min-partition-keys", "", 2, "Minimum number of generated partition keys")
	rootCmd.Flags().IntVarP(&maxClusteringKeys, "max-clustering-keys", "", 4, "Maximum number of generated clustering keys")
	rootCmd.Flags().IntVarP(&minClusteringKeys, "min-clustering-keys", "", 2, "Minimum number of generated clustering keys")
	rootCmd.Flags().IntVarP(&maxColumns, "max-columns", "", 16, "Maximum number of generated columns")
	rootCmd.Flags().IntVarP(&minColumns, "min-columns", "", 8, "Minimum number of generated columns")
	rootCmd.Flags().StringVarP(&datasetSize, "dataset-size", "", "large", "Specify the type of dataset size to use, small|large")
	rootCmd.Flags().StringVarP(&cqlFeatures, "cql-features", "", "basic", "Specify the type of cql features to use, basic|normal|all")
	rootCmd.Flags().StringVarP(&level, "level", "", "info", "Specify the logging level, debug|info|warn|error|dpanic|panic|fatal")
	rootCmd.Flags().IntVarP(&maxRetriesMutate, "max-mutation-retries", "", 2, "Maximum number of attempts to apply a mutation")
	rootCmd.Flags().DurationVarP(
		&maxRetriesMutateSleep, "max-mutation-retries-backoff", "", 10*time.Millisecond,
		"Duration between attempts to apply a mutation for example 10ms or 1s")
	rootCmd.Flags().Uint64VarP(&pkBufferReuseSize, "partition-key-buffer-reuse-size", "", 100, "Number of reused buffered partition keys")
	rootCmd.Flags().Uint64VarP(&partitionCount, "token-range-slices", "", 10000, "Number of slices to divide the token space into")
	rootCmd.Flags().StringVarP(
		&partitionKeyDistribution, "partition-key-distribution", "", "uniform",
		"Specify the distribution from which to draw partition keys, supported values are currently uniform|normal|zipf")
	rootCmd.Flags().Float64VarP(&normalDistMean, "normal-dist-mean", "", stdDistMean, "Mean of the normal distribution")
	rootCmd.Flags().Float64VarP(&normalDistSigma, "normal-dist-sigma", "", oneStdDev, "Sigma of the normal distribution, defaults to one standard deviation ~0.341")
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
		&oracleClusterHostSelectionPolicy, "oracle-host-selection-policy", "", "round-robin",
		"Host selection policy used by the driver for the oracle cluster: round-robin|host-pool|token-aware")
	rootCmd.Flags().StringVarP(
		&testClusterHostSelectionPolicy, "test-host-selection-policy", "", "round-robin",
		"Host selection policy used by the driver for the test cluster: round-robin|host-pool|token-aware")
	rootCmd.Flags().BoolVarP(&useServerSideTimestamps, "use-server-timestamps", "", false, "Use server-side generated timestamps for writes")
	rootCmd.Flags().DurationVarP(&requestTimeout, "request-timeout", "", 30*time.Second, "Duration of waiting request execution")
	rootCmd.Flags().DurationVarP(&connectTimeout, "connect-timeout", "", 30*time.Second, "Duration of waiting connection established")
	rootCmd.Flags().IntVarP(&profilingPort, "profiling-port", "", 0, "If non-zero starts pprof profiler on given port at 'http://0.0.0.0:<port>/profile'")
	rootCmd.Flags().IntVarP(&maxErrorsToStore, "max-errors-to-store", "", 1000, "Maximum number of errors to store and output at the end")
}

func printSetup() error {
	tw := new(tabwriter.Writer)
	tw.Init(os.Stdout, 0, 8, 2, '\t', tabwriter.AlignRight)
	rand.Seed(seed)
	fmt.Fprintf(tw, "Seed:\t%d\n", seed)
	fmt.Fprintf(tw, "Maximum duration:\t%s\n", duration)
	fmt.Fprintf(tw, "Warmup duration:\t%s\n", warmup)
	fmt.Fprintf(tw, "Concurrency:\t%d\n", concurrency)
	fmt.Fprintf(tw, "Test cluster:\t%s\n", testClusterHost)
	fmt.Fprintf(tw, "Oracle cluster:\t%s\n", oracleClusterHost)
	if outFileArg == "" {
		fmt.Fprintf(tw, "Output file:\t%s\n", "<stdout>")
	} else {
		fmt.Fprintf(tw, "Output file:\t%s\n", outFileArg)
	}
	tw.Flush()
	return nil
}
