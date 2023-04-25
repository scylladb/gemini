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
	"io/ioutil"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/gocql/gocql"
	"github.com/hailocab/go-hostpool"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/scylladb/gemini"
	"github.com/scylladb/gemini/auth"
	"github.com/scylladb/gemini/replication"
	"github.com/scylladb/gemini/store"
	"github.com/scylladb/gemini/tableopts"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/rand"
	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
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
	requestTimeout					 time.Duration
	connectTimeout					 time.Duration
)

const (
	writeMode = "write"
	readMode  = "read"
	mixedMode = "mixed"
)

type JobError struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
	Query     string    `json:"query"`
}

type Results interface {
	Merge(*Status) Status
	Print()
}

func interactive() bool {
	return !nonInteractive
}

type testJob func(context.Context, <-chan heartBeat, *gemini.Schema, gemini.SchemaConfig, *gemini.Table, store.Store, *rand.Rand, gemini.PartitionRangeConfig, *gemini.Generator, chan Status, string, time.Duration, *zap.Logger) error

func readSchema(confFile string) (*gemini.Schema, error) {
	byteValue, err := ioutil.ReadFile(confFile)
	if err != nil {
		return nil, err
	}

	var shm gemini.Schema

	err = json.Unmarshal(byteValue, &shm)
	if err != nil {
		return nil, err
	}

	schemaBuilder := gemini.NewSchemaBuilder()
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

func run(cmd *cobra.Command, args []string) error {
	logger := createLogger(level)
	defer logger.Sync()

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

	if err := printSetup(); err != nil {
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
	defer outFile.Sync()

	schemaConfig := createSchemaConfig(logger)
	if err := schemaConfig.Valid(); err != nil {
		return errors.Wrap(err, "invalid schema configuration")
	}
	var schema *gemini.Schema
	if len(schemaFile) > 0 {
		var err error
		schema, err = readSchema(schemaFile)
		if err != nil {
			return errors.Wrap(err, "cannot create schema")
		}
	} else {
		schema = gemini.GenSchema(schemaConfig)
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
			tf, err := createFile(tracingOutFile, os.Stdout)
			if err != nil {
				return err
			}
			tracingFile = tf
			defer tracingFile.Sync()
		}
	}
	store := store.New(schema, testCluster, oracleCluster, storeConfig, tracingFile, logger)
	defer store.Close()

	if dropSchema && mode != readMode {
		for _, stmt := range schema.GetDropSchema() {
			logger.Debug(stmt)
			if err := store.Mutate(context.Background(), createBuilder{stmt: stmt}); err != nil {
				return errors.Wrap(err, "unable to drop schema")
			}
		}
	}

	testKeyspace, oracleKeyspace := schema.GetCreateKeyspaces()
	if err := store.Create(context.Background(), createBuilder{stmt: testKeyspace}, createBuilder{stmt: oracleKeyspace}); err != nil {
		return errors.Wrap(err, "unable to create keyspace")
	}

	for _, stmt := range schema.GetCreateSchema() {
		logger.Debug(stmt)
		if err := store.Mutate(context.Background(), createBuilder{stmt: stmt}); err != nil {
			return errors.Wrap(err, "unable to create schema")
		}
	}

	result := make(chan Status, 10000)
	ctx, done := context.WithTimeout(context.Background(), duration+warmup)
	g, gCtx := errgroup.WithContext(ctx)
	var graceful = make(chan os.Signal, 1)
	signal.Notify(graceful, syscall.SIGTERM, syscall.SIGINT)
	g.Go(func() error {
		select {
		case <-gCtx.Done():
			return ctx.Err()
		case <-graceful:
			logger.Info("Told to stop, exiting.")
			done()
			return ctx.Err()
		}
	})
	pump := &Pump{
		ch:     make(chan heartBeat, 10000),
		logger: logger.Named("pump"),
	}
	generators := createGenerators(gCtx, schema, schemaConfig, distFunc, concurrency, partitionCount, logger)
	sp := createSpinner(interactive())
	g.Go(func() error {
		defer done()
		return pump.Start(gCtx)
	})
	resCh := make(chan *Status, 1)
	g.Go(func() error {
		defer done()
		res, err := sampleStatus(gCtx, result, sp, logger)
		sp.Stop()
		resCh <- res
		return err
	})
	time.AfterFunc(duration+warmup, func() {
		defer done()
		logger.Info("Test run completed. Exiting.")
	})

	if warmup > 0 {
		if err := job(gCtx, WarmupJob, concurrency, schema, schemaConfig, store, pump, generators, result, logger); err != nil {
			logger.Error("warmup encountered an error", zap.Error(err))
		}
	}

	select {
	case <-gCtx.Done():
	default:
		testJobs := jobsFromMode(mode)
		for id := range testJobs {
			tJob := testJobs[id]
			g.Go(func() error {
				return job(gCtx, tJob, concurrency, schema, schemaConfig, store, pump, generators, result, logger)
			})
		}
		if err := g.Wait(); err != nil {
			logger.Debug("error detected", zap.Error(err))
		}
	}
	close(result)
	logger.Info("result channel closed")
	res := <-resCh
	res.PrintResult(outFile, schema)
	if res.HasErrors() {
		return errors.Errorf("gemini encountered errors, exiting with non zero status")
	}
	return nil
}

func jobsFromMode(mode string) []testJob {
	switch mode {
	case writeMode:
		return []testJob{
			MutationJob,
		}
	case readMode:
		return []testJob{
			ValidationJob,
		}
	default:
		return []testJob{
			MutationJob,
			ValidationJob,
		}
	}
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

func createDistributionFunc(distribution string, size, seed uint64, mu, sigma float64) (gemini.DistributionFunc, error) {
	switch strings.ToLower(distribution) {
	case "zipf":
		dist := rand.NewZipf(rand.New(rand.NewSource(seed)), 1.1, 1.1, size)
		return func() gemini.TokenIndex {
			return gemini.TokenIndex(dist.Uint64())
		}, nil
	case "normal":
		dist := distuv.Normal{
			Src:   rand.NewSource(seed),
			Mu:    mu,
			Sigma: sigma,
		}
		return func() gemini.TokenIndex {
			return gemini.TokenIndex(dist.Rand())
		}, nil
	case "uniform":
		rnd := rand.New(rand.NewSource(seed))
		return func() gemini.TokenIndex {
			return gemini.TokenIndex(rnd.Uint64n(size))
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

func createClusters(consistency gocql.Consistency, testHostSelectionPolicy gocql.HostSelectionPolicy, oracleHostSelectionPolicy gocql.HostSelectionPolicy, logger *zap.Logger) (*gocql.ClusterConfig, *gocql.ClusterConfig) {
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
	oracleCluster.Timeout = 120 * time.Second
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

func getCQLFeature(feature string) gemini.CQLFeature {
	switch strings.ToLower(feature) {
	case "all":
		return gemini.CQL_FEATURE_ALL
	case "normal":
		return gemini.CQL_FEATURE_NORMAL
	default:
		return gemini.CQL_FEATURE_BASIC
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
	rootCmd.MarkFlagRequired("test-cluster")
	rootCmd.Flags().StringVarP(&testClusterUsername, "test-username", "", "", "Username for the test cluster")
	rootCmd.Flags().StringVarP(&testClusterPassword, "test-password", "", "", "Password for the test cluster")
	rootCmd.Flags().StringSliceVarP(&oracleClusterHost, "oracle-cluster", "o", []string{}, "Host names or IPs of the oracle cluster that provides correct answers. If omitted no oracle will be used")
	rootCmd.Flags().StringVarP(&oracleClusterUsername, "oracle-username", "", "", "Username for the oracle cluster")
	rootCmd.Flags().StringVarP(&oracleClusterPassword, "oracle-password", "", "", "Password for the oracle cluster")
	rootCmd.Flags().StringVarP(&schemaFile, "schema", "", "", "Schema JSON config file")
	rootCmd.Flags().StringVarP(&mode, "mode", "m", mixedMode, "Query operation mode. Mode options: write, read, mixed (default)")
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
	rootCmd.Flags().StringVarP(&replicationStrategy, "replication-strategy", "", "simple", "Specify the desired replication strategy as either the coded short hand simple|network to get the default for each type or provide the entire specification in the form {'class':'....'}")
	rootCmd.Flags().StringVarP(&oracleReplicationStrategy, "oracle-replication-strategy", "", "simple", "Specify the desired replication strategy of the oracle cluster as either the coded short hand simple|network to get the default for each type or provide the entire specification in the form {'class':'....'}")
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
	rootCmd.Flags().DurationVarP(&maxRetriesMutateSleep, "max-mutation-retries-backoff", "", 10*time.Millisecond, "Duration between attempts to apply a mutation for example 10ms or 1s")
	rootCmd.Flags().Uint64VarP(&pkBufferReuseSize, "partition-key-buffer-reuse-size", "", 100, "Number of reused buffered partition keys")
	rootCmd.Flags().Uint64VarP(&partitionCount, "token-range-slices", "", 10000, "Number of slices to divide the token space into")
	rootCmd.Flags().StringVarP(&partitionKeyDistribution, "partition-key-distribution", "", "uniform", "Specify the distribution from which to draw partition keys, supported values are currently uniform|normal|zipf")
	rootCmd.Flags().Float64VarP(&normalDistMean, "normal-dist-mean", "", stdDistMean, "Mean of the normal distribution")
	rootCmd.Flags().Float64VarP(&normalDistSigma, "normal-dist-sigma", "", oneStdDev, "Sigma of the normal distribution, defaults to one standard deviation ~0.341")
	rootCmd.Flags().StringVarP(&tracingOutFile, "tracing-outfile", "", "", "Specify the file to which tracing information gets written. Two magic names are available, 'stdout' and 'stderr'. By default tracing is disabled.")
	rootCmd.Flags().BoolVarP(&useCounters, "use-counters", "", false, "Ensure that at least one table is a counter table")
	rootCmd.Flags().IntVarP(&asyncObjectStabilizationAttempts, "async-objects-stabilization-attempts", "", 10, "Maximum number of attempts to validate result sets from MV and SI")
	rootCmd.Flags().DurationVarP(&asyncObjectStabilizationDelay, "async-objects-stabilization-backoff", "", 10*time.Millisecond, "Duration between attempts to validate result sets from MV and SI for example 10ms or 1s")
	rootCmd.Flags().BoolVarP(&useLWT, "use-lwt", "", false, "Emit LWT based updates")
	rootCmd.Flags().StringVarP(&oracleClusterHostSelectionPolicy, "oracle-host-selection-policy", "", "round-robin", "Host selection policy used by the driver for the oracle cluster: round-robin|host-pool|token-aware")
	rootCmd.Flags().StringVarP(&testClusterHostSelectionPolicy, "test-host-selection-policy", "", "round-robin", "Host selection policy used by the driver for the test cluster: round-robin|host-pool|token-aware")
	rootCmd.Flags().BoolVarP(&useServerSideTimestamps, "use-server-timestamps", "", false, "Use server-side generated timestamps for writes")
	rootCmd.Flags().DurationVarP(&requestTimeout, "request-timeout", "", 30*time.Second, "Duration of waiting request execution")
	rootCmd.Flags().DurationVarP(&connectTimeout, "connect-timeout", "", 30*time.Second, "Duration of waiting connection established")
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

func newHeartBeat() heartBeat {
	r := rand.Intn(10)
	switch r {
	case 0:
		return heartBeat{
			sleep: 10 * time.Millisecond,
		}
	default:
		return heartBeat{}
	}
}
