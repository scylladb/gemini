// Copyright (C) 2018 ScyllaDB

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/briandowns/spinner"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/scylladb/gemini"
	"github.com/scylladb/gemini/replication"
	"github.com/scylladb/gemini/store"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/rand"
	"golang.org/x/net/context"
	"gonum.org/v1/gonum/stat/distuv"
	"gopkg.in/tomb.v2"
)

var (
	testClusterHost          []string
	oracleClusterHost        []string
	schemaFile               string
	outFileArg               string
	concurrency              uint64
	seed                     uint64
	dropSchema               bool
	verbose                  bool
	mode                     string
	failFast                 bool
	nonInteractive           bool
	duration                 time.Duration
	bind                     string
	warmup                   time.Duration
	compactionStrategy       string
	replicationStrategy      string
	consistency              string
	maxPartitionKeys         int
	minPartitionKeys         int
	maxClusteringKeys        int
	minClusteringKeys        int
	maxColumns               int
	minColumns               int
	datasetSize              string
	cqlFeatures              string
	level                    string
	maxRetriesMutate         int
	maxRetriesMutateSleep    time.Duration
	pkBufferReuseSize        uint64
	partitionCount           uint64
	partitionKeyDistribution string
	normalDistMean           float64
	normalDistSigma          float64
	tracingOutFile           string
	useCounters              bool
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

type testJob func(context.Context, <-chan heartBeat, *gemini.Schema, gemini.SchemaConfig, *gemini.Table, store.Store, *rand.Rand, gemini.PartitionRangeConfig, *gemini.Generator, chan Status, string, time.Duration, *zap.Logger)

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

	testCluster, oracleCluster := createClusters(cons)
	storeConfig := store.Config{
		MaxRetriesMutate:      maxRetriesMutate,
		MaxRetriesMutateSleep: maxRetriesMutateSleep,
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
	for _, stmt := range schema.GetCreateSchema() {
		logger.Debug(stmt)
		if err := store.Mutate(context.Background(), createBuilder{stmt: stmt}); err != nil {
			return errors.Wrap(err, "unable to create schema")
		}
	}

	t := &tomb.Tomb{}
	result := make(chan Status, 10000)
	endResult := make(chan Status, 1)
	pump := createPump(t, 10000, logger)
	generators := createGenerators(schema, schemaConfig, distFunc, concurrency, partitionCount, logger)
	t.Go(func() error {
		var sp *spinner.Spinner = nil
		if interactive() {
			sp = createSpinner()
		}
		pump.Start(duration+warmup, createPumpCallback(generators, result, sp))
		endResult <- sampleStatus(result, sp, logger)
		return nil
	})

	launch(schema, schemaConfig, store, pump, generators, result, logger)
	close(result)
	logger.Debug("result channel closed")
	_ = t.Wait()
	res := <-endResult
	res.PrintResult(outFile, schema)
	if res.HasErrors() {
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

func launch(schema *gemini.Schema, schemaConfig gemini.SchemaConfig, store store.Store, pump *Pump, generators []*gemini.Generator, result chan Status, logger *zap.Logger) {
	if doWarmup(schema, schemaConfig, store, pump, generators, result, logger) {
		logger.Debug("doWarmup terminates launch")
		return
	}
	t := &tomb.Tomb{}
	switch mode {
	case writeMode:
		entombJobs(t, concurrency, schema, schemaConfig, store, pump, generators, result, logger, MutationJob)
	case readMode:
		entombJobs(t, concurrency, schema, schemaConfig, store, pump, generators, result, logger, ValidationJob)
	default:
		entombJobs(t, concurrency, schema, schemaConfig, store, pump, generators, result, logger, MutationJob, ValidationJob)
	}
	_ = t.Wait()
	logger.Info("All jobs complete")
}

func doWarmup(schema *gemini.Schema, schemaConfig gemini.SchemaConfig, s store.Store, pump *Pump, generators []*gemini.Generator, result chan Status, logger *zap.Logger) bool {
	if warmup > 0 {
		t := &tomb.Tomb{}
		entombJobs(t, concurrency, schema, schemaConfig, s, pump, generators, result, logger, WarmupJob)
		_ = t.Wait()
		logger.Info("Warmup done")
		select {
		case <-pump.t.Dying():
			logger.Debug("Warmup dying")
			return true
		case <-pump.t.Dead():
			logger.Debug("Warmup dead")
			return true
		default:
			logger.Debug("Warmup not dying")
		}
	}
	return false
}

func wrapJobInTombFunc(t *tomb.Tomb, f testJob, actors uint64, schema *gemini.Schema, schemaConfig gemini.SchemaConfig, s store.Store, pump *Pump, generators []*gemini.Generator, result chan Status, logger *zap.Logger) func() error {
	return func() error {
		job(t, f, concurrency, schema, schemaConfig, s, pump, generators, result, logger)
		return nil
	}
}

func entombJobs(t *tomb.Tomb, actors uint64, schema *gemini.Schema, schemaConfig gemini.SchemaConfig, s store.Store, pump *Pump, generators []*gemini.Generator, result chan Status, logger *zap.Logger, fs ...testJob) {
	t.Go(func() error {
		for _, f := range fs {
			t.Go(wrapJobInTombFunc(t, f, actors, schema, schemaConfig, s, pump, generators, result, logger))
		}
		return nil
	})
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

func createClusters(consistency gocql.Consistency) (*gocql.ClusterConfig, *gocql.ClusterConfig) {
	retryPolicy := &gocql.ExponentialBackoffRetryPolicy{
		Min:        time.Second,
		Max:        10 * time.Second,
		NumRetries: 5,
	}
	testCluster := gocql.NewCluster(testClusterHost...)
	testCluster.Timeout = 5 * time.Second
	testCluster.RetryPolicy = retryPolicy
	testCluster.Consistency = consistency
	if len(oracleClusterHost) == 0 {
		return testCluster, nil
	}
	oracleCluster := gocql.NewCluster(oracleClusterHost...)
	oracleCluster.Timeout = 5 * time.Second
	oracleCluster.RetryPolicy = retryPolicy
	oracleCluster.Consistency = consistency
	return testCluster, oracleCluster
}

func getCompactionStrategy(cs string, logger *zap.Logger) *gemini.CompactionStrategy {
	switch cs {
	case "stcs":
		return gemini.NewSizeTieredCompactionStrategy()
	case "twcs":
		return gemini.NewTimeWindowCompactionStrategy()
	case "lcs":
		return gemini.NewLeveledCompactionStrategy()
	case "":
		return nil
	default:
		compactionStrategy := &gemini.CompactionStrategy{}
		if err := json.Unmarshal([]byte(strings.ReplaceAll(cs, "'", "\"")), compactionStrategy); err != nil {
			logger.Error("unable to parse compaction strategy", zap.String("strategy", cs), zap.Error(err))
			return nil
		}
		return compactionStrategy
	}
}

func getReplicationStrategy(rs string, fallback *replication.Replication, logger *zap.Logger) *replication.Replication {
	switch rs {
	case "network":
		return replication.NewNetworkTopologyStrategy()
	case "simple":
		return replication.NewSimpleStrategy()
	default:
		replicationStrategy := &replication.Replication{}
		if err := json.Unmarshal([]byte(strings.ReplaceAll(rs, "'", "\"")), rs); err != nil {
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
	rootCmd.Flags().StringSliceVarP(&oracleClusterHost, "oracle-cluster", "o", []string{}, "Host names or IPs of the oracle cluster that provides correct answers. If omitted no oracle will be used")
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
	rootCmd.Flags().StringVarP(&compactionStrategy, "compaction-strategy", "", "", "Specify the desired CS as either the coded short hand stcs|twcs|lcs to get the default for each type or provide the entire specification in the form {'class':'....'}")
	rootCmd.Flags().StringVarP(&replicationStrategy, "replication-strategy", "", "simple", "Specify the desired replication strategy as either the coded short hand simple|network to get the default for each type or provide the entire specification in the form {'class':'....'}")
	rootCmd.Flags().StringVarP(&consistency, "consistency", "", "QUORUM", "Specify the desired consistency as ANY|ONE|TWO|THREE|QUORUM|LOCAL_QUORUM|EACH_QUORUM|LOCAL_ONE")
	rootCmd.Flags().IntVarP(&maxPartitionKeys, "max-partition-keys", "", 6, "Maximum number of generated partition keys")
	rootCmd.Flags().IntVarP(&minPartitionKeys, "min-partition-keys", "", 2, "Minimum number of generated partition keys")
	rootCmd.Flags().IntVarP(&maxClusteringKeys, "max-clustering-keys", "", 4, "Maximum number of generated clustering keys")
	rootCmd.Flags().IntVarP(&minClusteringKeys, "min-clustering-keys", "", 2, "Minimum number of generated clustering keys")
	rootCmd.Flags().IntVarP(&maxColumns, "max-columns", "", 16, "Maximum number of generated columns")
	rootCmd.Flags().IntVarP(&maxColumns, "min-columns", "", 8, "Minimum number of generated columns")
	rootCmd.Flags().StringVarP(&datasetSize, "dataset-size", "", "large", "Specify the type of dataset size to use, small|large")
	rootCmd.Flags().StringVarP(&cqlFeatures, "cql-features", "", "basic", "Specify the type of cql features to use, basic|normal|large")
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
