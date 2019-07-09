// Copyright (C) 2018 ScyllaDB

package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/briandowns/spinner"
	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/scylladb/gemini"
	"github.com/scylladb/gemini/store"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/rand"
	"golang.org/x/net/context"
)

var (
	testClusterHost       []string
	oracleClusterHost     []string
	schemaFile            string
	outFileArg            string
	concurrency           uint64
	seed                  uint64
	dropSchema            bool
	verbose               bool
	mode                  string
	failFast              bool
	nonInteractive        bool
	duration              time.Duration
	bind                  string
	warmup                time.Duration
	compactionStrategy    string
	consistency           string
	maxPartitionKeys      int
	maxClusteringKeys     int
	maxColumns            int
	datasetSize           string
	cqlFeatures           string
	level                 string
	maxRetriesMutate      int
	maxRetriesMutateSleep time.Duration
	pkBufferSize          uint64
	pkBufferReuseSize     uint64
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

type testJob func(context.Context, <-chan heartBeat, *sync.WaitGroup, *gemini.Schema, gemini.SchemaConfig, *gemini.Table, store.Store, *rand.Rand, gemini.PartitionRangeConfig, *gemini.Source, chan Status, string, time.Duration, *zap.Logger)

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

func run(cmd *cobra.Command, args []string) {
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
		logger.Error("unable to print setup", zap.Error(err))
		return
	}

	outFile := os.Stdout
	if outFileArg != "" {
		of, err := os.Create(outFileArg)
		if err != nil {
			logger.Error("Unable to open output file", zap.String("file", outFileArg), zap.Error(err))
			return
		}
		outFile = of
	}
	defer outFile.Sync()

	schemaConfig := createSchemaConfig(logger)
	var schema *gemini.Schema
	if len(schemaFile) > 0 {
		var err error
		schema, err = readSchema(schemaFile)
		if err != nil {
			logger.Error("cannot create schema", zap.Error(err))
			return
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
	store := store.New(schema, testCluster, oracleCluster, storeConfig, logger)
	defer store.Close()

	if dropSchema && mode != readMode {
		for _, stmt := range schema.GetDropSchema() {
			logger.Debug(stmt)
			if err := store.Mutate(context.Background(), createBuilder{stmt: stmt}); err != nil {
				logger.Error("unable to drop schema", zap.Error(err))
				return
			}
		}
	}
	for _, stmt := range schema.GetCreateSchema() {
		logger.Debug(stmt)
		if err := store.Mutate(context.Background(), createBuilder{stmt: stmt}); err != nil {
			logger.Error("unable to create schema", zap.Error(err))
			return
		}
	}

	done := &sync.WaitGroup{}
	done.Add(1)
	result := make(chan Status, 10000)
	pump := createPump(10000, logger)
	generators := createGenerators(schema, schemaConfig, concurrency)
	go func() {
		defer done.Done()
		var sp *spinner.Spinner = nil
		if interactive() {
			sp = createSpinner()
		}
		pump.Start(duration+warmup, createPumpCallback(result, sp))
		res := sampleStatus(pump, result, sp, logger)
		res.PrintResult(outFile)
		for _, g := range generators {
			g.Stop()
		}
	}()

	launch(schema, schemaConfig, store, pump, generators, result, logger)
	close(result)
	done.Wait()
}

func launch(schema *gemini.Schema, schemaConfig gemini.SchemaConfig, store store.Store, pump *Pump, generators []*gemini.Generators, result chan Status, logger *zap.Logger) {
	if warmup > 0 {
		done := &sync.WaitGroup{}
		done.Add(1)
		job(done, WarmupJob, concurrency, schema, schemaConfig, store, pump, generators, result, logger)
		done.Wait()
		logger.Info("Warmup done")
	}
	done := &sync.WaitGroup{}
	done.Add(1)
	switch mode {
	case writeMode:
		go job(done, MutationJob, concurrency, schema, schemaConfig, store, pump, generators, result, logger)
	case readMode:
		go job(done, ValidationJob, concurrency, schema, schemaConfig, store, pump, generators, result, logger)
	default:
		done.Add(1)
		go job(done, MutationJob, concurrency, schema, schemaConfig, store, pump, generators, result, logger)
		go job(done, ValidationJob, concurrency, schema, schemaConfig, store, pump, generators, result, logger)
	}
	done.Wait()
	logger.Info("All jobs complete")
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
	Use:   "gemini",
	Short: "Gemini is an automatic random testing tool for Scylla.",
	Run:   run,
}

func Execute() {
}

func init() {

	rootCmd.Version = version + ", commit " + commit + ", date " + date
	rootCmd.Flags().StringSliceVarP(&testClusterHost, "test-cluster", "t", []string{}, "Host names or IPs of the test cluster that is system under test")
	rootCmd.MarkFlagRequired("test-cluster")
	rootCmd.Flags().StringSliceVarP(&oracleClusterHost, "oracle-cluster", "o", []string{}, "Host names or IPs of the oracle cluster that provides correct answers")
	rootCmd.MarkFlagRequired("oracle-cluster")
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
	rootCmd.Flags().StringVarP(&consistency, "consistency", "", "QUORUM", "Specify the desired consistency as ANY|ONE|TWO|THREE|QUORUM|LOCAL_QUORUM|EACH_QUORUM|LOCAL_ONE")
	rootCmd.Flags().IntVarP(&maxPartitionKeys, "max-partition-keys", "", 2, "Maximum number of generated partition keys")
	rootCmd.Flags().IntVarP(&maxClusteringKeys, "max-clustering-keys", "", 4, "Maximum number of generated clustering keys")
	rootCmd.Flags().IntVarP(&maxColumns, "max-columns", "", 16, "Maximum number of generated columns")
	rootCmd.Flags().StringVarP(&datasetSize, "dataset-size", "", "large", "Specify the type of dataset size to use, small|large")
	rootCmd.Flags().StringVarP(&cqlFeatures, "cql-features", "", "basic", "Specify the type of cql features to use, basic|normal|large")
	rootCmd.Flags().StringVarP(&level, "level", "", "info", "Specify the logging level, debug|info|warn|error|dpanic|panic|fatal")
	rootCmd.Flags().IntVarP(&maxRetriesMutate, "max-mutation-retries", "", 2, "Maximum number of attempts to apply a mutation")
	rootCmd.Flags().DurationVarP(&maxRetriesMutateSleep, "max-mutation-retries-backoff", "", 10*time.Millisecond, "Duration between attempts to apply a mutation for example 10ms or 1s")
	rootCmd.Flags().Uint64VarP(&pkBufferSize, "partition-key-buffer-size", "", 1000, "Number of buffered partition keys")
	rootCmd.Flags().Uint64VarP(&pkBufferReuseSize, "partition-key-buffer-reuse-size", "", 2000, "Number of reused buffered partition keys")
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
