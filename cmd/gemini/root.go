// Copyright (C) 2018 ScyllaDB

package main

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/briandowns/spinner"
	"github.com/gocql/gocql"
	"github.com/pkg/errors"
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

type Status struct {
	WriteOps    int        `json:"write_ops"`
	WriteErrors int        `json:"write_errors"`
	ReadOps     int        `json:"read_ops"`
	ReadErrors  int        `json:"read_errors"`
	Errors      []JobError `json:"errors,omitempty"`
}

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

type testJob func(context.Context, <-chan heartBeat, *sync.WaitGroup, *gemini.Schema, gemini.SchemaConfig, *gemini.Table, store.Store, *rand.Rand, gemini.PartitionRangeConfig, <-chan gemini.Value, chan gemini.Value, chan Status, string, time.Duration, *zap.Logger)

func (r *Status) Merge(sum *Status) Status {
	sum.WriteOps += r.WriteOps
	sum.WriteErrors += r.WriteErrors
	sum.ReadOps += r.ReadOps
	sum.ReadErrors += r.ReadErrors
	sum.Errors = append(sum.Errors, r.Errors...)
	return *sum
}

func (r *Status) PrintResult(w io.Writer) {
	if err := r.PrintResultAsJSON(w); err != nil {
		// In case there has been it has been a long run we want to display it anyway...
		fmt.Printf("Unable to print result as json, using plain text to stdout, error=%s\n", err)
		fmt.Printf("Gemini version: %s\n", version)
		fmt.Printf("Results:\n")
		fmt.Printf("\twrite ops:    %v\n", r.WriteOps)
		fmt.Printf("\tread ops:     %v\n", r.ReadOps)
		fmt.Printf("\twrite errors: %v\n", r.WriteErrors)
		fmt.Printf("\tread errors:  %v\n", r.ReadErrors)
		for i, err := range r.Errors {
			fmt.Printf("Error %d: %s\n", i, err)
		}
	}
}

func (r *Status) PrintResultAsJSON(w io.Writer) error {
	result := map[string]interface{}{
		"result":         r,
		"gemini_version": version,
	}
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent(" ", " ")
	if err := encoder.Encode(result); err != nil {
		return errors.Wrap(err, "unable to create json from result")
	}
	return nil
}

func (r Status) String() string {
	return fmt.Sprintf("write ops: %v | read ops: %v | write errors: %v | read errors: %v", r.WriteOps, r.ReadOps, r.WriteErrors, r.ReadErrors)
}

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

	runJob(Job, schema, schemaConfig, store, mode, outFile, logger)
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

func createSchemaConfig(logger *zap.Logger) gemini.SchemaConfig {
	defaultConfig := createDefaultSchemaConfig(logger)
	switch strings.ToLower(datasetSize) {
	case "small":
		return gemini.SchemaConfig{
			CompactionStrategy: defaultConfig.CompactionStrategy,
			MaxPartitionKeys:   defaultConfig.MaxPartitionKeys,
			MaxClusteringKeys:  defaultConfig.MaxClusteringKeys,
			MaxColumns:         defaultConfig.MaxColumns,
			MaxUDTParts:        2,
			MaxTupleParts:      2,
			MaxBlobLength:      20,
			MaxStringLength:    20,
			CQLFeature:         defaultConfig.CQLFeature,
		}
	default:
		return defaultConfig
	}
}

func createDefaultSchemaConfig(logger *zap.Logger) gemini.SchemaConfig {
	const (
		MaxBlobLength   = 1e4
		MinBlobLength   = 0
		MaxStringLength = 1000
		MinStringLength = 0
		MaxTupleParts   = 20
		MaxUDTParts     = 20
	)
	return gemini.SchemaConfig{
		CompactionStrategy: getCompactionStrategy(compactionStrategy, logger),
		MaxPartitionKeys:   3,
		MaxClusteringKeys:  maxClusteringKeys,
		MaxColumns:         maxColumns,
		MaxUDTParts:        MaxUDTParts,
		MaxTupleParts:      MaxTupleParts,
		MaxBlobLength:      MaxBlobLength,
		MinBlobLength:      MinBlobLength,
		MaxStringLength:    MaxStringLength,
		MinStringLength:    MinStringLength,
		CQLFeature:         getCQLFeature(cqlFeatures),
	}
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

func runJob(f testJob, schema *gemini.Schema, schemaConfig gemini.SchemaConfig, s store.Store, mode string, out *os.File, logger *zap.Logger) {
	defer out.Sync()
	logger = logger.Named("run_job")
	c := make(chan Status, 10000)

	// Wait group for the worker goroutines.
	var workers sync.WaitGroup
	workerCtx, _ := context.WithCancel(context.Background())
	workers.Add(len(schema.Tables) * int(concurrency))

	// Wait group for the finished goroutine.
	var finished sync.WaitGroup
	finished.Add(1)

	pump := createPump(10000, duration+warmup, logger)

	partitionRangeConfig := gemini.PartitionRangeConfig{
		MaxBlobLength:   schemaConfig.MaxBlobLength,
		MinBlobLength:   schemaConfig.MinBlobLength,
		MaxStringLength: schemaConfig.MaxStringLength,
		MinStringLength: schemaConfig.MinStringLength,
	}

	var gs []*gemini.Generators
	for _, table := range schema.Tables {
		gCfg := &gemini.GeneratorsConfig{
			Table:            table,
			Partitions:       partitionRangeConfig,
			Size:             concurrency,
			Seed:             seed,
			PkBufferSize:     pkBufferSize,
			PkUsedBufferSize: pkBufferReuseSize,
		}
		g := gemini.NewGenerator(gCfg)
		gs = append(gs, g)
		for i := 0; i < int(concurrency); i++ {
			r := rand.New(rand.NewSource(seed))
			go f(workerCtx, pump.ch, &workers, schema, schemaConfig, table, s, r, partitionRangeConfig, g.GetNew(i), g.GetOld(i), c, mode, warmup, logger)
		}
	}

	go func() {
		var sp *spinner.Spinner = nil
		if interactive() {
			sp = createSpinner()
		}
		pump.Start(createPumpCallback(c, &workers, sp))
		res := sampleResults(pump, c, sp, logger)
		res.PrintResult(out)
		for _, g := range gs {
			g.Stop()
		}
		finished.Done()
	}()

	finished.Wait()
}

func sampleResults(p *Pump, c chan Status, sp *spinner.Spinner, logger *zap.Logger) Status {
	logger = logger.Named("sample_results")
	var testRes Status
	done := false
	for res := range c {
		testRes = res.Merge(&testRes)
		if sp != nil {
			sp.Suffix = fmt.Sprintf(" Running Gemini... %v", testRes)
		}
		if testRes.ReadErrors > 0 || testRes.WriteErrors > 0 {
			if failFast {
				if !done {
					done = true
					logger.Warn("Errors detected. Exiting.")
					p.Stop()
				}
			}
		}
	}
	return testRes
}

func ddlJob(ctx context.Context, schema *gemini.Schema, sc *gemini.SchemaConfig, table *gemini.Table, s store.Store, r *rand.Rand, p gemini.PartitionRangeConfig, testStatus *Status, logger *zap.Logger) {
	if sc.CQLFeature != gemini.CQL_FEATURE_ALL {
		logger.Debug("ddl statements disabled")
		return
	}
	table.Lock()
	defer table.Unlock()
	ddlStmts, postStmtHook, err := schema.GenDDLStmt(table, r, p, sc)
	if err != nil {
		logger.Error("Failed! Mutation statement generation failed", zap.Error(err))
		testStatus.WriteErrors++
		return
	}
	if ddlStmts == nil {
		if w := logger.Check(zap.DebugLevel, "no statement generated"); w != nil {
			w.Write(zap.String("job", "ddl"))
		}
		return
	}
	defer postStmtHook()
	defer func() {
		if verbose {
			jsonSchema, _ := json.MarshalIndent(schema, "", "    ")
			fmt.Printf("Schema: %v\n", string(jsonSchema))
		}
	}()
	for _, ddlStmt := range ddlStmts {
		ddlQuery := ddlStmt.Query
		if w := logger.Check(zap.DebugLevel, "ddl statement"); w != nil {
			w.Write(zap.String("pretty_cql", ddlStmt.PrettyCQL()))
		}
		if err := s.Mutate(ctx, ddlQuery); err != nil {
			e := JobError{
				Timestamp: time.Now(),
				Message:   "DDL failed: " + err.Error(),
				Query:     ddlStmt.PrettyCQL(),
			}
			testStatus.Errors = append(testStatus.Errors, e)
			testStatus.WriteErrors++
		} else {
			testStatus.WriteOps++
		}
	}
}

func mutationJob(ctx context.Context, schema *gemini.Schema, _ *gemini.SchemaConfig, table *gemini.Table, s store.Store, r *rand.Rand, p gemini.PartitionRangeConfig, newPartitionValues <-chan gemini.Value, oldPartitionValues chan gemini.Value, testStatus *Status, deletes bool, logger *zap.Logger) {
	mutateStmt, err := schema.GenMutateStmt(table, newPartitionValues, oldPartitionValues, r, p, deletes)
	if err != nil {
		logger.Error("Failed! Mutation statement generation failed", zap.Error(err))
		testStatus.WriteErrors++
		return
	}
	if mutateStmt == nil {
		if w := logger.Check(zap.DebugLevel, "no statement generated"); w != nil {
			w.Write(zap.String("job", "mutation"))
		}
		return
	}
	mutateQuery := mutateStmt.Query
	mutateValues := mutateStmt.Values()
	if w := logger.Check(zap.DebugLevel, "validation statement"); w != nil {
		w.Write(zap.String("pretty_cql", mutateStmt.PrettyCQL()))
	}
	if err := s.Mutate(ctx, mutateQuery, mutateValues...); err != nil {
		e := JobError{
			Timestamp: time.Now(),
			Message:   "Mutation failed: " + err.Error(),
			Query:     mutateStmt.PrettyCQL(),
		}
		testStatus.Errors = append(testStatus.Errors, e)
		testStatus.WriteErrors++
	} else {
		testStatus.WriteOps++
	}
}

func validationJob(ctx context.Context, schema *gemini.Schema, _ *gemini.SchemaConfig, table *gemini.Table, s store.Store, r *rand.Rand, p gemini.PartitionRangeConfig, partitionValues <-chan gemini.Value, testStatus *Status, logger *zap.Logger) {
	checkStmt := schema.GenCheckStmt(table, partitionValues, r, p)
	if checkStmt == nil {
		if w := logger.Check(zap.DebugLevel, "no statement generated"); w != nil {
			w.Write(zap.String("job", "validation"))
		}
		return
	}
	checkQuery := checkStmt.Query
	checkValues := checkStmt.Values()
	if w := logger.Check(zap.DebugLevel, "validation statement"); w != nil {
		w.Write(zap.String("pretty_cql", checkStmt.PrettyCQL()))
	}
	if err := s.Check(ctx, table, checkQuery, checkValues...); err != nil {
		// De-duplication needed?
		e := JobError{
			Timestamp: time.Now(),
			Message:   "Validation failed: " + err.Error(),
			Query:     checkStmt.PrettyCQL(),
		}
		testStatus.Errors = append(testStatus.Errors, e)
		testStatus.ReadErrors++
	} else {
		testStatus.ReadOps++
	}
}

type heartBeat struct {
	sleep time.Duration
}

func (hb heartBeat) await() {
	if hb.sleep > 0 {
		time.Sleep(hb.sleep)
	}
}
func Job(ctx context.Context, pump <-chan heartBeat, wg *sync.WaitGroup, schema *gemini.Schema, schemaCfg gemini.SchemaConfig, table *gemini.Table, s store.Store, r *rand.Rand, p gemini.PartitionRangeConfig, newValues <-chan gemini.Value, oldValues chan gemini.Value, c chan Status, mode string, warmup time.Duration, logger *zap.Logger) {
	defer wg.Done()

	schemaConfig := &schemaCfg
	mutationLogger := logger.Named("mutation_job")
	validationLogger := logger.Named("validation_job")
	ddlLogger := logger.Named("ddl_job")

	testStatus := Status{}
	var i int
	warmupTimer := time.NewTimer(warmup)
warmup:
	for {
		select {
		case <-ctx.Done():
			return
		case <-warmupTimer.C:
			break warmup
		default:
			mutationJob(ctx, schema, schemaConfig, table, s, r, p, newValues, oldValues, &testStatus, false, mutationLogger)
			if i%1000 == 0 {
				c <- testStatus
				testStatus = Status{}
			}
		}
	}

	for hb := range pump {
		hb.await()
		switch mode {
		case writeMode:
			mutationJob(ctx, schema, schemaConfig, table, s, r, p, newValues, oldValues, &testStatus, true, mutationLogger)
		case readMode:
			validationJob(ctx, schema, schemaConfig, table, s, r, p, oldValues, &testStatus, validationLogger)
		default:
			ind := r.Intn(1000000)
			if ind%2 == 0 {
				if ind%100000 == 0 {
					ddlJob(ctx, schema, schemaConfig, table, s, r, p, &testStatus, ddlLogger)
				} else {
					mutationJob(ctx, schema, schemaConfig, table, s, r, p, newValues, oldValues, &testStatus, true, mutationLogger)
				}
			} else {
				validationJob(ctx, schema, schemaConfig, table, s, r, p, oldValues, &testStatus, validationLogger)
			}
		}

		if i%1000 == 0 {
			c <- testStatus
			testStatus = Status{}
		}
		if failFast && (testStatus.ReadErrors > 0 || testStatus.WriteErrors > 0) {
			break
		}
		i++
	}
	logger.Debug("pump closed")
	c <- testStatus
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

func createSpinner() *spinner.Spinner {
	spinnerCharSet := []string{"|", "/", "-", "\\"}
	sp := spinner.New(spinnerCharSet, 1*time.Second)
	_ = sp.Color("black")
	sp.Start()
	return sp
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
