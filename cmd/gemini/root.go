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
	"context"
	"encoding/json"
	"fmt"
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

	"github.com/gocql/gocql"
	"github.com/gocql/gocql/hostpolicy"
	"github.com/hailocab/go-hostpool"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/gemini/pkg/builders"
	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/jobs"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/realrandom"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

var (
	rootCmd = &cobra.Command{
		Use:          "gemini",
		Short:        "Gemini is an automatic random testing tool for Scylla.",
		RunE:         run,
		SilenceUsage: true,
	}

	versionInfo VersionInfo
)

func init() {
	var err error

	versionInfo, err = NewVersionInfo()
	if err != nil {
		panic(err)
	}

	rootCmd.Version = versionInfo.String()

	setupFlags(rootCmd)
}

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

	schemaBuilder := builders.SchemaBuilder{}
	schemaBuilder.Keyspace(shm.Keyspace).Config(schemaConfig)
	for t, tbl := range shm.Tables {
		shm.Tables[t].LinkIndexAndColumns()
		schemaBuilder.Table(tbl)
	}
	return schemaBuilder.Build(), nil
}

//nolint:gocyclo
func run(cmd *cobra.Command, _ []string) error {
	ctx, cancel := signal.NotifyContext(
		cmd.Context(),
		syscall.SIGTERM,
		syscall.SIGABRT,
		syscall.SIGINT,
	)
	defer cancel()

	logger := createLogger(level)

	metrics.StartMetricsServer(ctx, bind, logger.Named("metrics"))

	val, err := cmd.PersistentFlags().GetBool("version-json")
	if err != nil {
		return err
	}

	if val {
		var data []byte
		data, err = json.MarshalIndent(versionInfo, "", "    ")
		if err != nil {
			return err
		}

		//nolint:forbidigo
		fmt.Println(string(data))
		return nil
	}

	globalStatus := status.NewGlobalStatus(maxErrorsToStore)
	defer utils.IgnoreError(logger.Sync)

	if err = validateSeed(seed); err != nil {
		return errors.Wrapf(err, "failed to parse --seed argument")
	}
	if err = validateSeed(schemaSeed); err != nil {
		return errors.Wrapf(err, "failed to parse --schema-seed argument")
	}

	intSeed := seedFromString(seed)
	intSchemaSeed := seedFromString(schemaSeed)

	if profilingPort != 0 {
		go func() {
			mux := http.NewServeMux()

			mux.HandleFunc("GET /debug/pprof/", pprof.Index)
			mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)

			log.Fatal(http.ListenAndServe("0.0.0.0:"+strconv.Itoa(profilingPort), mux))
		}()
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
		schema, err = readSchema(schemaFile, schemaConfig)
		if err != nil {
			return errors.Wrap(err, "cannot create schema")
		}
	} else {
		intSchemaSeed = seedFromString(schemaSeed)
		schema = generators.GenSchema(schemaConfig, intSchemaSeed)
	}

	testCluster, oracleCluster, err := createClusters(
		consistency,
		testClusterHostSelectionPolicy,
		oracleClusterHostSelectionPolicy,
	)
	if err != nil {
		return err
	}

	jsonSchema, _ := json.MarshalIndent(schema, "", "    ")

	printSetup(intSeed, intSchemaSeed)
	fmt.Printf("Schema: %v\n", string(jsonSchema)) //nolint:forbidigo

	storeConfig := store.Config{
		MaxRetriesMutate:            maxRetriesMutate,
		MaxRetriesMutateSleep:       maxRetriesMutateSleep,
		UseServerSideTimestamps:     useServerSideTimestamps,
		TestLogStatementsFile:       testStatementLogFile,
		OracleLogStatementsFile:     oracleStatementLogFile,
		LogStatementFileCompression: stmtlogger.MustParseCompression(statementLogFileCompression),
	}

	st, err := store.New(ctx, schema, testCluster, oracleCluster, storeConfig, logger)
	if err != nil {
		return err
	}

	defer utils.IgnoreError(st.Close)

	if dropSchema && mode != jobs.ReadMode {
		for _, stmt := range generators.GetDropKeyspace(schema) {
			logger.Debug(stmt)
			if err = st.Mutate(ctx, typedef.SimpleStmt(stmt, typedef.DropKeyspaceStatementType)); err != nil {
				return errors.Wrap(err, "unable to drop schema")
			}
		}
	}

	testKeyspace, oracleKeyspace := generators.GetCreateKeyspaces(schema)
	if err = st.Create(
		ctx,
		typedef.SimpleStmt(testKeyspace, typedef.CreateKeyspaceStatementType),
		typedef.SimpleStmt(oracleKeyspace, typedef.CreateKeyspaceStatementType)); err != nil {
		return errors.Wrap(err, "unable to create keyspace")
	}

	for _, stmt := range generators.GetCreateSchema(schema) {
		logger.Debug(stmt)
		if err = st.Mutate(ctx, typedef.SimpleStmt(stmt, typedef.CreateSchemaStatementType)); err != nil {
			return errors.Wrap(err, "unable to create schema")
		}
	}

	ctx, done := context.WithTimeout(ctx, duration+warmup+time.Second*2)
	defer done()
	stopFlag := stop.NewFlag("main")
	warmupStopFlag := stop.NewFlag("warmup")
	stop.StartOsSignalsTransmitter(logger, stopFlag, warmupStopFlag)

	randSrc, distFunc, err := distributions.New(
		partitionKeyDistribution,
		partitionCount,
		intSeed,
		stdDistMean,
		oneStdDev,
	)
	if err != nil {
		return errors.Wrapf(
			err,
			"Faile to create distribution function: %s",
			partitionKeyDistribution,
		)
	}

	gens := generators.New(
		ctx,
		schema,
		distFunc,
		intSeed,
		partitionCount,
		pkBufferReuseSize,
		logger,
		randSrc,
	)
	defer utils.IgnoreError(gens.Close)

	if warmup > 0 && !stopFlag.IsHardOrSoft() {
		jobsList := jobs.ListFromMode(jobs.WarmupMode, warmup, concurrency)
		if err = jobsList.Run(ctx, schema, schemaConfig, st, gens, globalStatus, logger, warmupStopFlag, failFast, verbose, randSrc); err != nil {
			logger.Error("warmup encountered an error", zap.Error(err))
			stopFlag.SetHard(true)
		}
	}

	if !stopFlag.IsHardOrSoft() {
		jobsList := jobs.ListFromMode(mode, duration, concurrency)
		if err = jobsList.Run(ctx, schema, schemaConfig, st, gens, globalStatus, logger, stopFlag.CreateChild("workload"), failFast, verbose, randSrc); err != nil {
			logger.Debug("error detected", zap.Error(err))
		}
	}

	logger.Info("test finished")
	globalStatus.PrintResult(outFile, schema, version, versionInfo)
	if globalStatus.HasErrors() {
		return errors.Errorf("gemini encountered errors, exiting with non zero status")
	}

	return nil
}

func createFile(fname string, def *os.File) (*os.File, error) {
	if fname == "" {
		return def, nil
	}

	f, err := os.Create(fname)
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to open output file %s", fname)
	}

	return f, nil
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

func createClusters(
	consistency, testSelectionPolicy, oracleSelectionPolicy string,
) (*gocql.ClusterConfig, *gocql.ClusterConfig, error) {
	for i := range len(testClusterHost) {
		testClusterHost[i] = strings.TrimSpace(testClusterHost[i])
	}

	c, err := gocql.ParseConsistencyWrapper(consistency)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to parse consistency %s", consistency)
	}

	testHostSelectionPolicy, err := getHostSelectionPolicy(testSelectionPolicy, testClusterHost)
	if err != nil {
		return nil, nil, err
	}

	oracleHostSelectionPolicy, err := getHostSelectionPolicy(
		oracleSelectionPolicy,
		oracleClusterHost,
	)
	if err != nil {
		return nil, nil, err
	}

	for i := range len(oracleClusterHost) {
		oracleClusterHost[i] = strings.TrimSpace(oracleClusterHost[i])
	}

	testCluster := gocql.NewCluster(testClusterHost...)
	testCluster.Timeout = requestTimeout
	testCluster.ConnectTimeout = connectTimeout
	testCluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		Min:        time.Second,
		Max:        60 * time.Second,
		NumRetries: 5,
	}

	testClusterObserver := store.ClusterObserver{
		ClusterName: "test_cluster",
	}

	testCluster.Consistency = c
	testCluster.DefaultTimestamp = !useServerSideTimestamps
	testCluster.PoolConfig.HostSelectionPolicy = testHostSelectionPolicy
	testCluster.ConnectObserver = testClusterObserver
	testCluster.QueryObserver = testClusterObserver
	testCluster.BatchObserver = testClusterObserver

	if testClusterUsername != "" && testClusterPassword != "" {
		testCluster.Authenticator = gocql.PasswordAuthenticator{
			Username: testClusterUsername,
			Password: testClusterPassword,
		}
	}

	if len(oracleClusterHost) == 0 {
		return testCluster, nil, nil
	}

	oracleClusterObserver := store.ClusterObserver{
		ClusterName: "oracle_cluster",
	}

	oracleCluster := gocql.NewCluster(oracleClusterHost...)
	oracleCluster.Timeout = requestTimeout
	oracleCluster.ConnectTimeout = connectTimeout
	oracleCluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		Min:        time.Second,
		Max:        60 * time.Second,
		NumRetries: 5,
	}
	oracleCluster.ConnectObserver = oracleClusterObserver
	oracleCluster.QueryObserver = oracleClusterObserver
	oracleCluster.BatchObserver = oracleClusterObserver
	oracleCluster.Consistency = c
	oracleCluster.DefaultTimestamp = !useServerSideTimestamps
	oracleCluster.PoolConfig.HostSelectionPolicy = oracleHostSelectionPolicy

	if oracleClusterUsername == "" || oracleClusterPassword == "" {
		oracleCluster.Authenticator = gocql.PasswordAuthenticator{
			Username: oracleClusterUsername,
			Password: oracleClusterPassword,
		}
	}

	return testCluster, oracleCluster, nil
}

func getReplicationStrategy(
	rs string,
	fallback replication.Replication,
	logger *zap.Logger,
) replication.Replication {
	switch rs {
	case "network":
		return replication.NewNetworkTopologyStrategy()
	case "simple":
		return replication.NewSimpleStrategy()
	default:
		strategy := replication.Replication{}
		if err := json.Unmarshal([]byte(strings.ReplaceAll(rs, "'", "\"")), &strategy); err != nil {
			logger.Error(
				"unable to parse replication strategy",
				zap.String("strategy", rs),
				zap.Error(err),
			)
			return fallback
		}
		return strategy
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
		return hostpolicy.HostPool(hostpool.New(hosts)), nil
	case "token-aware":
		return gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy()), nil
	default:
		return nil, fmt.Errorf("unknown host selection policy \"%s\"", policy)
	}
}

func printSetup(seed, schemaSeed uint64) {
	tw := new(tabwriter.Writer)
	tw.Init(os.Stdout, 0, 8, 2, '\t', tabwriter.AlignRight)
	_, _ = fmt.Fprintf(tw, "Seed:\t%d\n", seed)
	_, _ = fmt.Fprintf(tw, "Schema seed:\t%d\n", schemaSeed)
	_, _ = fmt.Fprintf(tw, "Maximum duration:\t%s\n", duration)
	_, _ = fmt.Fprintf(tw, "Warmup duration:\t%s\n", warmup)
	_, _ = fmt.Fprintf(tw, "Concurrency:\t%d\n", concurrency)
	_, _ = fmt.Fprintf(tw, "Test cluster:\t%s\n", testClusterHost)
	_, _ = fmt.Fprintf(tw, "Oracle cluster:\t%s\n", oracleClusterHost)
	if outFileArg == "" {
		_, _ = fmt.Fprintf(tw, "Output file:\t%s\n", "<stdout>")
	} else {
		_, _ = fmt.Fprintf(tw, "Output file:\t%s\n", outFileArg)
	}
	_ = tw.Flush()
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
