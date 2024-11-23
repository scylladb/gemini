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
	"fmt"
	"log"
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
	"github.com/hailocab/go-hostpool"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"golang.org/x/exp/rand"
	"golang.org/x/net/context"

	"github.com/scylladb/gemini/pkg/auth"
	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/jobs"
	"github.com/scylladb/gemini/pkg/realrandom"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"

	"github.com/scylladb/gemini/pkg/status"
)

func interactive() bool {
	return !nonInteractive
}

func run(_ *cobra.Command, _ []string) error {
	start := time.Now()

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

	rand.Seed(intSeed)

	cons := gocql.ParseConsistency(consistency)

	testHostSelectionPolicy, err := getHostSelectionPolicy(testClusterHostSelectionPolicy, testClusterHost)
	if err != nil {
		return err
	}
	oracleHostSelectionPolicy, err := getHostSelectionPolicy(oracleClusterHostSelectionPolicy, oracleClusterHost)
	if err != nil {
		return err
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGABRT, syscall.SIGTERM)
	defer cancel()

	go func() {
		mux := http.NewServeMux()
		mux.Handle("GET /metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(bind, mux))
	}()

	if profilingPort != 0 {
		go func() {
			mux := http.NewServeMux()
			mux.HandleFunc("GET /debug/pprof/", pprof.Index)
			mux.HandleFunc("GET /debug/pprof/cmdline", pprof.Cmdline)
			mux.HandleFunc("GET /debug/pprof/profile", pprof.Profile)
			mux.HandleFunc("GET /debug/pprof/symbol", pprof.Symbol)
			mux.HandleFunc("GET /debug/pprof/trace", pprof.Trace)
			log.Fatal(http.ListenAndServe(":"+strconv.Itoa(profilingPort), mux))
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
		schema, intSchemaSeed, err = generateSchema(logger, schemaConfig, schemaSeed)
		if err != nil {
			return errors.Wrapf(err, "failed to create schema for seed %s", schemaSeed)
		}
	}

	printSetup(intSeed, intSchemaSeed, mode)

	testCluster, oracleCluster := createClusters(cons, testHostSelectionPolicy, oracleHostSelectionPolicy, logger)
	storeConfig := store.Config{
		MaxRetriesMutate:        maxRetriesMutate,
		MaxRetriesMutateSleep:   maxRetriesMutateSleep,
		UseServerSideTimestamps: useServerSideTimestamps,
		TestLogStatementsFile:   testStatementLogFile,
		OracleLogStatementsFile: oracleStatementLogFile,
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
			defer func() {
				utils.IgnoreError(tracingFile.Sync)
				utils.IgnoreError(tracingFile.Close)
			}()
		}
	}
	st, err := store.New(schema, testCluster, oracleCluster, storeConfig, tracingFile, logger)
	if err != nil {
		return err
	}
	defer utils.IgnoreError(st.Close)

	if dropSchema && mode != jobs.ReadMode {
		for _, stmt := range generators.GetDropKeyspace(schema) {
			logger.Debug(stmt)
			if err = st.Mutate(context.Background(), typedef.SimpleStmt(stmt, typedef.DropKeyspaceStatementType)); err != nil {
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

	for _, stmt := range generators.GetCreateSchema(schema, useMaterializedViews) {
		logger.Debug(stmt)
		if err = st.Mutate(ctx, typedef.SimpleStmt(stmt, typedef.CreateSchemaStatementType)); err != nil {
			return errors.Wrap(err, "unable to create schema")
		}
	}

	distFunc, err := generators.ParseDistributionDefault(partitionKeyDistribution, partitionCount, intSeed)
	if err != nil {
		return err
	}

	gens, err := generators.New(ctx, schema, schemaConfig, intSeed, partitionCount, logger, distFunc, pkBufferReuseSize)
	if err != nil {
		return err
	}

	defer utils.IgnoreError(gens.Close)

	ctx, done := context.WithTimeout(ctx, duration+warmup+10*time.Second)
	defer done()

	if !nonInteractive {
		sp := createSpinner(interactive())
		ticker := time.NewTicker(time.Second)

		go func() {
			defer ticker.Stop()

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

	jobsList := jobs.New(mode, duration, concurrency, logger, schema, st, globalStatus, intSeed, gens, warmup, failFast)
	if err = jobsList.Run(ctx); err != nil {
		logger.Error("error detected", zap.Error(err))
	}

	logger.Info("test finished")
	globalStatus.PrintResult(outFile, schema, version, start)

	if globalStatus.HasErrors() {
		return errors.Errorf("gemini encountered errors, exiting with non zero status")
	}

	return nil
}

func createFile(name string, def *os.File) (*os.File, error) {
	if name != "" {
		f, err := os.Create(name)
		if err != nil {
			return nil, errors.Wrapf(err, "Unable to open output file %s", name)
		}

		return f, nil
	}

	return def, nil
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

func getHostSelectionPolicy(policy string, hosts []string) (gocql.HostSelectionPolicy, error) {
	switch strings.ToLower(policy) {
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

func printSetup(seed, schemaSeed uint64, mode string) {
	tw := new(tabwriter.Writer)
	tw.Init(os.Stdout, 0, 8, 2, '\t', tabwriter.AlignRight)
	fmt.Fprintf(tw, "Seed:\t%d\n", seed)
	fmt.Fprintf(tw, "Schema seed:\t%d\n", schemaSeed)
	fmt.Fprintf(tw, "Maximum duration:\t%s\n", duration)
	fmt.Fprintf(tw, "Warmup duration:\t%s\n", warmup)
	fmt.Fprintf(tw, "Concurrency:\t%d\n", concurrency)
	fmt.Fprintf(tw, "Test cluster:\t%s\n", testClusterHost)
	fmt.Fprintf(tw, "Oracle cluster:\t%s\n", oracleClusterHost)
	fmt.Fprintf(tw, "Mode:\t%s\n", mode)
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
