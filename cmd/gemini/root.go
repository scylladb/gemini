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

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/jobs"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/realrandom"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
	"github.com/scylladb/gemini/pkg/workpool"
)

const (
	WorkPoolSize = 2048
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

//nolint:gocyclo
func run(cmd *cobra.Command, _ []string) error {
	ctx, cancel := signal.NotifyContext(
		cmd.Context(),
		syscall.SIGTERM,
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

	outFile, err := utils.CreateFile(outFileArg, true, os.Stdout)
	if err != nil {
		return err
	}

	pool := workpool.New(WorkPoolSize)

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
			"Failed to create distribution function: %s",
			partitionKeyDistribution,
		)
	}

	schema, schemaConfig, err := getSchema(intSeed, logger)
	if err != nil {
		return errors.Wrap(err, "failed to get schema")
	}

	gens := generators.New(
		schema,
		distFunc,
		intSeed,
		partitionCount,
		pkBufferReuseSize,
		logger,
		randSrc,
	)
	utils.AddFinalizer(func() {
		logger.Info("closing generators")

		if err = gens.Close(); err != nil {
			logger.Error("failed to close generators", zap.Error(err))
		} else {
			logger.Info("generators closed")
		}
	})

	storeConfig := store.Config{
		MaxRetriesMutate:        maxRetriesMutate,
		MaxRetriesMutateSleep:   maxRetriesMutateSleep,
		UseServerSideTimestamps: useServerSideTimestamps,
		OracleStatementFile:     oracleStatementLogFile,
		TestStatementFile:       testStatementLogFile,
		Compression:             stmtlogger.NoCompression,
		TestClusterConfig: store.ScyllaClusterConfig{
			Name:                    stmtlogger.TypeTest,
			Hosts:                   testClusterHost,
			HostSelectionPolicy:     testClusterHostSelectionPolicy,
			Consistency:             consistency,
			RequestTimeout:          requestTimeout,
			ConnectTimeout:          connectTimeout,
			UseServerSideTimestamps: useServerSideTimestamps,
			Username:                testClusterUsername,
			Password:                testClusterPassword,
		},
	}

	if len(oracleClusterHost) > 0 {
		storeConfig.OracleClusterConfig = &store.ScyllaClusterConfig{
			Name:                    stmtlogger.TypeOracle,
			Hosts:                   oracleClusterHost,
			HostSelectionPolicy:     oracleClusterHostSelectionPolicy,
			Consistency:             consistency,
			RequestTimeout:          requestTimeout,
			ConnectTimeout:          connectTimeout,
			UseServerSideTimestamps: useServerSideTimestamps,
			Username:                oracleClusterUsername,
			Password:                oracleClusterPassword,
		}
	}

	st, err := store.New(gens.Gens[schema.Tables[0].Name].Get(ctx).Value, pool, schema, storeConfig, logger.Named("store"), globalStatus.Errors)
	if err != nil {
		return err
	}

	utils.AddFinalizer(func() {
		logger.Info("closing store")

		if err = st.Close(); err != nil {
			logger.Error("failed to close store", zap.Error(err))
		} else {
			logger.Info("store closed")
		}
	})

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

	stopFlag := stop.NewFlag("main")
	warmupStopFlag := stop.NewFlag("warmup")
	stop.StartOsSignalsTransmitter(logger, stopFlag, warmupStopFlag)

	if warmup > 0 && !stopFlag.IsHardOrSoft() {
		jobsList := jobs.ListFromMode(jobs.WarmupMode, warmup, concurrency)
		time.AfterFunc(warmup, func() {
			warmupStopFlag.SetSoft(true)
		})
		if err = jobsList.Run(ctx, schema, schemaConfig, st, gens, globalStatus, logger, warmupStopFlag, failFast, verbose, randSrc); err != nil {
			logger.Error("warmup encountered an error", zap.Error(err))
			stopFlag.SetHard(true)
		}
	}

	if !stopFlag.IsHardOrSoft() {
		jobsList := jobs.ListFromMode(mode, duration, concurrency)
		time.AfterFunc(duration, func() {
			stopFlag.SetHard(true)
		})
		if err = jobsList.Run(ctx, schema, schemaConfig, st, gens, globalStatus, logger, stopFlag, failFast, verbose, randSrc); err != nil {
			logger.Debug("error detected", zap.Error(err))
			stopFlag.SetHard(true)
		}
	}

	globalStatus.PrintResult(outFile, schema, version, versionInfo)
	if globalStatus.HasErrors() {
		return errors.New("gemini encountered errors, exiting with non zero status")
	}

	logger.Info("test finished")
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

	file, err := utils.CreateFile("gemini.log", false, os.Stdout)
	if err != nil {
		log.Fatalf("failed to create log file: %v", err)
	}

	encoderCfg := zap.NewProductionEncoderConfig()
	encoderCfg.EncodeTime = zapcore.RFC3339NanoTimeEncoder
	encoderCfg.EncodeDuration = zapcore.StringDurationEncoder
	encoderCfg.EncodeLevel = zapcore.LowercaseLevelEncoder
	encoderCfg.EncodeCaller = zapcore.FullCallerEncoder

	logger := zap.New(zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderCfg),
		zapcore.NewMultiWriteSyncer(zapcore.Lock(file.(zapcore.WriteSyncer)), zapcore.Lock(os.Stdout)),
		lvl,
	))
	return logger
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
