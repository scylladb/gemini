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
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/random"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/schema"
	"github.com/scylladb/gemini/pkg/services"
	"github.com/scylladb/gemini/pkg/statements"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/tableopts"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

const (
	stdDistMean = math.MaxUint64 / 2
	oneStdDev   = 0.341 * math.MaxUint64
)

var rootCmd = &cobra.Command{
	Use:              "gemini",
	Short:            "Gemini is an automatic random testing tool for Scylla.",
	RunE:             run,
	PersistentPreRun: preRun,
	SilenceUsage:     true,
}

func init() {
	setupFlags(rootCmd)

	rootCmd.AddCommand(Benchmark())
}

func preRun(cmd *cobra.Command, _ []string) {
	metrics.StartMetricsServer(cmd.Context(), metricsPort)

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
}

func checkVersion(cmd *cobra.Command) (bool, error) {
	versionJSON, err := cmd.PersistentFlags().GetBool("version-json")

	if versionFlag || versionJSON {
		if err != nil {
			return false, err
		}

		var versionInfo VersionInfo

		versionInfo, err = NewVersionInfo()
		if err != nil {
			return false, err
		}

		cmd.Version = versionInfo.String()

		if versionJSON {
			var data []byte
			data, err = json.Marshal(versionInfo)
			if err != nil {
				return false, err
			}

			//nolint:forbidigo
			fmt.Println(string(data))
			return true, nil
		}

		//nolint:forbidigo
		fmt.Println(versionInfo.String())

		return true, nil
	}

	return false, nil
}

//nolint:gocyclo
func run(cmd *cobra.Command, _ []string) error {
	shouldAbort, err := checkVersion(cmd)
	if err != nil {
		return err
	}

	if shouldAbort {
		return nil
	}

	logger := createLogger(level)
	defer utils.IgnoreError(logger.Sync)

	stopFlag := stop.NewFlag("main")
	stop.StartOsSignalsTransmitter(logger, stopFlag)

	if mutationConcurrency == 0 {
		mutationConcurrency = concurrency
	}

	if readConcurrency == 0 {
		readConcurrency = concurrency
	}

	for i := range len(testClusterHost) {
		testClusterHost[i] = strings.TrimSpace(testClusterHost[i])
	}

	for i := range len(oracleClusterHost) {
		oracleClusterHost[i] = strings.TrimSpace(oracleClusterHost[i])
	}

	if err = validateSeed(seed); err != nil {
		return errors.Wrapf(err, "failed to parse --seed argument")
	}
	if err = validateSeed(schemaSeed); err != nil {
		return errors.Wrapf(err, "failed to parse --schema-seed argument")
	}

	statementRatio, err := parseStatementRatiosJSON(statementRatios)
	if err != nil {
		return errors.Wrapf(err, "failed to parse statement ratios JSON")
	}

	intSeed := seedFromString(seed)
	sc, err := schema.Get(
		createSchemaConfig(datasetSize, logger),
		schemaSeed,
		schemaFile,
	)
	if err != nil {
		return errors.Wrap(err, "failed to get schema")
	}

	storeConfig := store.Config{
		MaxRetriesMutate:                 maxRetriesMutate,
		MaxRetriesMutateSleep:            maxRetriesMutateSleep,
		AsyncObjectStabilizationAttempts: asyncObjectStabilizationAttempts,
		AsyncObjectStabilizationDelay:    asyncObjectStabilizationDelay,
		UseServerSideTimestamps:          useServerSideTimestamps,
		OracleStatementFile:              oracleStatementLogFile,
		TestStatementFile:                testStatementLogFile,
		Compression:                      stmtlogger.CompressionNone,
		TestClusterConfig: store.ScyllaClusterConfig{
			Name:                    stmtlogger.TypeTest,
			Hosts:                   testClusterHost,
			HostSelectionPolicy:     store.HostSelectionPolicy(testClusterHostSelectionPolicy),
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
			HostSelectionPolicy:     store.HostSelectionPolicy(oracleClusterHostSelectionPolicy),
			Consistency:             consistency,
			RequestTimeout:          requestTimeout,
			ConnectTimeout:          connectTimeout,
			UseServerSideTimestamps: useServerSideTimestamps,
			Username:                oracleClusterUsername,
			Password:                oracleClusterPassword,
		}
	}

	workload, err := services.NewWorkload(&services.WorkloadConfig{
		RandomStringBuffer:    randomStringBuffer,
		MaxErrorsToStore:      maxErrorsToStore,
		OutputFile:            outFileArg,
		PartitionDistribution: distributions.Distribution(partitionKeyDistribution),
		PartitionCount:        partitionCount,
		IOWorkerPoolSize:      iOWorkerPool,
		Seed:                  intSeed,
		MU:                    normalDistMean,
		Sigma:                 normalDistSigma,
		WarmupDuration:        warmup,
		Duration:              duration,
		RunningMode:           mode,
		MutationConcurrency:   mutationConcurrency,
		ReadConcurrency:       readConcurrency,
		DropSchema:            dropSchema,
		StatementRatios:       statementRatio,
	}, storeConfig, sc, logger, stopFlag)
	if err != nil {
		return err
	}

	defer func() {
		if err = workload.Close(); err != nil {
			logger.Error("failed to close workload",
				zap.Error(err),
				zap.String("mode", mode),
				zap.String("seed", seed),
				zap.String("schemaSeed", schemaSeed),
			)
		}
	}()

	printSetup(sc, statementRatio, intSeed, seedFromString(schemaSeed))

	if err = workload.Run(cmd.Context()); err != nil {
		logger.Error("failed to run gemini workload", zap.Error(err))
	}

	return workload.PrintResults(version)
}

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
	encoderCfg.EncodeCaller = nil

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
		return typedef.CQLFeatureAll
	case "normal":
		return typedef.CQLFeatureNormal
	default:
		return typedef.CQLFeatureBasic
	}
}

func printSetup(schema *typedef.Schema, ratio statements.Ratios, seed, schemaSeed uint64) {
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

	jsonSchema, _ := json.MarshalIndent(schema, "", "    ")
	fmt.Printf("Schema: %v\n", string(jsonSchema)) //nolint:forbidigo

	jsonStatementsRatio, _ := json.MarshalIndent(ratio, "", "    ")
	fmt.Printf("Statement Ratios: %v\n", string(jsonStatementsRatio)) //nolint:forbidigo
}

func RealRandom() uint64 {
	return rand.New(random.Source).Uint64()
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

func createDefaultSchemaConfig(logger *zap.Logger) typedef.SchemaConfig {
	rs := schema.GetReplicationStrategy(replicationStrategy, replication.NewNetworkTopologyStrategy(), logger)
	ors := schema.GetReplicationStrategy(oracleReplicationStrategy, rs, logger)

	deletedBuckets := make([]time.Duration, 0, len(deletedPartitionsTimeBucket))
	for _, bucket := range deletedPartitionsTimeBucket {
		dur, err := time.ParseDuration(bucket)
		if err != nil {
			logger.Fatal("failed to parse deleted partitions time bucket", zap.Error(err))
		}

		deletedBuckets = append(deletedBuckets, dur)
	}

	return typedef.SchemaConfig{
		ReplicationStrategy:              rs,
		OracleReplicationStrategy:        ors,
		TableOptions:                     tableopts.CreateTableOptions(tableOptions, logger),
		MaxTables:                        maxTables,
		MaxPartitionKeys:                 maxPartitionKeys,
		MinPartitionKeys:                 minPartitionKeys,
		MaxClusteringKeys:                maxClusteringKeys,
		MinClusteringKeys:                minClusteringKeys,
		MaxColumns:                       maxColumns,
		MinColumns:                       minColumns,
		MaxUDTParts:                      schema.MaxUDTParts,
		MaxTupleParts:                    schema.MaxTupleParts,
		MaxBlobLength:                    schema.MaxBlobLength,
		MinBlobLength:                    schema.MinBlobLength,
		MaxStringLength:                  schema.MaxStringLength,
		MinStringLength:                  schema.MinStringLength,
		UseCounters:                      useCounters,
		UseLWT:                           useLWT,
		CQLFeature:                       getCQLFeature(cqlFeatures),
		UseMaterializedViews:             useMaterializedViews,
		AsyncObjectStabilizationAttempts: asyncObjectStabilizationAttempts,
		AsyncObjectStabilizationDelay:    asyncObjectStabilizationDelay,
		DeleteBuckets:                    deletedBuckets,
	}
}

func createSchemaConfig(datasetSize string, logger *zap.Logger) typedef.SchemaConfig {
	defaultConfig := createDefaultSchemaConfig(logger)
	switch strings.ToLower(datasetSize) {
	case "small":
		return typedef.SchemaConfig{
			ReplicationStrategy:              defaultConfig.ReplicationStrategy,
			OracleReplicationStrategy:        defaultConfig.OracleReplicationStrategy,
			TableOptions:                     defaultConfig.TableOptions,
			MaxTables:                        defaultConfig.MaxTables,
			MaxPartitionKeys:                 defaultConfig.MaxPartitionKeys,
			MinPartitionKeys:                 defaultConfig.MinPartitionKeys,
			MaxClusteringKeys:                defaultConfig.MaxClusteringKeys,
			MinClusteringKeys:                defaultConfig.MinClusteringKeys,
			MaxColumns:                       defaultConfig.MaxColumns,
			MinColumns:                       defaultConfig.MinColumns,
			MaxUDTParts:                      2,
			MaxTupleParts:                    2,
			MaxBlobLength:                    20,
			MaxStringLength:                  20,
			UseCounters:                      defaultConfig.UseCounters,
			UseLWT:                           defaultConfig.UseLWT,
			CQLFeature:                       defaultConfig.CQLFeature,
			AsyncObjectStabilizationAttempts: defaultConfig.AsyncObjectStabilizationAttempts,
			UseMaterializedViews:             defaultConfig.UseMaterializedViews,
			AsyncObjectStabilizationDelay:    defaultConfig.AsyncObjectStabilizationDelay,
			MinBlobLength:                    10,
			MinStringLength:                  10,
			DeleteBuckets:                    defaultConfig.DeleteBuckets,
		}
	default:
		return defaultConfig
	}
}
