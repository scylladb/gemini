// Copyright 2025 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package services

import (
	"context"
	"math/rand/v2"
	"os"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/jobs"
	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/statements"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
	"github.com/scylladb/gemini/pkg/workpool"
)

type (
	WorkloadConfig struct {
		PartitionDistribution distributions.Distribution
		RunningMode           string
		OutputFile            string
		MU                    float64
		Seed                  uint64
		IOWorkerPoolSize      int
		MaxErrorsToStore      int
		RandomStringBuffer    int
		Sigma                 float64
		WarmupDuration        time.Duration
		Duration              time.Duration
		PartitionCount        uint64
		MutationConcurrency   int
		ReadConcurrency       int
		DropSchema            bool
		StatementRatios       statements.Ratios
	}

	Workload struct {
		config   *WorkloadConfig
		status   *status.GlobalStatus
		logger   *zap.Logger
		schema   *typedef.Schema
		stopFlag *stop.Flag
		store    store.Store
		pool     *workpool.Pool
		jobs     *jobs.Jobs
		distFunc distributions.DistributionFunc
	}
)

var emptyRatios statements.Ratios

func NewWorkload(config *WorkloadConfig, storeConfig store.Config, schema *typedef.Schema, logger *zap.Logger, flag *stop.Flag) (*Workload, error) {
	logger.Debug("creating workload",
		zap.String("mode", config.RunningMode),
		zap.Duration("warmup", config.WarmupDuration),
		zap.Duration("duration", config.Duration),
		zap.Uint64("partition_count", config.PartitionCount),
		zap.Int("mutation_concurrency", config.MutationConcurrency),
		zap.Int("read_concurrency", config.ReadConcurrency),
		zap.Int("io_worker_pool", config.IOWorkerPoolSize),
	)

	randSrc, distFunc := distributions.New(
		config.PartitionDistribution,
		config.PartitionCount,
		config.Seed,
		config.MU,
		config.Sigma,
	)

	if config.StatementRatios == emptyRatios {
		config.StatementRatios = statements.DefaultStatementRatios()
	}

	if config.RandomStringBuffer <= 0 {
		config.RandomStringBuffer = 32 * 1024 * 1024 // 32 MiB
	}

	logger.Debug("pre-allocating random string buffer", zap.Int("size", config.RandomStringBuffer))
	utils.PreallocateRandomString(rand.New(randSrc), config.RandomStringBuffer)

	logger.Debug("creating generators")
	//gens, err := generators.New(
	//	schema,
	//	distFunc,
	//	config.PartitionCount,
	//	config.PartitionBufferSize,
	//	logger,
	//	randSrc,
	//)
	//if err != nil {
	//	logger.Error("failed to create generators", zap.Error(err))
	//	return nil, err
	//}

	logger.Debug("generating schema changes")

	partitionConfig := schema.Config.GetPartitionRangeConfig()
	schemaChanges := partitions.NewPartitionKeys(rand.New(randSrc), schema.Tables[0], &partitionConfig)
	globalStatus := status.NewGlobalStatus(config.MaxErrorsToStore)
	logger.Debug("creating workpool", zap.Int("size", config.IOWorkerPoolSize))
	pool := workpool.New(config.IOWorkerPoolSize)

	logger.Debug("creating store")
	st, err := store.New(schemaChanges, pool, schema, storeConfig, logger.Named("store"), globalStatus.Errors)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create store")
	}

	logger.Debug("creating statement ratio controller")
	statementRatioController, err := statements.NewRatioController(config.StatementRatios, rand.New(randSrc))
	if err != nil {
		return nil, errors.Wrap(err, "failed to create statement ratio controller")
	}

	logger.Debug("creating jobs")
	w := &Workload{
		config:   config,
		status:   globalStatus,
		logger:   logger,
		schema:   schema,
		store:    st,
		pool:     pool,
		distFunc: distFunc,
		stopFlag: flag.CreateChild("jobs"),
		jobs: jobs.New(
			config.MutationConcurrency,
			config.ReadConcurrency,
			schema,
			st,
			globalStatus,
			statementRatioController,
			logger.Named("jobs"),
			randSrc,
		),
	}

	logger.Debug("creating schema and tables")
	if err = w.createSchemaAndTables(context.Background()); err != nil {
		return nil, errors.Wrap(err, "failed to create schema and tables")
	}

	logger.Debug("workload created successfully")
	return w, nil
}

func (w *Workload) GetGlobalStatus() *status.GlobalStatus {
	return w.status
}

func (w *Workload) createSchemaAndTables(base context.Context) error {
	if w.config.DropSchema && w.config.RunningMode != jobs.ReadMode {
		w.logger.Debug("dropping existing schema")
		for _, stmt := range statements.GetDropKeyspace(w.schema) {
			dropCtx, dropCancel := context.WithTimeout(base, 30*time.Second)
			w.logger.Debug("executing drop statement", zap.String("statement", stmt))
			if err := w.store.Mutate(dropCtx, typedef.SimpleStmt(stmt, typedef.DropKeyspaceStatementType)); err != nil {
				dropCancel()
				return errors.Wrap(err, "unable to drop schema")
			}
			dropCancel()
		}
		w.logger.Debug("schema dropped successfully")
	}

	ctx, cancel := context.WithTimeout(base, 30*time.Second)
	defer cancel()

	w.logger.Debug("creating keyspaces")
	testKeyspace, oracleKeyspace := statements.GetCreateKeyspaces(w.schema)
	if err := w.store.Create(
		ctx,
		typedef.SimpleStmt(testKeyspace, typedef.CreateKeyspaceStatementType),
		typedef.SimpleStmt(oracleKeyspace, typedef.CreateKeyspaceStatementType)); err != nil {
		return errors.Wrap(err, "unable to create keyspace")
	}
	w.logger.Debug("keyspaces created successfully")

	w.logger.Debug("creating tables")
	for _, stmt := range statements.GetCreateSchema(w.schema) {
		createSchemaCtx, createSchemaCancel := context.WithTimeout(base, 30*time.Second)

		w.logger.Debug("executing create table statement", zap.String("statement", stmt))
		if err := w.store.Mutate(createSchemaCtx, typedef.SimpleStmt(stmt, typedef.CreateSchemaStatementType)); err != nil {
			createSchemaCancel()
			return errors.Wrap(err, "unable to create schema")
		}

		createSchemaCancel()
	}
	w.logger.Debug("tables created successfully")

	return nil
}

func (w *Workload) Run(base context.Context) error {
	if w.config.WarmupDuration > 0 {
		w.logger.Debug("starting warmup phase", zap.Duration("duration", w.config.WarmupDuration))
		stopFlag := w.stopFlag.CreateChild("warmup")

		if err := w.jobs.Run(base, w.config.WarmupDuration, w.distFunc, w.config.PartitionCount, w.schema.Config.GetPartitionRangeConfig(), stopFlag, jobs.WarmupMode); err != nil {
			return errors.Wrap(err, "failed to run warmup jobs")
		}
		w.logger.Debug("warmup phase completed")
	}

	if w.config.Duration > 0 {
		w.logger.Debug("starting main workload", zap.Duration("duration", w.config.Duration), zap.String("mode", w.config.RunningMode))
		stopFlag := w.stopFlag.CreateChild("gemini")

		if err := w.jobs.Run(
			base,
			w.config.Duration,
			w.distFunc,
			w.config.PartitionCount,
			w.schema.Config.GetPartitionRangeConfig(),
			stopFlag,
			w.config.RunningMode,
		); err != nil {
			return err
		}

		w.logger.Debug("main workload completed")
	}

	// If any errors were recorded during the workload, surface them to the caller
	if w.status.HasErrors() {
		return errors.New("workload encountered errors")
	}

	return nil
}

func (w *Workload) PrintResults(geminiVersion string) error {
	writer, err := utils.CreateFile(w.config.OutputFile, false, os.Stdout)
	if err != nil {
		return err
	}

	defer func() {
		file, ok := writer.(*os.File)

		if !ok {
			return
		}

		if file == os.Stderr || file == os.Stdout {
			return
		}

		if syncErr := file.Sync(); syncErr != nil {
			w.logger.Error("failed to sync output file",
				zap.Error(syncErr),
				zap.String("file", w.config.OutputFile))
		}

		if closeErr := file.Close(); closeErr != nil {
			w.logger.Error("failed to close output file",
				zap.Error(closeErr),
				zap.String("file", w.config.OutputFile))
		}
	}()

	w.status.PrintResult(writer, w.schema, geminiVersion, w.config.StatementRatios.GetStatementInfo())
	if w.status.HasErrors() {
		return errors.New("gemini encountered errors, exiting with non zero status")
	}

	return nil
}

func (w *Workload) Close() error {
	w.logger.Info("closing workload")

	w.logger.Info("setting stop flag")
	w.stopFlag.SetSoft(true)

	w.logger.Info("closing workpool")
	err := w.pool.Close()

	w.logger.Debug("closing store")
	err = multierr.Append(err, w.store.Close())

	if err != nil {
		w.logger.Error("workload closed with errors", zap.Error(err))
	} else {
		w.logger.Debug("workload closed successfully")
	}

	return err
}
