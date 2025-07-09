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
	"os"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/generators/statements"
	"github.com/scylladb/gemini/pkg/jobs"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
	"github.com/scylladb/gemini/pkg/workpool"
)

type (
	WorkloadConfig struct {
		RunningMode           string
		OutputFile            string
		PartitionDistribution distributions.Distribution
		Seed                  uint64
		PartitionBufferSize   int
		IOWorkerPoolSize      int
		MaxErrorsToStore      int
		MU                    float64
		Sigma                 float64
		WarmupDuration        time.Duration
		Duration              time.Duration
		PartitionCount        int
		MutationConcurrency   int
		ReadConcurrency       int
		DropSchema            bool
	}

	Workload struct {
		config     *WorkloadConfig
		status     *status.GlobalStatus
		logger     *zap.Logger
		schema     *typedef.Schema
		stopFlag   *stop.Flag
		store      store.Store
		pool       *workpool.Pool
		generators *generators.Generators
		jobs       *jobs.Jobs
	}
)

func NewWorkload(config *WorkloadConfig, storeConfig store.Config, schema *typedef.Schema, logger *zap.Logger, flag *stop.Flag) (*Workload, error) {
	randSrc, distFunc := distributions.New(
		config.PartitionDistribution,
		config.PartitionCount,
		config.Seed,
		config.MU,
		config.Sigma,
	)

	gens := generators.New(
		schema,
		distFunc,
		config.Seed,
		config.PartitionCount,
		config.PartitionBufferSize,
		logger,
		randSrc,
	)

	schemaChanges, err := gens.Get(schema.Tables[0]).Get(context.Background())
	if err != nil {
		return nil, errors.Wrap(err, "failed to generate schema changes")
	}

	globalStatus := status.NewGlobalStatus(config.MaxErrorsToStore)
	pool := workpool.New(config.IOWorkerPoolSize)

	st, err := store.New(schemaChanges, pool, schema, storeConfig, logger.Named("store"), globalStatus.Errors)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create store")
	}

	return &Workload{
		config:     config,
		status:     globalStatus,
		logger:     logger,
		schema:     schema,
		store:      st,
		pool:       pool,
		generators: gens,
		stopFlag:   flag.CreateChild("jobs"),
		jobs: jobs.New(
			config.MutationConcurrency,
			config.ReadConcurrency,
			schema,
			st,
			gens,
			globalStatus,
			logger.Named("jobs"),
			randSrc,
		),
	}, nil
}

func (w *Workload) GetGlobalStatus() *status.GlobalStatus {
	return w.status
}

func (w *Workload) Run(base context.Context) error {
	if w.config.DropSchema && w.config.RunningMode != jobs.ReadMode {
		for _, stmt := range statements.GetDropKeyspace(w.schema) {
			w.logger.Debug(stmt)
			if err := w.store.Mutate(base, typedef.SimpleStmt(stmt, typedef.DropKeyspaceStatementType)); err != nil {
				return errors.Wrap(err, "unable to drop schema")
			}
		}
	}

	testKeyspace, oracleKeyspace := statements.GetCreateKeyspaces(w.schema)
	if err := w.store.Create(
		base,
		typedef.SimpleStmt(testKeyspace, typedef.CreateKeyspaceStatementType),
		typedef.SimpleStmt(oracleKeyspace, typedef.CreateKeyspaceStatementType)); err != nil {
		return errors.Wrap(err, "unable to create keyspace")
	}

	for _, stmt := range statements.GetCreateSchema(w.schema) {
		w.logger.Debug(stmt)
		if err := w.store.Mutate(base, typedef.SimpleStmt(stmt, typedef.CreateSchemaStatementType)); err != nil {
			return errors.Wrap(err, "unable to create schema")
		}
	}

	if w.config.WarmupDuration > 0 {
		stopFlag := w.stopFlag.CreateChild("warmup")
		ctx, cancel := context.WithTimeout(base, w.config.WarmupDuration+2*time.Second)
		defer cancel()
		time.AfterFunc(w.config.WarmupDuration, func() {
			stopFlag.SetSoft(false)
			cancel()
		})

		if err := w.jobs.Run(ctx, stopFlag, jobs.WarmupMode); err != nil {
			return errors.Wrap(err, "failed to run warmup jobs")
		}
	}

	if w.config.Duration > 0 {
		stopFlag := w.stopFlag.CreateChild("gemini")
		ctx, cancel := context.WithTimeout(base, w.config.Duration+2*time.Second)
		defer cancel()
		time.AfterFunc(w.config.Duration, func() {
			stopFlag.SetSoft(false)
			cancel()
		})

		if err := w.jobs.Run(ctx, stopFlag, w.config.RunningMode); err != nil {
			return errors.Wrap(err, "failed to run jobs")
		}
	}

	return nil
}

func (w *Workload) PrintResults(geminiVersion string, versionInfo any) error {
	writer, err := utils.CreateFile(w.config.OutputFile, false, os.Stdout)
	if err != nil {
		return err
	}

	defer func() {
		file, ok := writer.(*os.File)

		if !ok {
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

	w.status.PrintResult(writer, w.schema, geminiVersion, versionInfo)
	if w.status.HasErrors() {
		return errors.New("gemini encountered errors, exiting with non zero status")
	}

	return nil
}

func (w *Workload) Close() error {
	w.stopFlag.SetSoft(false)
	err := w.pool.Close()
	err = multierr.Append(err, w.store.Close())
	return multierr.Append(err, w.generators.Close())
}
