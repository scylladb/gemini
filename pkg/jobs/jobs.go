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

package jobs

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

const (
	WriteMode  = "write"
	ReadMode   = "read"
	MixedMode  = "mixed"
	WarmupMode = "warmup"
)

const (
	warmupName   = "Warmup"
	validateName = "Validation"
	mutateName   = "Mutation"
)

var (
	warmup   = job{name: warmupName, function: warmupJob}
	validate = job{name: validateName, function: validationJob}
	mutate   = job{name: mutateName, function: mutationJob}
)

type List struct {
	name     string
	jobs     []job
	duration time.Duration
	workers  uint64
}

type job struct {
	function func(
		context.Context,
		*typedef.Schema,
		typedef.SchemaConfig,
		*typedef.Table,
		store.Store,
		*rand.Rand,
		*typedef.PartitionRangeConfig,
		*generators.Generator,
		*status.GlobalStatus,
		*zap.Logger,
		*stop.Flag,
		bool,
		bool,
	) error
	name string
}

func ListFromMode(mode string, duration time.Duration, workers uint64) List {
	jobs := make([]job, 0, 2)
	name := "work cycle"
	switch mode {
	case WriteMode:
		jobs = append(jobs, mutate)
	case ReadMode:
		jobs = append(jobs, validate)
	case WarmupMode:
		jobs = append(jobs, warmup)
		name = "warmup cycle"
	default:
		jobs = append(jobs, mutate, validate)
	}
	return List{
		name:     name,
		jobs:     jobs,
		duration: duration,
		workers:  workers,
	}
}

func (l List) Run(
	base context.Context,
	schema *typedef.Schema,
	schemaConfig typedef.SchemaConfig,
	s store.Store,
	generators *generators.Generators,
	globalStatus *status.GlobalStatus,
	logger *zap.Logger,
	stopFlag *stop.Flag,
	failFast, verbose bool,
	src rand.Source,
) error {
	ctx, cancel := context.WithTimeout(base, l.duration)
	defer cancel()
	logger = logger.Named(l.name)
	g, gCtx := errgroup.WithContext(ctx)

	partitionRangeConfig := schemaConfig.GetPartitionRangeConfig()
	logger.Info("start jobs")
	for _, table := range schema.Tables {
		rnd := rand.New(src)
		for range l.workers {
			for idx := range l.jobs {
				jobF := l.jobs[idx].function
				generator := generators.Get(table)
				g.Go(func() error {
					return jobF(
						gCtx,
						schema,
						schemaConfig,
						table,
						s,
						rnd,
						&partitionRangeConfig,
						generator,
						globalStatus,
						logger,
						stopFlag,
						failFast,
						verbose,
					)
				})
			}
		}
	}

	return g.Wait()
}

// mutationJob continuously applies mutations against the database
// for as long as the pump is active.
func mutationJob(
	ctx context.Context,
	schema *typedef.Schema,
	schemaCfg typedef.SchemaConfig,
	table *typedef.Table,
	s store.Store,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	g *generators.Generator,
	globalStatus *status.GlobalStatus,
	logger *zap.Logger,
	stopFlag *stop.Flag,
	failFast, verbose bool,
) error {
	schemaConfig := &schemaCfg
	logger = logger.Named("mutation_job")
	logger.Info("starting mutation loop")
	defer func() {
		logger.Info("ending mutation loop")
	}()
	executionCount := 0
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		if executionCount%10000 == 0 {
			time.Sleep(time.Duration(utils.RandInt2(r, 0, 10)) * time.Millisecond)
		}

		var err error

		metrics.ExecutionTime("mutation_job", func() {
			if r.IntN(1000000)%100000 == 0 {
				err = ddl(ctx, schema, schemaConfig, table, s, r, p, globalStatus, logger, verbose)
			} else {
				err = mutation(ctx, schema, schemaConfig, table, s, r, p, g, globalStatus, true, logger)
			}
		})

		executionCount++

		if err != nil {
			return err
		}

		if failFast && globalStatus.HasErrors() {
			stopFlag.SetSoft(true)
			return nil
		}
	}
}

// validationJob continuously applies validations against the database
// for as long as the pump is active.
func validationJob(
	ctx context.Context,
	schema *typedef.Schema,
	schemaCfg typedef.SchemaConfig,
	table *typedef.Table,
	s store.Store,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	g *generators.Generator,
	globalStatus *status.GlobalStatus,
	logger *zap.Logger,
	stopFlag *stop.Flag,
	failFast, _ bool,
) error {
	logger = logger.Named("validation_job")
	logger.Info("starting validation loop")
	defer func() {
		logger.Info("ending validation loop")
	}()

	maxAttempts := schemaCfg.AsyncObjectStabilizationAttempts
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	delay := schemaCfg.AsyncObjectStabilizationDelay
	if delay < 0 {
		delay = 10 * time.Millisecond
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		var (
			err  error
			stmt *typedef.Stmt
		)

		metrics.ExecutionTime("validation_job", func() {
			stmt = GenCheckStmt(schema, table, g, r, p)
			if stmt == nil {
				logger.Info("Validation. No statement generated from GenCheckStmt.")
				return
			}

			err = validation(ctx, table, s, stmt, logger, maxAttempts, delay)
			if stmt.ValuesWithToken != nil {
				for _, token := range stmt.ValuesWithToken {
					g.ReleaseToken(token.Token)
				}
			}
		})

		switch {
		case err == nil:
			globalStatus.ReadOps.Add(1)
		case errors.Is(err, context.Canceled):
			return nil
		default:
			query, prettyErr := stmt.PrettyCQL()
			if prettyErr != nil {
				return PrettyCQLError{
					PrettyCQL: prettyErr,
					Stmt:      stmt,
					Err:       err,
				}
			}

			globalStatus.AddReadError(joberror.JobError{
				Timestamp: time.Now(),
				StmtType:  stmt.QueryType.String(),
				Message:   "Validation failed: " + err.Error(),
				Query:     query,
			})
		}

		if failFast && globalStatus.HasErrors() {
			stopFlag.SetSoft(true)
			return nil
		}
	}
}

// warmupJob continuously applies mutations against the database
// for as long as the pump is active or the supplied duration expires.
func warmupJob(
	ctx context.Context,
	schema *typedef.Schema,
	schemaCfg typedef.SchemaConfig,
	table *typedef.Table,
	s store.Store,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	g *generators.Generator,
	globalStatus *status.GlobalStatus,
	logger *zap.Logger,
	stopFlag *stop.Flag,
	failFast, _ bool,
) error {
	schemaConfig := &schemaCfg
	logger = logger.Named("warmup")
	logger.Info("starting warmup loop")
	defer func() {
		logger.Info("ending warmup loop")
	}()
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		err := mutation(ctx, schema, schemaConfig, table, s, r, p, g, globalStatus, false, logger)
		if err != nil {
			return err
		}

		if failFast && globalStatus.HasErrors() {
			stopFlag.SetSoft(true)
			return nil
		}
	}
}

func ddl(
	ctx context.Context,
	schema *typedef.Schema,
	sc *typedef.SchemaConfig,
	table *typedef.Table,
	s store.Store,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	globalStatus *status.GlobalStatus,
	logger *zap.Logger,
	verbose bool,
) error {
	if sc.CQLFeature != typedef.CQL_FEATURE_ALL {
		logger.Debug("ddl statements disabled")
		return nil
	}
	if len(table.MaterializedViews) > 0 {
		// Scylla does not allow changing the DDL of a table with materialized views.
		return nil
	}
	table.Lock()
	defer table.Unlock()
	ddlStmts, err := GenDDLStmt(schema, table, r, p, sc)
	if err != nil {
		logger.Error("Failed! DDL Mutation statement generation failed", zap.Error(err))
		globalStatus.WriteErrors.Add(1)
		return err
	}

	if ddlStmts == nil {
		logger.Debug("no statement generated", zap.String("job", "ddl"))
		return nil
	}

	for _, ddlStmt := range ddlStmts.List {
		if err = s.Mutate(ctx, ddlStmt); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}

			prettyCQL, prettyCQLErr := ddlStmt.PrettyCQL()
			if prettyCQLErr != nil {
				return PrettyCQLError{
					PrettyCQL: prettyCQLErr,
					Stmt:      ddlStmt,
					Err:       err,
				}
			}

			globalStatus.AddWriteError(joberror.JobError{
				Timestamp: time.Now(),
				StmtType:  ddlStmts.QueryType.String(),
				Message:   "DDL failed: " + err.Error(),
				Query:     prettyCQL,
			})

			return err
		}
		globalStatus.WriteOps.Add(1)
	}
	ddlStmts.PostStmtHook()
	if verbose {
		jsonSchema, _ := json.MarshalIndent(schema, "", "    ")
		fmt.Printf("New schema: %v\n", string(jsonSchema)) //nolint:forbidigo
	}
	return nil
}

func mutation(
	ctx context.Context,
	schema *typedef.Schema,
	_ *typedef.SchemaConfig,
	table *typedef.Table,
	s store.Store,
	r *rand.Rand,
	p *typedef.PartitionRangeConfig,
	g *generators.Generator,
	globalStatus *status.GlobalStatus,
	deletes bool,
	logger *zap.Logger,
) error {
	mutateStmt, err := GenMutateStmt(schema, table, g, r, p, deletes)
	if err != nil {
		logger.Error("Failed! Mutation statement generation failed", zap.Error(err))
		globalStatus.WriteErrors.Add(1)
		return err
	}
	if mutateStmt == nil {
		logger.Debug("no statement generated", zap.String("job", "mutation"))
		return err
	}

	if err = s.Mutate(ctx, mutateStmt); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil
		}

		prettyCQL, prettyCQLErr := mutateStmt.PrettyCQL()
		if prettyCQLErr != nil {
			return PrettyCQLError{
				PrettyCQL: prettyCQLErr,
				Stmt:      mutateStmt,
				Err:       err,
			}
		}

		globalStatus.AddWriteError(joberror.JobError{
			Timestamp: time.Now(),
			StmtType:  mutateStmt.QueryType.String(),
			Err:       err,
			Message:   "Mutation failed: ",
			Query:     prettyCQL,
		})

		return err
	}

	globalStatus.WriteOps.Add(1)
	g.GiveOlds(mutateStmt.ValuesWithToken...)

	return nil
}

func validation(
	ctx context.Context,
	table *typedef.Table,
	s store.Store,
	stmt *typedef.Stmt,
	logger *zap.Logger,
	maxAttempts int,
	delay time.Duration,
) error {
	var lastErr, err error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		lastErr = err
		err = s.Check(ctx, table, stmt, attempt == maxAttempts)

		if err == nil {
			if attempt > 1 {
				logger.Info(
					"Validation successfully completed",
					zap.Int("attempt", attempt),
				)
			}
			return nil
		}
		if errors.Is(err, context.Canceled) {
			// When context is canceled it means that test was commanded to stop
			// to skip logging part it is returned here
			return err
		}

		if errors.Is(err, unWrapErr(lastErr)) {
			logger.Warn(
				"Retrying failed validation. Error same as at attempt before.",
				zap.Int("attempt", attempt),
				zap.Int("max_attempts", maxAttempts),
			)
		} else {
			logger.Warn("Retrying failed validation.",
				zap.Int("attempt", attempt),
				zap.Int("max_attempts", maxAttempts),
				zap.Error(err),
			)
		}

		select {
		case <-ctx.Done():
			logger.Info(
				"Retrying failed validation stoped by done context",
				zap.Int("attempt", attempt),
				zap.Int("max_attempts", maxAttempts),
				zap.Error(err),
			)
			return nil
		default:
			time.Sleep(delay)
			attempt++
		}
	}

	logger.Error("Validation failed. Error: %s", zap.Error(err))

	return err
}

func unWrapErr(err error) error {
	nextErr := err
	for nextErr != nil {
		err = nextErr
		nextErr = errors.Unwrap(err)
	}
	return err
}
