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
	"errors"
	"fmt"
	"time"

	"github.com/scylladb/gemini/pkg/count"
	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"

	"go.uber.org/zap"
	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
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

var stmtsCounter = count.StmtsCounters.AddTotalCounter(count.Info{
	Name:                  "total statement`s count",
	Unit:                  "pc.",
	Description:           "count of all generated statement`s by its type",
	PrometheusIntegration: false,
}, "", []string{"Select", "SelectRange", "SelectByIndex", "SelectFromMaterializedView", "Delete", "Insert", "InsertJSON", "Update", "AlterColumn", "DropColumn", "AddColumn"})

type List struct {
	name     string
	jobs     []job
	duration time.Duration
	workers  uint64
}

type job struct {
	function func(
		context.Context,
		<-chan time.Duration,
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
	ctx context.Context,
	schema *typedef.Schema,
	schemaConfig typedef.SchemaConfig,
	s store.Store,
	pump <-chan time.Duration,
	generators []*generators.Generator,
	globalStatus *status.GlobalStatus,
	logger *zap.Logger,
	seed uint64,
	stopFlag *stop.Flag,
	failFast, verbose bool,
) error {
	logger = logger.Named(l.name)
	ctx = stopFlag.CancelContextOnSignal(ctx, stop.SignalHardStop)
	g, gCtx := errgroup.WithContext(ctx)
	time.AfterFunc(l.duration, func() {
		logger.Info("jobs time is up, begins jobs completion")
		stopFlag.SetSoft(true)
	})

	partitionRangeConfig := schemaConfig.GetPartitionRangeConfig()
	logger.Info("start jobs")
	for j := range schema.Tables {
		gen := generators[j]
		table := schema.Tables[j]
		for i := 0; i < int(l.workers); i++ {
			for idx := range l.jobs {
				jobF := l.jobs[idx].function
				r := rand.New(rand.NewSource(seed))
				g.Go(func() error {
					return jobF(gCtx, pump, schema, schemaConfig, table, s, r, &partitionRangeConfig, gen, globalStatus, logger, stopFlag, failFast, verbose)
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
	pump <-chan time.Duration,
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
	for {
		if stopFlag.IsHardOrSoft() {
			return nil
		}
		select {
		case <-stopFlag.SignalChannel():
			logger.Debug("mutation job terminated")
			return nil
		case hb := <-pump:
			time.Sleep(hb)
		}
		ind := r.Intn(1000000)
		if ind%100000 == 0 {
			_ = ddl(ctx, schema, schemaConfig, table, s, r, p, globalStatus, logger, verbose)
		} else {
			_ = mutation(ctx, schema, schemaConfig, table, s, r, p, g, globalStatus, true, logger)
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
	pump <-chan time.Duration,
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
	logger = logger.Named("validation_job")
	logger.Info("starting validation loop")
	defer func() {
		logger.Info("ending validation loop")
	}()

	for {
		if stopFlag.IsHardOrSoft() {
			return nil
		}
		select {
		case <-stopFlag.SignalChannel():
			return nil
		case hb := <-pump:
			time.Sleep(hb)
		}
		stmt := GenCheckStmt(schema, table, g, r, p)
		if stmt == nil {
			logger.Info("Validation. No statement generated from GenCheckStmt.")
			continue
		}

		err := validation(ctx, schemaConfig, table, s, stmt, g, globalStatus, logger)
		switch {
		case err == nil:
			globalStatus.ReadOps.Add(1)
		case errors.Is(err, context.Canceled):
			return nil
		default:
			globalStatus.AddReadError(&joberror.JobError{
				Timestamp: time.Now(),
				StmtType:  stmt.QueryType.ToString(),
				Message:   "Validation failed: " + err.Error(),
				Query:     stmt.PrettyCQL(),
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
	_ <-chan time.Duration,
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
		if stopFlag.IsHardOrSoft() {
			logger.Debug("warmup job terminated")
			return nil
		}
		// Do we care about errors during warmup?
		_ = mutation(ctx, schema, schemaConfig, table, s, r, p, g, globalStatus, false, logger)
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
		if w := logger.Check(zap.DebugLevel, "no statement generated"); w != nil {
			w.Write(zap.String("job", "ddl"))
		}
		return nil
	}
	for _, ddlStmt := range ddlStmts.List {
		if w := logger.Check(zap.DebugLevel, "ddl statement"); w != nil {
			w.Write(zap.String("pretty_cql", ddlStmt.PrettyCQL()))
		}
		stmtsCounter.Inc(int(ddlStmt.QueryType))
		if err = s.Mutate(ctx, ddlStmt.Query); err != nil {
			if errors.Is(err, context.Canceled) {
				return nil
			}
			globalStatus.AddWriteError(&joberror.JobError{
				Timestamp: time.Now(),
				StmtType:  ddlStmts.QueryType.ToString(),
				Message:   "DDL failed: " + err.Error(),
				Query:     ddlStmt.PrettyCQL(),
			})
			return err
		}
		globalStatus.WriteOps.Add(1)
	}
	ddlStmts.PostStmtHook()
	if verbose {
		jsonSchema, _ := json.MarshalIndent(schema, "", "    ")
		fmt.Printf("New schema: %v\n", string(jsonSchema))
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
		if w := logger.Check(zap.DebugLevel, "no statement generated"); w != nil {
			w.Write(zap.String("job", "mutation"))
		}
		return err
	}
	mutateQuery := mutateStmt.Query
	mutateValues := mutateStmt.Values
	stmtsCounter.Inc(int(mutateStmt.QueryType))
	if mutateStmt.ValuesWithToken != nil {
		defer func() {
			g.GiveOld(mutateStmt.ValuesWithToken)
		}()
	}
	if w := logger.Check(zap.DebugLevel, "mutation statement"); w != nil {
		w.Write(zap.String("pretty_cql", mutateStmt.PrettyCQL()))
	}
	if err = s.Mutate(ctx, mutateQuery, mutateValues...); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}
		globalStatus.AddWriteError(&joberror.JobError{
			Timestamp: time.Now(),
			StmtType:  mutateStmt.QueryType.ToString(),
			Message:   "Mutation failed: " + err.Error(),
			Query:     mutateStmt.PrettyCQL(),
		})
	} else {
		globalStatus.WriteOps.Add(1)
	}
	return nil
}

func validation(
	ctx context.Context,
	sc *typedef.SchemaConfig,
	table *typedef.Table,
	s store.Store,
	stmt *typedef.Stmt,
	g *generators.Generator,
	_ *status.GlobalStatus,
	logger *zap.Logger,
) error {
	if stmt.ValuesWithToken != nil {
		defer func() {
			g.ReleaseToken(stmt.ValuesWithToken.Token)
		}()
	}
	if w := logger.Check(zap.DebugLevel, "validation statement"); w != nil {
		w.Write(zap.String("pretty_cql", stmt.PrettyCQL()))
	}

	maxAttempts := 1
	delay := 10 * time.Millisecond
	if stmt.QueryType.PossibleAsyncOperation() {
		maxAttempts = sc.AsyncObjectStabilizationAttempts
		if maxAttempts < 1 {
			maxAttempts = 1
		}
		delay = sc.AsyncObjectStabilizationDelay
	}
	stmtsCounter.Inc(int(stmt.QueryType))
	var lastErr, err error
	attempt := 1
	for {
		lastErr = err
		err = s.Check(ctx, table, stmt.Query, stmt.Values...)

		if err == nil {
			if attempt > 1 {
				logger.Info(fmt.Sprintf("Validation successfully completed on %d attempt.", attempt))
			}
			return nil
		}
		if errors.Is(err, context.Canceled) {
			// When context is canceled it means that test was commanded to stop
			// to skip logging part it is returned here
			return err
		}
		if attempt == maxAttempts {
			break
		}
		if errors.Is(err, unWrapErr(lastErr)) {
			logger.Info(fmt.Sprintf("Retring failed validation. %d attempt from %d attempts. Error same as at attempt before. ", attempt, maxAttempts))
		} else {
			logger.Info(fmt.Sprintf("Retring failed validation. %d attempt from %d attempts. Error: %s", attempt, maxAttempts, err))
		}

		select {
		case <-time.After(delay):
		case <-ctx.Done():
			logger.Info(fmt.Sprintf("Retring failed validation stoped by done context. %d attempt from %d attempts. Error: %s", attempt, maxAttempts, err))
			return nil
		}
		attempt++
	}

	if attempt > 1 {
		logger.Info(fmt.Sprintf("Retring failed validation stoped by reach of max attempts %d. Error: %s", maxAttempts, err))
	} else {
		logger.Info(fmt.Sprintf("Validation failed. Error: %s", err))
	}

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
