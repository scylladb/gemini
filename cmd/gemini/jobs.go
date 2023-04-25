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
	"time"

	"github.com/scylladb/gemini"
	"github.com/scylladb/gemini/store"
	"go.uber.org/zap"
	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"
)

// MutationJob continuously applies mutations against the database
// for as long as the pump is active.
func MutationJob(ctx context.Context, pump <-chan heartBeat, schema *gemini.Schema, schemaCfg gemini.SchemaConfig, table *gemini.Table, s store.Store, r *rand.Rand, p gemini.PartitionRangeConfig, g *gemini.Generator, c chan Status, mode string, warmup time.Duration, logger *zap.Logger) error {
	schemaConfig := &schemaCfg
	logger = logger.Named("mutation_job")
	testStatus := Status{}
	defer func() {
		// Send any remaining updates back
		c <- testStatus
	}()
	var i int
	for {
		select {
		case <-ctx.Done():
			logger.Debug("mutation job terminated")
			return ctx.Err()
		case hb := <-pump:
			hb.await()
			ind := r.Intn(1000000)
			if ind%100000 == 0 {
				if err := ddl(ctx, schema, schemaConfig, table, s, r, p, &testStatus, logger); err != nil {
					return err
				}
			} else {
				if err := mutation(ctx, schema, schemaConfig, table, s, r, p, g, &testStatus, true, logger); err != nil {
					return err
				}
			}
			if i%1000 == 0 {
				c <- testStatus
				testStatus = Status{}
			}
			if failFast && (testStatus.ReadErrors > 0 || testStatus.WriteErrors > 0) {
				c <- testStatus
				return nil
			}
		}
		i++
	}
}

// ValidationJob continuously applies validations against the database
// for as long as the pump is active.
func ValidationJob(ctx context.Context, pump <-chan heartBeat, schema *gemini.Schema, schemaCfg gemini.SchemaConfig, table *gemini.Table, s store.Store, r *rand.Rand, p gemini.PartitionRangeConfig, g *gemini.Generator, c chan Status, mode string, warmup time.Duration, logger *zap.Logger) error {
	schemaConfig := &schemaCfg
	logger = logger.Named("validation_job")

	testStatus := Status{}
	defer func() {
		c <- testStatus
	}()
	var i int
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case hb := <-pump:
			hb.await()
			if cql, err := validation(ctx, schema, schemaConfig, table, s, r, p, g, &testStatus, logger); err != nil {
				e := JobError{
					Timestamp: time.Now(),
					Message:   "Validation failed: " + err.Error(),
					Query:     cql,
				}
				if len(testStatus.Errors) < maxErrorsToStore {
					testStatus.Errors = append(testStatus.Errors, e)
				}
				testStatus.ReadErrors++
			} else {
				testStatus.ReadOps++
			}

			if i%1000 == 0 {
				c <- testStatus
				testStatus = Status{}
			}
			if failFast && (testStatus.ReadErrors > 0 || testStatus.WriteErrors > 0) {
				return nil
			}
		}
		i++
	}
}

// WarmupJob continuously applies mutations against the database
// for as long as the pump is active or the supplied duration expires.
func WarmupJob(ctx context.Context, pump <-chan heartBeat, schema *gemini.Schema, schemaCfg gemini.SchemaConfig, table *gemini.Table, s store.Store, r *rand.Rand, p gemini.PartitionRangeConfig, g *gemini.Generator, c chan Status, mode string, warmup time.Duration, logger *zap.Logger) error {
	schemaConfig := &schemaCfg
	testStatus := Status{}
	var i int
	warmupTimer := time.NewTimer(warmup)
	for {
		select {
		case <-ctx.Done():
			logger.Debug("warmup job terminated")
			c <- testStatus
			return ctx.Err()
		case <-warmupTimer.C:
			logger.Debug("warmup job finished")
			c <- testStatus
			return nil
		default:
			// Do we care about errors during warmup?
			mutation(ctx, schema, schemaConfig, table, s, r, p, g, &testStatus, false, logger)
			if i%1000 == 0 {
				c <- testStatus
				testStatus = Status{}
			}
		}
	}
}

func job(ctx context.Context, f testJob, actors uint64, schema *gemini.Schema, schemaConfig gemini.SchemaConfig, s store.Store, pump *Pump, generators []*gemini.Generator, result chan Status, logger *zap.Logger) error {
	g, gCtx := errgroup.WithContext(ctx)
	partitionRangeConfig := gemini.PartitionRangeConfig{
		MaxBlobLength:   schemaConfig.MaxBlobLength,
		MinBlobLength:   schemaConfig.MinBlobLength,
		MaxStringLength: schemaConfig.MaxStringLength,
		MinStringLength: schemaConfig.MinStringLength,
		UseLWT:          schemaConfig.UseLWT,
	}

	for j, table := range schema.Tables {
		gen := generators[j]
		for i := 0; i < int(actors); i++ {
			r := rand.New(rand.NewSource(seed))
			g.Go(func() error {
				return f(gCtx, pump.ch, schema, schemaConfig, table, s, r, partitionRangeConfig, gen, result, mode, warmup, logger)
			})
		}
	}
	return g.Wait()
}

func ddl(ctx context.Context, schema *gemini.Schema, sc *gemini.SchemaConfig, table *gemini.Table, s store.Store, r *rand.Rand, p gemini.PartitionRangeConfig, testStatus *Status, logger *zap.Logger) error {
	if sc.CQLFeature != gemini.CQL_FEATURE_ALL {
		logger.Debug("ddl statements disabled")
		return nil
	}
	table.Lock()
	defer table.Unlock()
	if len(table.MaterializedViews) > 0 {
		// Scylla does not allow changing the DDL of a table with materialized views.
		return nil
	}
	ddlStmts, postStmtHook, err := schema.GenDDLStmt(table, r, p, sc)
	if err != nil {
		logger.Error("Failed! Mutation statement generation failed", zap.Error(err))
		testStatus.WriteErrors++
		return err
	}
	if ddlStmts == nil {
		if w := logger.Check(zap.DebugLevel, "no statement generated"); w != nil {
			w.Write(zap.String("job", "ddl"))
		}
		return nil
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
			if len(testStatus.Errors) < maxErrorsToStore {
				testStatus.Errors = append(testStatus.Errors, e)
			}
			testStatus.WriteErrors++
		} else {
			testStatus.WriteOps++
		}
	}
	return nil
}

func mutation(ctx context.Context, schema *gemini.Schema, _ *gemini.SchemaConfig, table *gemini.Table, s store.Store, r *rand.Rand, p gemini.PartitionRangeConfig, g *gemini.Generator, testStatus *Status, deletes bool, logger *zap.Logger) error {
	mutateStmt, err := schema.GenMutateStmt(table, g, r, p, deletes)
	if err != nil {
		logger.Error("Failed! Mutation statement generation failed", zap.Error(err))
		testStatus.WriteErrors++
		return err
	}
	if mutateStmt == nil {
		if w := logger.Check(zap.DebugLevel, "no statement generated"); w != nil {
			w.Write(zap.String("job", "mutation"))
		}
		return err
	}
	mutateQuery := mutateStmt.Query
	token, mutateValues := mutateStmt.Values()
	defer func() {
		v := make(gemini.Value, len(table.PartitionKeys))
		copy(v, mutateValues)
		g.GiveOld(gemini.ValueWithToken{Token: token, Value: v})
	}()
	if w := logger.Check(zap.DebugLevel, "validation statement"); w != nil {
		w.Write(zap.String("pretty_cql", mutateStmt.PrettyCQL()))
	}
	if err := s.Mutate(ctx, mutateQuery, mutateValues...); err != nil {
		e := JobError{
			Timestamp: time.Now(),
			Message:   "Mutation failed: " + err.Error(),
			Query:     mutateStmt.PrettyCQL(),
		}
		if len(testStatus.Errors) < maxErrorsToStore {
			testStatus.Errors = append(testStatus.Errors, e)
		}
		testStatus.WriteErrors++
	} else {
		testStatus.WriteOps++
	}
	return nil
}

func validation(ctx context.Context, schema *gemini.Schema, sc *gemini.SchemaConfig, table *gemini.Table, s store.Store, r *rand.Rand, p gemini.PartitionRangeConfig, g *gemini.Generator, testStatus *Status, logger *zap.Logger) (string, error) {
	checkStmt := schema.GenCheckStmt(table, g, r, p)
	if checkStmt == nil {
		if w := logger.Check(zap.DebugLevel, "no statement generated"); w != nil {
			w.Write(zap.String("job", "validation"))
		}
		return "", nil
	}
	checkQuery := checkStmt.Query
	token, checkValues := checkStmt.Values()
	defer func() {
		// Signal done with this pk...
		g.GiveOld(gemini.ValueWithToken{Token: token})
	}()
	if w := logger.Check(zap.DebugLevel, "validation statement"); w != nil {
		w.Write(zap.String("pretty_cql", checkStmt.PrettyCQL()))
	}
	if err := s.Check(ctx, table, checkQuery, checkValues...); err != nil {
		if checkStmt.QueryType.PossibleAsyncOperation() {
			maxAttempts := sc.AsyncObjectStabilizationAttempts
			delay := sc.AsyncObjectStabilizationDelay
			for attempts := 0; attempts < maxAttempts; attempts++ {
				logger.Info("validation failed for possible async operation",
					zap.Duration("trying_again_in", delay))
				select {
				case <-time.After(delay):
				case <-ctx.Done():
					return checkStmt.PrettyCQL(), err
				}
				// Should we sample all the errors?
				if err = s.Check(ctx, table, checkQuery, checkValues...); err == nil {
					// Result sets stabilized
					return "", nil
				}
			}
		}
		return checkStmt.PrettyCQL(), err
	}
	return "", nil
}
