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
	"gopkg.in/tomb.v2"
)

// MutationJob continuously applies mutations against the database
// for as long as the pump is active.
func MutationJob(ctx context.Context, pump <-chan heartBeat, schema *gemini.Schema, schemaCfg gemini.SchemaConfig, table *gemini.Table, s store.Store, r *rand.Rand, p gemini.PartitionRangeConfig, g *gemini.Generator, c chan Status, mode string, warmup time.Duration, logger *zap.Logger) {
	schemaConfig := &schemaCfg
	logger = logger.Named("mutation_job")
	testStatus := Status{}
	defer func() {
		// Send any remaining updates back
		c <- testStatus
	}()
	var i int
	for hb := range pump {
		hb.await()
		ind := r.Intn(1000000)
		if ind%100000 == 0 {
			ddl(ctx, schema, schemaConfig, table, s, r, p, &testStatus, logger)
		} else {
			mutation(ctx, schema, schemaConfig, table, s, r, p, g, &testStatus, true, logger)
		}
		if i%1000 == 0 {
			c <- testStatus
			testStatus = Status{}
		}
		if failFast && (testStatus.ReadErrors > 0 || testStatus.WriteErrors > 0) {
			c <- testStatus
			return
		}
		i++
	}
}

// ValidationJob continuously applies validations against the database
// for as long as the pump is active.
func ValidationJob(ctx context.Context, pump <-chan heartBeat, schema *gemini.Schema, schemaCfg gemini.SchemaConfig, table *gemini.Table, s store.Store, r *rand.Rand, p gemini.PartitionRangeConfig, g *gemini.Generator, c chan Status, mode string, warmup time.Duration, logger *zap.Logger) {
	schemaConfig := &schemaCfg
	logger = logger.Named("validation_job")

	testStatus := Status{}
	defer func() {
		c <- testStatus
	}()
	var i int
	for hb := range pump {
		hb.await()
		validation(ctx, schema, schemaConfig, table, s, r, p, g, &testStatus, logger)
		if i%1000 == 0 {
			c <- testStatus
			testStatus = Status{}
		}
		if failFast && (testStatus.ReadErrors > 0 || testStatus.WriteErrors > 0) {
			return
		}
		i++
	}
}

// WarmupJob continuously applies mutations against the database
// for as long as the pump is active or the supplied duration expires.
func WarmupJob(ctx context.Context, pump <-chan heartBeat, schema *gemini.Schema, schemaCfg gemini.SchemaConfig, table *gemini.Table, s store.Store, r *rand.Rand, p gemini.PartitionRangeConfig, g *gemini.Generator, c chan Status, mode string, warmup time.Duration, logger *zap.Logger) {
	schemaConfig := &schemaCfg
	testStatus := Status{}
	var i int
	warmupTimer := time.NewTimer(warmup)
	for {
		select {
		case _, ok := <-pump:
			if !ok {
				logger.Debug("warmup job terminated")
				c <- testStatus
				return
			}
		}
		select {
		case <-warmupTimer.C:
			c <- testStatus
			return
		default:
			mutation(ctx, schema, schemaConfig, table, s, r, p, g, &testStatus, false, logger)
			if i%1000 == 0 {
				c <- testStatus
				testStatus = Status{}
			}
		}
	}
}

func job(t *tomb.Tomb, f testJob, actors uint64, schema *gemini.Schema, schemaConfig gemini.SchemaConfig, s store.Store, pump *Pump, generators []*gemini.Generator, result chan Status, logger *zap.Logger) {
	workerCtx, _ := context.WithCancel(context.Background())
	partitionRangeConfig := gemini.PartitionRangeConfig{
		MaxBlobLength:   schemaConfig.MaxBlobLength,
		MinBlobLength:   schemaConfig.MinBlobLength,
		MaxStringLength: schemaConfig.MaxStringLength,
		MinStringLength: schemaConfig.MinStringLength,
	}

	for j, table := range schema.Tables {
		g := generators[j]
		for i := 0; i < int(actors); i++ {
			r := rand.New(rand.NewSource(seed))
			t.Go(func() error {
				f(workerCtx, pump.ch, schema, schemaConfig, table, s, r, partitionRangeConfig, g, result, mode, warmup, logger)
				return nil
			})
		}
	}
}

func ddl(ctx context.Context, schema *gemini.Schema, sc *gemini.SchemaConfig, table *gemini.Table, s store.Store, r *rand.Rand, p gemini.PartitionRangeConfig, testStatus *Status, logger *zap.Logger) {
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

func mutation(ctx context.Context, schema *gemini.Schema, _ *gemini.SchemaConfig, table *gemini.Table, s store.Store, r *rand.Rand, p gemini.PartitionRangeConfig, g *gemini.Generator, testStatus *Status, deletes bool, logger *zap.Logger) {
	mutateStmt, err := schema.GenMutateStmt(table, g, r, p, deletes)
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
		testStatus.Errors = append(testStatus.Errors, e)
		testStatus.WriteErrors++
	} else {
		testStatus.WriteOps++
	}
}

func validation(ctx context.Context, schema *gemini.Schema, _ *gemini.SchemaConfig, table *gemini.Table, s store.Store, r *rand.Rand, p gemini.PartitionRangeConfig, g *gemini.Generator, testStatus *Status, logger *zap.Logger) {
	checkStmt := schema.GenCheckStmt(table, g, r, p)
	if checkStmt == nil {
		if w := logger.Check(zap.DebugLevel, "no statement generated"); w != nil {
			w.Write(zap.String("job", "validation"))
		}
		return
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
