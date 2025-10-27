// Copyright 2025 ScyllaDB
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
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/statements"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

type Validation struct {
	table       *typedef.Table
	statement   *statements.Generator
	generator   partitions.Interface
	status      *status.GlobalStatus
	stopFlag    *stop.Flag
	store       store.Store
	maxAttempts int
	delay       time.Duration
}

func NewValidation(
	schema *typedef.Schema,
	table *typedef.Table,
	generator partitions.Interface,
	status *status.GlobalStatus,
	statementRatioController *statements.RatioController,
	stopFlag *stop.Flag,
	store store.Store,
	seed [32]byte,
) *Validation {
	maxAttempts := schema.Config.AsyncObjectStabilizationAttempts
	delay := schema.Config.AsyncObjectStabilizationDelay
	pc := schema.Config.GetPartitionRangeConfig()

	if maxAttempts <= 1 {
		maxAttempts = 10
	}

	if delay <= 10*time.Millisecond {
		delay = 200 * time.Millisecond
	}

	statementGenerator := statements.New(
		schema.Keyspace.Name,
		generator,
		table,
		rand.New(rand.NewChaCha8(seed)),
		&pc,
		statementRatioController,
		schema.Config.UseLWT,
	)

	return &Validation{
		table:       table,
		statement:   statementGenerator,
		generator:   generator,
		status:      status,
		stopFlag:    stopFlag,
		store:       store,
		maxAttempts: maxAttempts,
		delay:       delay,
	}
}

func (v *Validation) run(ctx context.Context, metric prometheus.Counter) error {
	stmt, stmtErr := v.statement.Select(ctx)
	if errors.Is(stmtErr, utils.ErrNoPartitionKeyValues) {
		return utils.ErrNoPartitionKeyValues
	}

	if stmt == nil {
		if v.status.HasReachedErrorCount() {
			v.stopFlag.SetSoft(true)
		}
		return ErrNoStatement
	}

	// Delegate retries to the store implementation
	validatedRows, err := v.store.Check(ctx, v.table, stmt, 0)
	if err == nil {
		metric.Add(float64(validatedRows))
		v.status.AddValidatedRows(validatedRows)
		v.status.ReadOp()
		return nil
	}

	if errors.Is(err, context.DeadlineExceeded) {
		return nil
	}

	if errors.Is(err, context.Canceled) {
		return nil
	}

	return joberror.JobError{
		Timestamp:     time.Now(),
		Err:           err,
		StmtType:      stmt.QueryType,
		Message:       "Validation failed: " + err.Error(),
		Query:         stmt.Query,
		PartitionKeys: stmt.PartitionKeys.Values,
		Values:        stmt.Values,
	}
}

func (v *Validation) Do(ctx context.Context) error {
	name := v.Name()

	executionTime := metrics.ExecutionTimeStart(name)
	metrics.GeminiInformation.WithLabelValues("validation_" + v.table.Name).Inc()
	defer metrics.GeminiInformation.WithLabelValues("validation_" + v.table.Name).Dec()

	validatedRowsMetric := metrics.ValidatedRows.WithLabelValues(v.table.Name)

	for !v.stopFlag.IsHardOrSoft() {
		// Check if context is cancelled before proceeding
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		err := executionTime.RunFuncE(func() error {
			return v.run(ctx, validatedRowsMetric)
		})

		if err == nil {
			continue
		}

		if errors.Is(err, utils.ErrNoPartitionKeyValues) {
			timer := utils.GetTimer(100 * time.Millisecond)
			select {
			case <-timer.C:
				utils.PutTimer(timer)
				continue
			case <-ctx.Done():
				utils.PutTimer(timer)
				return nil
			}
		}

		if errors.Is(err, context.Canceled) {
			return context.Canceled
		}

		if errors.Is(err, ErrNoStatement) {
			return nil
		}

		// Handle different error types that can be returned from the store
		var jobErr joberror.JobError
		if errors.As(err, &jobErr) {
			// It's already a joberror.JobError, use it directly
			v.status.AddReadError(jobErr)
		} else {
			panic(fmt.Sprintf("invalid type err %T: %v", err, err))
		}

		if v.status.HasReachedErrorCount() {
			v.stopFlag.SetSoft(true)
			return errors.New("validation job stopped due to errors")
		}
	}

	return nil
}

func (v *Validation) Name() string {
	return "validation_" + v.table.Name
}
