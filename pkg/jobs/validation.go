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
	"github.com/scylladb/gocqlx/v3/qb"

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
	generator    partitions.Interface
	store        store.Store
	table        *typedef.Table
	statement    *statements.Generator
	status       *status.GlobalStatus
	stopFlag     *stop.Flag
	keyspaceName string
	maxAttempts  int
	delay        time.Duration
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
		table:        table,
		keyspaceName: schema.Keyspace.Name,
		statement:    statementGenerator,
		generator:    generator,
		status:       status,
		stopFlag:     stopFlag,
		store:        store,
		maxAttempts:  maxAttempts,
		delay:        delay,
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

// createSelectStmtForPartitionKeys creates a SELECT statement for specific partition key values
func (v *Validation) createSelectStmtForPartitionKeys(partitionKeyValues *typedef.Values) *typedef.Stmt {
	keyspaceAndTable := v.keyspaceName + "." + v.table.Name
	builder := qb.Select(keyspaceAndTable)

	for _, pk := range v.table.PartitionKeys {
		builder = builder.Where(qb.Eq(pk.Name))
	}

	query, _ := builder.ToCql()

	return &typedef.Stmt{
		QueryType: typedef.SelectStatementType,
		Query:     query,
		PartitionKeys: typedef.PartitionKeys{
			Values: partitionKeyValues,
		},
		Values: partitionKeyValues.ToCQLValues(v.table.PartitionKeys),
	}
}

// validateDeletedPartition checks that a deleted partition returns no rows from the database
func (v *Validation) validateDeletedPartition(ctx context.Context, partitionKeyValues *typedef.Values) error {
	stmt := v.createSelectStmtForPartitionKeys(partitionKeyValues)

	// Check that the partition returns 0 rows
	rowCount, err := v.store.Check(ctx, v.table, stmt, 0)
	if err != nil {
		// Store check failed - this is an error
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return err
		}
		return joberror.JobError{
			Timestamp:     time.Now(),
			Err:           err,
			StmtType:      stmt.QueryType,
			Message:       "Deleted partition validation failed: " + err.Error(),
			Query:         stmt.Query,
			PartitionKeys: stmt.PartitionKeys.Values,
			Values:        stmt.Values,
		}
	}

	if rowCount > 0 {
		// Deleted partition still has rows - this is a validation error
		err = fmt.Errorf("deleted partition still exists with %d rows", rowCount)
		return joberror.JobError{
			Timestamp:     time.Now(),
			Err:           err,
			StmtType:      stmt.QueryType,
			Message:       fmt.Sprintf("Deleted partition validation failed: partition still exists with %d rows", rowCount),
			Query:         stmt.Query,
			PartitionKeys: stmt.PartitionKeys.Values,
			Values:        stmt.Values,
		}
	}

	// Partition correctly returns 0 rows
	return nil
}

func (v *Validation) Do(ctx context.Context) error {
	name := v.Name()

	executionTime := metrics.ExecutionTimeStart(name)
	metrics.GeminiInformation.WithLabelValues("validation_" + v.table.Name).Inc()
	defer metrics.GeminiInformation.WithLabelValues("validation_" + v.table.Name).Dec()

	validatedRowsMetric := metrics.ValidatedRows.WithLabelValues(v.table.Name)
	deletedPartitionsCh := v.generator.Deleted()

	for !v.stopFlag.IsHardOrSoft() {
		// Check for deleted partitions to validate or perform regular validation
		select {
		case <-ctx.Done():
			return nil
		case deletedPartition, ok := <-deletedPartitionsCh:
			if !ok {
				// Channel closed, stop processing deleted partitions
				deletedPartitionsCh = nil
				continue
			}

			// Validate that the deleted partition is actually gone
			err := executionTime.RunFuncE(func() error {
				return v.validateDeletedPartition(ctx, deletedPartition)
			})

			if err == nil {
				// Deleted partition validation succeeded
				v.status.ReadOp()
				continue
			}

			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				continue
			}

			// Handle validation error
			var jobErr joberror.JobError
			if errors.As(err, &jobErr) {
				v.status.AddReadError(jobErr)
			} else {
				panic(fmt.Sprintf("invalid type err %T: %v", err, err))
			}

			if v.status.HasReachedErrorCount() {
				v.stopFlag.SetSoft(true)
				return errors.New("validation job stopped due to errors")
			}

		default:
			// Perform regular validation
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
	}

	return nil
}

func (v *Validation) Name() string {
	return "validation_" + v.table.Name
}
