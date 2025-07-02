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
	"math/rand/v2"
	"time"

	"go.uber.org/multierr"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/generators/statements"
	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
)

type Validation struct {
	table       *typedef.Table
	statement   *statements.Generator
	generator   generators.Interface
	status      *status.GlobalStatus
	stopFlag    *stop.Flag
	store       store.Store
	maxAttempts int
	delay       time.Duration
}

func NewValidation(
	keyspace string,
	table *typedef.Table,
	schemaConfig typedef.SchemaConfig,
	generator generators.Interface,
	status *status.GlobalStatus,
	stopFlag *stop.Flag,
	store store.Store,
	seed [32]byte,
) *Validation {
	maxAttempts := schemaConfig.AsyncObjectStabilizationAttempts
	delay := schemaConfig.AsyncObjectStabilizationDelay
	pc := schemaConfig.GetPartitionRangeConfig()

	if maxAttempts <= 1 {
		maxAttempts = 10
	}

	if delay <= 10*time.Millisecond {
		delay = 200 * time.Millisecond
	}

	statementGenerator := statements.New(
		keyspace,
		generator,
		table,
		rand.New(rand.NewChaCha8(seed)),
		&pc,
		schemaConfig.UseLWT,
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

func (v *Validation) Do(ctx context.Context) error {
	name := v.Name()

	executionTime := metrics.ExecutionTimeStart(name)

	for !v.stopFlag.IsHardOrSoft() {
		err := executionTime.RunFuncE(func() error {
			var acc error

			stmt := v.statement.Select(ctx)
			if stmt == nil {
				if v.status.HasErrors() {
					v.stopFlag.SetSoft(true)
				}
				return ErrNoStatement
			}

			for attempt := 1; attempt <= v.maxAttempts; attempt++ {
				err := v.store.Check(ctx, v.table, stmt, attempt)
				if err == nil {
					v.status.ReadOp()
					return nil
				}

				if errors.Is(err, context.DeadlineExceeded) {
					continue
				}

				if errors.Is(err, context.Canceled) {
					return nil
				}

				if attempt == v.maxAttempts {
					return joberror.JobError{
						Timestamp:     time.Now(),
						Err:           acc,
						StmtType:      stmt.QueryType,
						Message:       "Validation failed:" + err.Error(),
						Query:         stmt.Query,
						PartitionKeys: stmt.PartitionKeys.Values,
					}
				}

				acc = multierr.Append(acc, err)
				time.Sleep(v.delay)
			}

			return nil
		})

		if errors.Is(err, ErrNoStatement) {
			time.Sleep(v.delay)
			continue
		}

		var jobErr joberror.JobError
		if errors.As(err, &jobErr) {
			v.status.AddReadError(jobErr)
		}

		if v.status.HasErrors() {
			v.stopFlag.SetSoft(true)
			return errors.New("validation job stopped due to errors")
		}
	}

	return nil
}

func (v *Validation) Name() string {
	return "validation_" + v.table.Name
}
