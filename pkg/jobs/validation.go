package jobs

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/generators/statements"
	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
)

type Validation struct {
	logger       *zap.Logger
	schema       *typedef.Schema
	store        store.Store
	random       *rand.Rand
	globalStatus *status.GlobalStatus
	failFast     bool
}

func NewValidation(
	logger *zap.Logger,
	schema *typedef.Schema,
	store store.Store,
	random *rand.Rand,
	globalStatus *status.GlobalStatus,
	failFast bool,
) *Validation {
	return &Validation{
		logger:       logger.Named("validation"),
		schema:       schema,
		store:        store,
		random:       random,
		globalStatus: globalStatus,
		failFast:     failFast,
	}
}

func (v *Validation) Name() string {
	return "Validation"
}

func (v *Validation) validate(ctx context.Context, generator generators.Interface, table *typedef.Table) error {

	err := v.validation(ctx, table, generator)

	switch {
	case err == nil:
		v.globalStatus.ReadOps.Add(1)
	case errors.Is(err, context.Canceled):
		return context.Canceled
	default:
		v.globalStatus.AddReadError(&joberror.JobError{
			Timestamp: time.Now(),
			//StmtType:  stmt.QueryType.String(),
			Message: "Validation failed: " + err.Error(),
		})

		if v.failFast && v.globalStatus.HasErrors() {
			return err
		}
	}

	return err
}

func (v *Validation) Do(ctx context.Context, generator generators.Interface, table *typedef.Table) error {
	v.logger.Info("starting validation loop")
	defer v.logger.Info("ending validation loop")

	for {
		select {
		case <-ctx.Done():
			v.logger.Info("Context Done...")
			return nil
		default:
		}

		if err := v.validate(ctx, generator, table); errors.Is(err, context.Canceled) {
			return nil
		}

		if v.failFast && v.globalStatus.HasErrors() {
			return nil
		}
	}
}

func (v *Validation) validation(
	ctx context.Context,
	table *typedef.Table,
	generator generators.Interface,
) error {
	partitionRangeConfig := v.schema.Config.GetPartitionRangeConfig()
	stmt, cleanup := statements.GenCheckStmt(v.schema, table, generator, v.random, &partitionRangeConfig)
	defer cleanup()

	if w := v.logger.Check(zap.DebugLevel, "validation statement"); w != nil {
		prettyCQL, prettyCQLErr := stmt.PrettyCQL()
		if prettyCQLErr != nil {
			return PrettyCQLError{
				PrettyCQL: prettyCQLErr,
			}
		}

		w.Write(zap.String("pretty_cql", prettyCQL))
	}

	maxAttempts := v.schema.Config.AsyncObjectStabilizationAttempts
	delay := time.Duration(0)
	if maxAttempts < 1 {
		maxAttempts = 1
	}

	var err error

	for attempt := 1; ; attempt++ {
		select {
		case <-ctx.Done():
			v.logger.Info("Context Done... validation exiting")
			return context.Canceled
		case <-time.After(delay):
			delay = v.schema.Config.AsyncObjectStabilizationDelay
		}

		err = v.store.Check(ctx, table, stmt, attempt == maxAttempts)

		if err == nil {
			if attempt > 1 {
				v.logger.Info(fmt.Sprintf("Validation successfully completed on %d attempt.", attempt))
			}
			return nil
		}

		if errors.Is(err, context.Canceled) {
			return context.Canceled
		}

		if attempt == maxAttempts {
			if attempt > 1 {
				v.logger.Info(fmt.Sprintf("Retring failed validation stoped by reach of max attempts %d. Error: %s", maxAttempts, err))
			} else {
				v.logger.Info(fmt.Sprintf("Validation failed. Error: %s", err))
			}

			return err
		}

		v.logger.Info(fmt.Sprintf("Retring failed validation. %d attempt from %d attempts. Error: %s", attempt, maxAttempts, err))
	}
}
