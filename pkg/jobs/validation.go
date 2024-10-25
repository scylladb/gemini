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
	"github.com/scylladb/gemini/pkg/utils"
)

type Validation struct {
	logger       *zap.Logger
	pump         <-chan time.Duration
	schema       *typedef.Schema
	store        store.Store
	random       *rand.Rand
	globalStatus *status.GlobalStatus
	failFast     bool
}

func NewValidation(
	logger *zap.Logger,
	pump <-chan time.Duration,
	schema *typedef.Schema,
	store store.Store,
	random *rand.Rand,
	globalStatus *status.GlobalStatus,
	failFast bool,
) *Validation {
	return &Validation{
		logger:       logger.Named("validation"),
		pump:         pump,
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
	partitionRangeConfig := v.schema.Config.GetPartitionRangeConfig()
	stmt, cleanup := statements.GenCheckStmt(v.schema, table, generator, v.random, &partitionRangeConfig)
	defer cleanup()

	err := validation(ctx, &v.schema.Config, table, v.store, stmt, v.logger)

	switch {
	case err == nil:
		v.globalStatus.ReadOps.Add(1)
	case errors.Is(err, context.Canceled):
		return context.Canceled
	default:
		query, prettyErr := stmt.PrettyCQL()
		if prettyErr != nil {
			return PrettyCQLError{
				PrettyCQL: prettyErr,
				Stmt:      stmt,
				Err:       err,
			}
		}

		v.globalStatus.AddReadError(&joberror.JobError{
			Timestamp: time.Now(),
			StmtType:  stmt.QueryType.String(),
			Message:   "Validation failed: " + err.Error(),
			Query:     query,
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
			return nil
		case hb := <-v.pump:
			time.Sleep(hb)
		}

		if err := v.validate(ctx, generator, table); errors.Is(err, context.Canceled) {
			return nil
		}

		if v.failFast && v.globalStatus.HasErrors() {
			return nil
		}
	}
}

func validation(
	ctx context.Context,
	sc *typedef.SchemaConfig,
	table *typedef.Table,
	s store.Store,
	stmt *typedef.Stmt,
	logger *zap.Logger,
) error {
	if w := logger.Check(zap.DebugLevel, "validation statement"); w != nil {
		prettyCQL, prettyCQLErr := stmt.PrettyCQL()
		if prettyCQLErr != nil {
			return PrettyCQLError{
				PrettyCQL: prettyCQLErr,
				Stmt:      stmt,
			}
		}

		w.Write(zap.String("pretty_cql", prettyCQL))
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

	var lastErr, err error
	attempt := 1
	for ; ; attempt++ {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			logger.Info(fmt.Sprintf("Retring failed validation stoped by done context. %d attempt from %d attempts. Error: %s", attempt, maxAttempts, err))
			return nil
		}

		lastErr = err
		err = s.Check(ctx, table, stmt, attempt == maxAttempts)

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
		if errors.Is(err, utils.UnwrapErr(lastErr)) {
			logger.Info(fmt.Sprintf("Retring failed validation. %d attempt from %d attempts. Error same as at attempt before. ", attempt, maxAttempts))
		} else {
			logger.Info(fmt.Sprintf("Retring failed validation. %d attempt from %d attempts. Error: %s", attempt, maxAttempts, err))
		}
	}

	if attempt > 1 {
		logger.Info(fmt.Sprintf("Retring failed validation stoped by reach of max attempts %d. Error: %s", maxAttempts, err))
	} else {
		logger.Info(fmt.Sprintf("Validation failed. Error: %s", err))
	}

	return err
}
