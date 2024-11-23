package jobs

import (
	"context"
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

type (
	Mutation struct {
		logger   *zap.Logger
		mutation mutation
		failFast bool
	}

	mutation struct {
		logger       *zap.Logger
		schema       *typedef.Schema
		store        store.Store
		globalStatus *status.GlobalStatus
		random       *rand.Rand
		deletes      bool
		ddl          bool
	}
)

func NewMutation(
	logger *zap.Logger,
	schema *typedef.Schema,
	store store.Store,
	globalStatus *status.GlobalStatus,
	rnd *rand.Rand,
	failFast bool,
	deletes bool,
	ddl bool,
) *Mutation {
	return &Mutation{
		logger: logger,
		mutation: mutation{
			logger:       logger.Named("mutation-with-deletes"),
			schema:       schema,
			store:        store,
			globalStatus: globalStatus,
			deletes:      deletes,
			random:       rnd,
			ddl:          ddl,
		},
		failFast: failFast,
	}
}

func (m *Mutation) Name() string {
	return "Mutation"
}

func (m *Mutation) Do(ctx context.Context, generator generators.Interface, table *typedef.Table) error {
	m.logger.Info("starting mutation loop")
	defer m.logger.Info("ending mutation loop")

	for {
		select {
		case <-ctx.Done():
			m.logger.Debug("mutation job terminated")
			return context.Canceled
		default:
		}

		var err error

		if m.mutation.ShouldDoDDL() {
			err = m.mutation.DDL(ctx, table)
		} else {
			err = m.mutation.Statement(ctx, generator, table)
		}

		if err != nil {
			return errors.WithStack(err)
		}
	}
}

func (m *mutation) Statement(ctx context.Context, generator generators.Interface, table *typedef.Table) error {
	partitionRangeConfig := m.schema.Config.GetPartitionRangeConfig()
	mutateStmt, err := statements.GenMutateStmt(m.schema, table, generator, m.random, &partitionRangeConfig, m.deletes)
	if err != nil {
		m.logger.Error("Failed! Mutation statement generation failed", zap.Error(err))
		m.globalStatus.WriteErrors.Add(1)
		return errors.WithStack(err)
	}

	if w := m.logger.Check(zap.DebugLevel, "mutation statement"); w != nil {
		prettyCQL, prettyCQLErr := mutateStmt.PrettyCQL()
		if prettyCQLErr != nil {
			return errors.WithStack(PrettyCQLError{
				PrettyCQL: prettyCQLErr,
				Stmt:      mutateStmt,
				Err:       err,
			})
		}

		w.Write(zap.String("pretty_cql", prettyCQL))
	}

	if err = m.store.Mutate(ctx, mutateStmt); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil
		}

		prettyCQL, prettyCQLErr := mutateStmt.PrettyCQL()
		if prettyCQLErr != nil {
			return errors.WithStack(PrettyCQLError{
				PrettyCQL: prettyCQLErr,
				Stmt:      mutateStmt,
				Err:       err,
			})
		}

		m.globalStatus.AddWriteError(&joberror.JobError{
			Timestamp: time.Now(),
			StmtType:  mutateStmt.QueryType.String(),
			Message:   "Mutation failed: " + err.Error(),
			Query:     prettyCQL,
		})

		return err
	}

	m.globalStatus.WriteOps.Add(1)
	generator.GiveOld(mutateStmt.ValuesWithToken...)

	return nil
}

func (m *mutation) HasErrors() bool {
	return m.globalStatus.HasErrors()
}

func (m *mutation) ShouldDoDDL() bool {
	if m.ddl && m.schema.Config.CQLFeature == typedef.CQL_FEATURE_ALL {
		ind := m.random.Intn(100000)
		return ind < 100
	}

	return false
}
