package jobs

import (
	"context"
	"time"

	"github.com/scylladb/gemini/pkg/generators"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/generators/statements"
	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
)

type (
	Mutation struct {
		logger   *zap.Logger
		mutation mutation
		stopFlag *stop.Flag
		pump     <-chan time.Duration
		failFast bool
		verbose  bool
	}

	mutation struct {
		logger               *zap.Logger
		schema               *typedef.Schema
		table                *typedef.Table
		store                store.Store
		partitionRangeConfig *typedef.PartitionRangeConfig
		schemaCfg            *typedef.SchemaConfig
		globalStatus         *status.GlobalStatus
		random               *rand.Rand
		deletes              bool
	}
)

func NewMutation(
	logger *zap.Logger,
	schema *typedef.Schema,
	table *typedef.Table,
	store store.Store,
	partitionRangeConfig *typedef.PartitionRangeConfig,
	globalStatus *status.GlobalStatus,
	stopFlag *stop.Flag,
	schemaCfg *typedef.SchemaConfig,
	pump <-chan time.Duration,
	failFast bool,
	verbose bool,
) *Mutation {
	return &Mutation{
		logger: logger.Named("mutation"),
		mutation: mutation{
			logger:               logger.Named("mutation-with-deletes"),
			schema:               schema,
			table:                table,
			store:                store,
			partitionRangeConfig: partitionRangeConfig,
			globalStatus:         globalStatus,
			deletes:              true,
			schemaCfg:            schemaCfg,
		},
		stopFlag: stopFlag,
		pump:     pump,
		failFast: failFast,
		verbose:  verbose,
	}
}

func (m *Mutation) Name() string {
	return "Mutation"
}

func (m *Mutation) Do(ctx context.Context, generator generators.Interface) error {
	m.logger.Info("starting mutation loop")
	defer m.logger.Info("ending mutation loop")

	for {
		if m.stopFlag.IsHardOrSoft() {
			return nil
		}
		select {
		case <-m.stopFlag.SignalChannel():
			m.logger.Debug("mutation job terminated")
			return nil
		case hb := <-m.pump:
			time.Sleep(hb)
		}

		var err error

		if m.mutation.ShouldDoDDL() {
			err = m.mutation.DDL(ctx)
		} else {
			err = m.mutation.Statement(ctx, generator)
		}

		if err != nil {
			// TODO: handle error
		}

		if m.failFast && m.mutation.HasErrors() {
			m.stopFlag.SetSoft(true)
			return nil
		}
	}
}

func (m *mutation) Statement(ctx context.Context, generator generators.Interface) error {
	mutateStmt, err := statements.GenMutateStmt(m.schema, m.table, generator, m.random, m.partitionRangeConfig, m.deletes)
	if err != nil {
		m.logger.Error("Failed! Mutation statement generation failed", zap.Error(err))
		m.globalStatus.WriteErrors.Add(1)
		return err
	}
	if mutateStmt == nil {
		if w := m.logger.Check(zap.DebugLevel, "no statement generated"); w != nil {
			w.Write(zap.String("job", "mutation"))
		}
		return err
	}

	if w := m.logger.Check(zap.DebugLevel, "mutation statement"); w != nil {
		prettyCQL, prettyCQLErr := mutateStmt.PrettyCQL()
		if prettyCQLErr != nil {
			return PrettyCQLError{
				PrettyCQL: prettyCQLErr,
				Stmt:      mutateStmt,
				Err:       err,
			}
		}

		w.Write(zap.String("pretty_cql", prettyCQL))
	}
	if err = m.store.Mutate(ctx, mutateStmt); err != nil {
		if errors.Is(err, context.Canceled) {
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
	if m.schemaCfg.CQLFeature != typedef.CQL_FEATURE_ALL {
		return false
	}

	ind := m.random.Intn(1000000)
	return ind%100000 == 0
}
