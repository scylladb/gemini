package jobs

import (
	"context"
	"golang.org/x/exp/rand"

	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
)

type Warmup struct {
	mutation mutation
	logger   *zap.Logger
	stopFlag *stop.Flag
	failFast bool
}

func NewWarmup(
	logger *zap.Logger,
	schema *typedef.Schema,
	store store.Store,
	partitionRangeConfig *typedef.PartitionRangeConfig,
	globalStatus *status.GlobalStatus,
	schemaCfg *typedef.SchemaConfig,
	stopFlag *stop.Flag,
	rnd *rand.Rand,
	failFast bool,
) *Warmup {
	return &Warmup{
		logger: logger.Named("mutation"),
		mutation: mutation{
			logger:               logger.Named("mutation-without-deletes"),
			schema:               schema,
			store:                store,
			partitionRangeConfig: partitionRangeConfig,
			schemaCfg:            schemaCfg,
			globalStatus:         globalStatus,
			random:               rnd,
			deletes:              false,
		},
		stopFlag: stopFlag,
		failFast: failFast,
	}
}

func (w *Warmup) Name() string {
	return "Warmup"
}

func (w *Warmup) Do(ctx context.Context, generator generators.Interface, table *typedef.Table) error {
	w.logger.Info("starting warmup loop")
	defer w.logger.Info("ending warmup loop")

	for {
		if w.stopFlag.IsHardOrSoft() {
			w.logger.Debug("warmup job terminated")
			return nil
		}

		_ = w.mutation.Statement(ctx, generator, table)
		if w.failFast && w.mutation.globalStatus.HasErrors() {
			w.stopFlag.SetSoft(true)
			return nil
		}
	}
}
