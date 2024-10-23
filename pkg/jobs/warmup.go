package jobs

import (
	"context"

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
	verbose  bool
}

func NewWarmup(
	logger *zap.Logger,
	schema *typedef.Schema,
	table *typedef.Table,
	store store.Store,
	partitionRangeConfig *typedef.PartitionRangeConfig,
	globalStatus *status.GlobalStatus,
	stopFlag *stop.Flag,
	failFast bool,
	verbose bool,
) *Warmup {
	return &Warmup{
		logger: logger.Named("mutation"),
		mutation: mutation{
			logger:               logger.Named("mutation-without-deletes"),
			schema:               schema,
			table:                table,
			store:                store,
			partitionRangeConfig: partitionRangeConfig,
			globalStatus:         globalStatus,
			deletes:              false,
		},
		stopFlag: stopFlag,
		failFast: failFast,
		verbose:  verbose,
	}
}

func (w *Warmup) Name() string {
	return "Warmup"
}

func (w *Warmup) Do(ctx context.Context, generator generators.Interface) error {
	w.logger.Info("starting warmup loop")
	defer w.logger.Info("ending warmup loop")

	for {
		if w.stopFlag.IsHardOrSoft() {
			w.logger.Debug("warmup job terminated")
			return nil
		}

		_ = w.mutation.Statement(ctx, generator)
		if w.failFast && w.mutation.globalStatus.HasErrors() {
			w.stopFlag.SetSoft(true)
			return nil
		}
	}
}
