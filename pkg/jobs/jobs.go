// Copyright 2019 ScyllaDB
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
	"time"

	"go.uber.org/zap"
	"golang.org/x/exp/rand"
	"golang.org/x/sync/errgroup"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
)

type Runner struct {
	duration     time.Duration
	logger       *zap.Logger
	random       *rand.Rand
	stopFlag     *stop.Flag
	workers      uint64
	generators   []*generators.Generator
	schema       *typedef.Schema
	failFast     bool
	schemaCfg    *typedef.SchemaConfig
	warmup       time.Duration
	globalStatus *status.GlobalStatus
	pump         <-chan time.Duration
	store        store.Store
	mode         Mode
}

type Job interface {
	Name() string
	Do(context.Context, generators.Interface, *typedef.Table) error
}

func New(
	mode string,
	duration time.Duration,
	workers uint64,
	logger *zap.Logger,
	schema *typedef.Schema,
	store store.Store,
	globalStatus *status.GlobalStatus,
	schemaCfg *typedef.SchemaConfig,
	seed uint64,
	gens []*generators.Generator,
	failFast bool,
	warmup time.Duration,
) *Runner {
	return &Runner{
		warmup:       warmup,
		globalStatus: globalStatus,
		pump:         NewPump(stopFlag, logger.Named("Pump")),
		store:        store,
		mode:         ModeFromString(mode),
		logger:       logger,
		schemaCfg:    schemaCfg,
		duration:     duration,
		workers:      workers,
		stopFlag:     stopFlag,
		failFast:     failFast,
		random:       rand.New(rand.NewSource(seed)),
		generators:   gens,
		schema:       schema,
	}
}

func (l *Runner) Name() string {
	return "Runner"
}

func (l *Runner) Run(ctx context.Context) error {
	ctx = l.stopFlag.CancelContextOnSignal(ctx, stop.SignalHardStop)
	partitionRangeConfig := l.schemaCfg.GetPartitionRangeConfig()

	l.logger.Info("start jobs")

	if l.warmup > 0 {
		l.logger.Info("Warmup Job Started",
			zap.Int("duration", int(l.warmup.Seconds())),
			zap.Int("workers", int(l.workers)),
		)
		time.AfterFunc(l.warmup, func() {
			l.logger.Info("jobs time is up, begins jobs completion")
			l.stopFlag.SetSoft(true)
		})

		warmup := func(_ <-chan time.Duration, rnd *rand.Rand) Job {
			return NewWarmup(l.logger, l.schema, l.store, &partitionRangeConfig, l.globalStatus, l.schemaCfg, l.stopFlag, rnd, l.failFast)
		}

		if err := l.start(ctx, warmup); err != nil {
			return err
		}
	}

	time.AfterFunc(l.duration, func() {
		l.logger.Info("jobs time is up, begins jobs completion")
		l.stopFlag.SetSoft(true)
	})

	if l.mode.IsWrite() {
		return l.start(ctx, func(pump <-chan time.Duration, rnd *rand.Rand) Job {
			return NewMutation(
				l.logger.Named("Mutation"),
				l.schema,
				l.store,
				&partitionRangeConfig,
				l.globalStatus,
				l.stopFlag,
				rnd,
				l.schemaCfg,
				pump,
				l.failFast,
			)
		})
	}

	return l.start(ctx, func(pump <-chan time.Duration, rnd *rand.Rand) Job {
		return NewValidation(
			l.logger,
			pump,
			l.schema, l.schemaCfg,
			l.store,
			rnd,
			&partitionRangeConfig,
			l.globalStatus,
			l.stopFlag,
			l.failFast,
		)
	})
}

func (l *Runner) start(ctx context.Context, job func(<-chan time.Duration, *rand.Rand) Job) error {
	g, gCtx := errgroup.WithContext(ctx)

	g.SetLimit(int(l.workers))

	partitionRangeConfig := l.schemaCfg.GetPartitionRangeConfig()

	for j, table := range l.schema.Tables {
		gen := l.generators[j]
		pump := NewPump(l.stopFlag, l.logger.Named("Pump-"+table.Name))
		rnd := rand.New(rand.NewSource(l.random.Uint64()))

		v := NewValidation(l.logger, pump, l.schema, l.schemaCfg, l.store, rnd, &partitionRangeConfig, l.globalStatus, l.stopFlag, l.failFast)
		j := job(pump, rnd)

		g.TryGo(func() error {
			return v.Do(gCtx, gen, table)
		})

		for i := 0; i < int(l.workers)-1; i++ {
			g.TryGo(func() error {
				return j.Do(gCtx, gen, table)
			})
		}
	}

	return g.Wait()
}
