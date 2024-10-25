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

	"github.com/scylladb/gemini/pkg/burst"
	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
)

type (
	Runner struct {
		duration     time.Duration
		logger       *zap.Logger
		random       *rand.Rand
		workers      uint64
		generators   []*generators.Generator
		schema       *typedef.Schema
		failFast     bool
		warmup       time.Duration
		globalStatus *status.GlobalStatus
		store        store.Store
		mode         Mode
	}
	Job interface {
		Name() string
		Do(context.Context, generators.Interface, *typedef.Table) error
	}
)

func New(
	mode string,
	duration time.Duration,
	workers uint64,
	logger *zap.Logger,
	schema *typedef.Schema,
	store store.Store,
	globalStatus *status.GlobalStatus,
	seed uint64,
	gens []*generators.Generator,
	failFast bool,
	warmup time.Duration,
) *Runner {
	return &Runner{
		warmup:       warmup,
		globalStatus: globalStatus,
		store:        store,
		mode:         ModeFromString(mode),
		logger:       logger,
		duration:     duration,
		workers:      workers,
		failFast:     failFast,
		random:       rand.New(rand.NewSource(seed)),
		generators:   gens,
		schema:       schema,
	}
}

func (l *Runner) Run(ctx context.Context) error {
	l.logger.Info("start jobs")

	if l.warmup > 0 {
		l.logger.Info("Warmup Job Started",
			zap.Int("duration", int(l.warmup.Seconds())),
			zap.Int("workers", int(l.workers)),
		)

		warmupCtx, cancel := context.WithTimeout(ctx, l.warmup)
		defer cancel()
		l.startMutation(warmupCtx, cancel, l.random, "Warmup", false, false)
	}

	ctx, cancel := context.WithTimeout(ctx, l.duration+1*time.Second)
	defer cancel()

	src := rand.NewSource(l.random.Uint64())

	if l.mode.IsRead() {
		go l.startValidation(ctx, cancel, src)
	}

	if l.mode.IsWrite() {
		l.startMutation(ctx, cancel, src, "Mutation", true, true)
	}

	return nil
}

func (l *Runner) startMutation(ctx context.Context, cancel context.CancelFunc, src rand.Source, name string, deletes, ddl bool) {
	logger := l.logger.Named(name)

	err := l.start(ctx, rand.New(src), func(pump <-chan time.Duration, rnd *rand.Rand) Job {
		return NewMutation(
			logger,
			l.schema,
			l.store,
			l.globalStatus,
			rnd,
			pump,
			l.failFast,
			deletes,
			ddl,
		)
	})

	if err != nil {
		logger.Error("Mutation job failed", zap.Error(err))
		if l.failFast {
			cancel()
		}
	}
}

func (l *Runner) startValidation(ctx context.Context, cancel context.CancelFunc, src rand.Source) {
	err := l.start(ctx, rand.New(src), func(pump <-chan time.Duration, rnd *rand.Rand) Job {
		return NewValidation(
			l.logger,
			pump,
			l.schema,
			l.store,
			rnd,
			l.globalStatus,
			l.failFast,
		)
	})

	if err != nil {
		l.logger.Error("Validation job failed", zap.Error(err))
		if l.failFast {
			cancel()
		}
	}
}

func (l *Runner) start(ctx context.Context, rnd *rand.Rand, job func(<-chan time.Duration, *rand.Rand) Job) error {
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(int(l.workers))

	for j, table := range l.schema.Tables {
		gen := l.generators[j]
		pump := burst.New(ctx, 10, 10*time.Millisecond)

		for range l.workers {
			src := rand.NewSource(rnd.Uint64())
			g.TryGo(func() error {
				return job(pump, rand.New(src)).Do(gCtx, gen, table)
			})
		}
	}

	return g.Wait()
}
