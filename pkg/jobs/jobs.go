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
	"errors"
	"sync"
	"time"

	"go.uber.org/zap"
	"golang.org/x/exp/rand"

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
		generators   *generators.Generators
		schema       *typedef.Schema
		warmup       time.Duration
		globalStatus *status.GlobalStatus
		store        store.Store
		mode         Mode
		failFast     bool
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
	gens *generators.Generators,
	warmup time.Duration,
	failFast bool,
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
	var wg sync.WaitGroup

	if l.warmup > 0 {
		l.logger.Info("Warmup Job Started",
			zap.Int("duration", int(l.warmup.Seconds())),
			zap.Int("workers", int(l.workers)),
		)

		warmupCtx, cancel := context.WithTimeout(ctx, l.warmup)
		defer cancel()
		l.startMutation(warmupCtx, cancel, &wg, l.random, "Warmup", false, false)
		wg.Wait()
	}

	ctx, cancel := context.WithTimeout(ctx, l.duration+1*time.Second)
	defer cancel()

	src := rand.NewSource(l.random.Uint64())

	if l.mode.IsRead() {
		l.startValidation(ctx, &wg, cancel, src)
	}

	if l.mode.IsWrite() {
		l.startMutation(ctx, cancel, &wg, src, "Mutation", true, true)
	}

	wg.Wait()

	return nil
}

func (l *Runner) startMutation(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, src rand.Source, name string, deletes, ddl bool) {
	logger := l.logger.Named(name)

	err := l.start(ctx, cancel, wg, rand.New(src), func(rnd *rand.Rand) Job {
		return NewMutation(
			logger,
			l.schema,
			l.store,
			l.globalStatus,
			rnd,
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

func (l *Runner) startValidation(ctx context.Context, wg *sync.WaitGroup, cancel context.CancelFunc, src rand.Source) {
	err := l.start(ctx, cancel, wg, rand.New(src), func(rnd *rand.Rand) Job {
		return NewValidation(
			l.logger,
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

func (l *Runner) start(ctx context.Context, cancel context.CancelFunc, wg *sync.WaitGroup, rnd *rand.Rand, job func(*rand.Rand) Job) error {
	wg.Add(int(l.workers))

	for _, table := range l.schema.Tables {
		gen := l.generators.Get()
		for range l.workers {
			j := job(rand.New(rand.NewSource(rnd.Uint64())))
			go func(j Job) {
				defer wg.Done()
				if err := j.Do(ctx, gen, table); err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}

					l.logger.Error("job failed", zap.String("table", table.Name), zap.Error(err))

					if l.failFast {
						cancel()
					}
				}
			}(j)
		}
	}

	return nil
}
