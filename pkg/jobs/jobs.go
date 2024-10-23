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

type Mode []string

const (
	WriteMode  = "write"
	ReadMode   = "read"
	MixedMode  = "mixed"
	WarmupMode = "warmup"
)

func ModeFromString(m string) Mode {
	switch m {
	case WriteMode:
		return Mode{WriteMode}
	case ReadMode:
		return Mode{ReadMode}
	case MixedMode:
		return Mode{WriteMode, ReadMode}
	case WarmupMode:
		return Mode{WarmupMode}
	default:
		return Mode{}
	}
}

type List struct {
	name       string
	duration   time.Duration
	logger     *zap.Logger
	random     *rand.Rand
	stopFlag   *stop.Flag
	workers    uint64
	jobs       []Job
	generators []*generators.Generator
	schema     *typedef.Schema
	verbose    bool
	failFast   bool
}

type Job interface {
	Name() string
	Do(context.Context, generators.Interface) error
}

func New(
	mode string,
	duration time.Duration,
	workers uint64,
	logger *zap.Logger,
	schema *typedef.Schema,
	table *typedef.Table,
	store store.Store,
	globalStatus *status.GlobalStatus,
	schemaCfg *typedef.SchemaConfig,
	seed uint64,
	gens []*generators.Generator,
	pump <-chan time.Duration,
	failFast bool,
	verbose bool,
) List {
	partitionRangeConfig := schemaCfg.GetPartitionRangeConfig()
	rnd := rand.New(rand.NewSource(seed))

	jobs := make([]Job, 0, 2)
	name := "work cycle"
	for _, m := range ModeFromString(mode) {
		switch m {
		case WriteMode:
			jobs = append(jobs, NewMutation(logger, schema, table, store, &partitionRangeConfig, globalStatus, stopFlag, schemaCfg, pump, failFast, verbose))
		case ReadMode:
			jobs = append(jobs, NewValidation(logger, pump, schema, schemaCfg, table, store, rnd, &partitionRangeConfig, globalStatus, stopFlag, failFast))
		case WarmupMode:
			jobs = append(jobs, NewWarmup(logger, schema, table, store, &partitionRangeConfig, globalStatus, stopFlag, failFast, verbose))
			name = "warmup cycle"
		}
	}

	return List{
		name:       name,
		jobs:       jobs,
		duration:   duration,
		workers:    workers,
		stopFlag:   stopFlag,
		failFast:   failFast,
		verbose:    verbose,
		random:     rnd,
		generators: gens,
		schema:     schema,
	}
}

func (l List) Name() string {
	return l.name
}

func (l List) Do(ctx context.Context) error {
	ctx = l.stopFlag.CancelContextOnSignal(ctx, stop.SignalHardStop)
	g, gCtx := errgroup.WithContext(ctx)
	time.AfterFunc(l.duration, func() {
		l.logger.Info("jobs time is up, begins jobs completion")
		l.stopFlag.SetSoft(true)
	})

	l.logger.Info("start jobs")

	for j := range l.schema.Tables {
		gen := l.generators[j]
		for i := 0; i < int(l.workers); i++ {
			for idx := range l.jobs {
				jobF := l.jobs[idx]
				g.Go(func() error {
					return jobF.Do(gCtx, gen)
				})
			}
		}
	}

	return g.Wait()
}
