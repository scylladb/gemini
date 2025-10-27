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
	"math/rand/v2"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/partitions"
	"github.com/scylladb/gemini/pkg/statements"
	"github.com/scylladb/gemini/pkg/status"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/store"
	"github.com/scylladb/gemini/pkg/typedef"
)

const (
	WriteMode  = "write"
	ReadMode   = "read"
	MixedMode  = "mixed"
	WarmupMode = "warmup"
)

type Jobs struct {
	store               store.Store
	schema              *typedef.Schema
	status              *status.GlobalStatus
	logger              *zap.Logger
	random              *rand.ChaCha8
	ratioController     *statements.RatioController
	name                string
	mutationConcurrency int
	readConcurrency     int
}

var ErrNoStatement = errors.New("no statement generated")

type Worker interface {
	Name() string
	Do(context.Context) error
}

func New(
	mutationConcurrency, readConcurrency int,
	schema *typedef.Schema,
	st store.Store,
	globalStatus *status.GlobalStatus,
	ratioController *statements.RatioController,
	logger *zap.Logger,
	src *rand.ChaCha8,
) *Jobs {
	logger.Info("creating jobs",
		zap.Int("mutation_concurrency", mutationConcurrency),
		zap.Int("read_concurrency", readConcurrency),
	)

	return &Jobs{
		schema:              schema,
		store:               st,
		status:              globalStatus,
		logger:              logger,
		random:              src,
		readConcurrency:     readConcurrency,
		mutationConcurrency: mutationConcurrency,
		ratioController:     ratioController,
	}
}

func (j *Jobs) parseMode(mode string) []string {
	var modes []string
	switch mode {
	case MixedMode:
		modes = []string{WriteMode, ReadMode}
	case WriteMode:
		modes = []string{WriteMode}
	case ReadMode:
		modes = []string{ReadMode}
	case WarmupMode:
		modes = []string{WarmupMode}
	}

	return modes
}

func (j *Jobs) Run(
	base context.Context,
	duration time.Duration,
	idxFunc distributions.DistributionFunc,
	partsCount uint64,
	partsConfig typedef.PartitionRangeConfig,
	stopFlag *stop.Flag,
	mode string,
) error {
	log := j.logger.Named(j.name)
	log.Info("start jobs", zap.String("mode", mode))
	defer log.Info("stop jobs")

	g, gCtx := errgroup.WithContext(base)

	for _, table := range j.schema.Tables {
		generator := partitions.New(gCtx, j.random, idxFunc, table, partsConfig, partsCount)
		log.Debug("processing table", zap.String("table", table.Name))

		time.AfterFunc(duration+1*time.Second, func() {
			log.Debug("warmup phase timeout reached, setting soft stop")
			stopFlag.SetSoft(false)
		})

		for _, m := range j.parseMode(mode) {
			switch m {
			case WriteMode, WarmupMode:
				log.Debug("starting mutation workers",
					zap.String("mode", m),
					zap.Int("count", j.mutationConcurrency),
				)
				for i := range j.mutationConcurrency {
					newSrc := [32]byte{}
					_, _ = j.random.Read(newSrc[:])

					mutation := NewMutation(
						j.schema,
						table,
						generator,
						j.status,
						j.ratioController,
						stopFlag,
						j.store,
						mode != WarmupMode,
						newSrc,
					)

					workerID := i
					g.Go(func() error {
						log.Debug("mutation worker started", zap.Int("worker_id", workerID))
						err := mutation.Do(base)
						if err != nil && !errors.Is(err, context.Canceled) {
							log.Error("mutation worker finished with error",
								zap.Int("worker_id", workerID),
								zap.Error(err),
							)
						} else {
							log.Debug("mutation worker finished", zap.Int("worker_id", workerID))
						}
						return err
					})
				}
			case ReadMode:
				log.Debug("starting validation workers",
					zap.String("mode", m),
					zap.Int("count", j.readConcurrency),
				)
				for i := range j.readConcurrency {
					newSrc := [32]byte{}
					_, _ = j.random.Read(newSrc[:])

					validation := NewValidation(
						j.schema,
						table,
						generator,
						j.status,
						j.ratioController,
						stopFlag,
						j.store,
						newSrc,
					)

					workerID := i
					g.Go(func() error {
						log.Debug("validation worker started", zap.Int("worker_id", workerID))
						err := validation.Do(gCtx)
						if err != nil && !errors.Is(err, context.Canceled) {
							log.Error("validation worker finished with error",
								zap.Int("worker_id", workerID),
								zap.Error(err),
							)
						} else {
							log.Debug("validation worker finished", zap.Int("worker_id", workerID))
						}
						return err
					})
				}
			}
		}
	}

	log.Info("waiting for all workers to complete")
	return g.Wait()
}

//nolint
// mutationJob continuously applies mutations against the database
// for as long as the pump is active.
//func mutationJob(ctx context.Context, stmtGen *statements.Generator, globalStatus *status.GlobalStatus, logger *zap.Logger, stopFlag *stop.Flag) error {
//	for !stopFlag.IsHardOrSoft() {
//		metrics.ExecutionTime("mutation_job", func() {
//			if schemaCfg.CQLFeature == typedef.CQL_FEATURE_ALL && r.IntN(1000000)%100000 == 0 {
//				_ = ddl(ctx, globalStatus, logger)
//				return
//			}
//
//			_ = mutation(ctx, globalStatus, true, logger)
//		})
//
//		if globalStatus.HasReachedErrorCount() {
//			stopFlag.SetSoft(true)
//			return nil
//		}
//	}
//
//	return nil
//}
