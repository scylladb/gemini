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
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/metrics"
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

// watchdogGracePeriod is the time after the configured run duration after
// which the watchdog will dump all goroutine stacks and forcibly exit the
// process. The deadline is duration + grace, giving normal shutdown plenty
// of headroom while still bounding total runtime so SCT does not have to
// wait out its own (multi-hour) timeout when gemini deadlocks.
const watchdogGracePeriod = 5 * time.Minute

// watchdogDump writes a full goroutine stack trace to a file in the current
// working directory and returns the path. It never panics; on failure it
// logs and returns the empty string. This is intentionally low-tech so it
// works even when the process is otherwise wedged.
func watchdogDump(log *zap.Logger) string {
	// Capture all goroutine stacks. Allocate generously; the second
	// argument true asks runtime.Stack to include every goroutine.
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		if len(buf) >= 1<<27 { // 128 MiB hard cap
			buf = buf[:n]
			break
		}
		buf = make([]byte, len(buf)*2)
	}

	path := fmt.Sprintf("gemini-watchdog-%d.dump", time.Now().Unix())
	f, err := os.Create(path)
	if err != nil {
		log.Error("watchdog could not create dump file", zap.String("path", path), zap.Error(err))
		return ""
	}
	defer func() {
		if cerr := f.Close(); cerr != nil {
			log.Error("watchdog could not close dump file", zap.String("path", path), zap.Error(cerr))
		}
	}()

	if _, err = f.Write(buf); err != nil {
		log.Error("watchdog could not write dump file", zap.String("path", path), zap.Error(err))
		return ""
	}
	return path
}

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
	maxErrors uint64,
) error {
	log := j.logger.Named(j.name)
	log.Info("start jobs", zap.String("mode", mode))
	defer log.Info("stop jobs")

	timeoutCtx, cancel := context.WithTimeout(base, duration+1*time.Second)
	defer cancel()

	g, gCtx := errgroup.WithContext(timeoutCtx)

	for _, table := range j.schema.Tables {
		generator := partitions.New(gCtx, j.random, idxFunc, table, partsConfig, partsCount, maxErrors)
		log.Debug("processing table", zap.String("table", table.Name))

		// Additionally, request a graceful stop of workers after the duration
		// so that loops depending on the stop flag can exit promptly.
		if duration > 0 {
			softTimer := time.AfterFunc(duration+1*time.Second, func() {
				log.Debug("workload timeout reached, setting soft stop (no parent propagation)")
				// Do not propagate to parent; each Run call manages its own lifecycle.
				stopFlag.SetSoft(false)
			})

			// Hard watchdog: if workers are still alive grace-period after
			// the deadline, we are deadlocked (most likely because some
			// downstream component back-pressured a goroutine that the
			// stop flag cannot reach — e.g. the 2026-04-30 statement
			// logger hang). Dump every goroutine stack to a file so the
			// next post-mortem takes minutes instead of days, then
			// os.Exit so SCT does not have to sit out its own 48h
			// timeout.
			watchdogDeadline := duration + watchdogGracePeriod
			watchdogTimer := time.AfterFunc(watchdogDeadline, func() {
				dumpPath := watchdogDump(log)
				log.Error("watchdog deadline exceeded — workers did not exit, forcing process exit",
					zap.Duration("deadline", watchdogDeadline),
					zap.String("goroutine_dump", dumpPath),
				)
				watchdogExit()
			})

			// Stop both timers once Run returns so they cannot fire
			// after the workload completes (important for tests and
			// sequential workloads that call Run multiple times).
			defer softTimer.Stop()
			defer watchdogTimer.Stop()
		}

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
						metrics.WorkersCurrent.WithLabelValues("mutation").Inc()
						defer metrics.WorkersCurrent.WithLabelValues("mutation").Dec()
						log.Debug("mutation worker started", zap.Int("worker_id", workerID))
						// Use the errgroup context so that mutation workers are
						// canceled together with the rest of the job set (like validation)
						// and respect error propagation uniformly in Mixed mode.
						err := mutation.Do(gCtx)
						switch {
						case err == nil || errors.Is(err, context.Canceled):
							log.Debug("mutation worker finished", zap.Int("worker_id", workerID))
							return nil
						case errors.Is(err, ErrMutationJobStopped):
							// Graceful shutdown due to error budget reached
							log.Debug("mutation worker finished (error budget reached)", zap.Int("worker_id", workerID))
							return nil
						default:
							log.Error("mutation worker finished with error",
								zap.Int("worker_id", workerID),
								zap.Error(err),
							)
							return err
						}
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
						metrics.WorkersCurrent.WithLabelValues("validation").Inc()
						defer metrics.WorkersCurrent.WithLabelValues("validation").Dec()

						log.Debug("validation worker started", zap.Int("worker_id", workerID))
						err := validation.Do(gCtx)
						switch {
						case err == nil || errors.Is(err, context.Canceled):
							log.Debug("validation worker finished", zap.Int("worker_id", workerID))
							return nil
						case errors.Is(err, ErrValidationJobStopped):
							// Graceful shutdown due to error budget reached
							log.Debug("validation worker finished (error budget reached)", zap.Int("worker_id", workerID))
							return nil
						default:
							log.Error("validation worker finished with error",
								zap.Int("worker_id", workerID),
								zap.Error(err),
							)
							return err
						}
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
