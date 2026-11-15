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
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"runtime"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/scylladb/gemini/pkg/concurrency"
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
	// limiter sets how many workers of each pool run at a time. The pools
	// start mutationConcurrency and readConcurrency workers, the maximums.
	limiter *concurrency.Limiter
	store   store.Store
	schema  *typedef.Schema
	status  *status.GlobalStatus
	logger  *zap.Logger
	random  rand.Source
	// indexFactory gives every worker its own partition index function, so a
	// draw takes no lock that the workers share. A nil indexFactory leaves
	// the workers on the shared function of the pool.
	indexFactory         distributions.Factory
	ratioController      *statements.RatioController
	name                 string
	mutationConcurrency  int
	readConcurrency      int
	targetedConsumeRatio float64
}

// ErrNoStatement is returned when no statement can be generated at this moment
// (e.g. no partition key is available yet). Callers back off briefly and retry.
var ErrNoStatement = errors.New("no statement generated")

// watchdogGracePeriod is the time after the configured run duration after
// which the watchdog will dump all goroutine stacks and forcibly exit the
// process. The deadline is duration + grace, giving normal shutdown plenty
// of headroom while still bounding total runtime so SCT does not have to
// wait out its own (multi-hour) timeout when gemini deadlocks.
const watchdogGracePeriod = 5 * time.Minute

// retryDrainGrace is how long the run context outlives the soft stop. The
// validation workers use it to finish the retries they already queued: a retry
// is a validation that already saw a difference, so dropping it at the end of
// the run would leave a possible divergence unreported. It must stay well
// below watchdogGracePeriod.
const retryDrainGrace = time.Minute

// seedWorker fills seed with fresh bytes of the run's source.
// workerPool returns the view of pool for one worker. With an indexFactory
// the worker draws partition indexes from its own source, seeded from the
// run's source.
func (j *Jobs) workerPool(pool *partitions.Partitions) partitions.Interface {
	if j.indexFactory == nil {
		return pool
	}

	var seed [32]byte
	j.seedWorker(&seed)

	return pool.ForWorker(j.indexFactory(seed))
}

// workerRatios returns a copy of the ratio controller with its own source,
// so the statement type draws of the workers share no lock.
func (j *Jobs) workerRatios() *statements.RatioController {
	var seed [32]byte
	j.seedWorker(&seed)

	return j.ratioController.WithRandom(rand.New(rand.NewChaCha8(seed)))
}

func (j *Jobs) seedWorker(seed *[32]byte) {
	for i := 0; i < len(seed); i += 8 {
		binary.LittleEndian.PutUint64(seed[i:], j.random.Uint64())
	}
}

// runDeadlines returns when the soft stop fires and when the run context ends,
// both measured from the start of the run. The context ends after the soft
// stop, so the workers can finish their queued retries. A run without a
// duration has no soft stop timer.
func runDeadlines(duration time.Duration) (softStop, contextEnd time.Duration) {
	softStop = duration + time.Second
	if duration <= 0 {
		return softStop, softStop
	}

	return softStop, softStop + retryDrainGrace
}

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
	Do(ctx context.Context) error
}

func New(
	mutationConcurrency, readConcurrency int,
	schema *typedef.Schema,
	st store.Store,
	globalStatus *status.GlobalStatus,
	ratioController *statements.RatioController,
	logger *zap.Logger,
	src rand.Source,
	ratios statements.Ratios,
	limiter *concurrency.Limiter,
	indexFactory distributions.Factory,
) *Jobs {
	logger.Info("creating jobs",
		zap.Int("mutation_concurrency", mutationConcurrency),
		zap.Int("read_concurrency", readConcurrency),
	)

	return &Jobs{
		schema:               schema,
		store:                st,
		status:               globalStatus,
		logger:               logger,
		random:               src,
		readConcurrency:      readConcurrency,
		mutationConcurrency:  mutationConcurrency,
		ratioController:      ratioController,
		targetedConsumeRatio: ratios.TargetedConsumeRatio(),
		limiter:              limiter,
		indexFactory:         indexFactory,
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

// Pools holds one partition pool per table of the schema, in the order of
// schema.Tables. The warmup and the main phase use the same pools, so the main
// phase validates the partitions that the warmup wrote.
type Pools []*partitions.Partitions

// Close stops the background work of every pool.
func (p Pools) Close() {
	for _, pool := range p {
		pool.Close()
	}
}

// minInvalidCap is the smallest invalid-partition cap of a pool.
const minInvalidCap = 1

// invalidCap returns how many partitions of a pool of count partitions can be
// marked invalid. Gemini marks a partition invalid when its own writes can
// have left the clusters different, or when it reported a divergence on it.
// The cap is half the pool, independent of the error budget. Write-side
// invalidations are not errors of the system under test, so they must not use
// up the budget that stops the run. Half the pool keeps most of the
// validation coverage, and the selection of a valid partition stays fast.
// The floor keeps the cap above zero, because zero means no cap.
func invalidCap(count uint64) uint64 {
	return max(count/2, minInvalidCap)
}

// NewPools builds one partition pool per table of the schema. ctx bounds the
// background work of the pools. The caller closes the pools after the last
// Run that uses them.
func (j *Jobs) NewPools(
	ctx context.Context,
	idxFunc distributions.DistributionFunc,
	partsCount uint64,
	partsConfig typedef.PartitionRangeConfig,
) Pools {
	pools := make(Pools, 0, len(j.schema.Tables))
	for _, table := range j.schema.Tables {
		pools = append(pools, partitions.New(ctx, j.random, idxFunc, table, partsConfig, partsCount, invalidCap(partsCount)))
	}

	return pools
}

// Run starts the workers of mode on pools, which NewPools built for the same
// schema, and waits for them. Run does not close pools, so a later Run can
// use them again.
func (j *Jobs) Run(
	base context.Context,
	duration time.Duration,
	pools Pools,
	stopFlag *stop.Flag,
	mode string,
) error {
	if len(pools) != len(j.schema.Tables) {
		return fmt.Errorf("jobs: %d partition pools for %d tables", len(pools), len(j.schema.Tables))
	}

	log := j.logger.Named(j.name)
	log.Info("start jobs", zap.String("mode", mode))
	defer log.Info("stop jobs")

	softStopAfter, contextEnd := runDeadlines(duration)

	timeoutCtx, cancel := context.WithTimeout(base, contextEnd)
	defer cancel()

	g, gCtx := errgroup.WithContext(timeoutCtx)

	// The limiter runs outside the errgroup: g.Wait returns when the workers
	// finish, and the limiter would keep it waiting.
	if j.limiter != nil {
		limiterCtx, stopLimiter := context.WithCancel(timeoutCtx)
		defer stopLimiter()
		go j.limiter.Run(limiterCtx, limiterInterval)
	}

	// Create exactly one soft-stop timer and one watchdog timer for this
	// Run() invocation, regardless of how many tables are in the schema.
	// Creating them inside the table loop would spawn N timer goroutines
	// all firing at the same absolute deadline, producing N dump files and
	// N racing os.Exit(2) calls.
	if duration > 0 {
		softTimer := time.AfterFunc(softStopAfter, func() {
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
		watchdogDeadline := contextEnd + watchdogGracePeriod
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

	for ti, table := range j.schema.Tables {
		generator := pools[ti]
		log.Debug("processing table", zap.String("table", table.Name))

		for _, m := range j.parseMode(mode) {
			switch m {
			case WriteMode, WarmupMode:
				log.Debug("starting mutation workers",
					zap.String("mode", m),
					zap.Int("count", j.mutationConcurrency),
				)
				for i := range j.mutationConcurrency {
					newSrc := [32]byte{}
					j.seedWorker(&newSrc)

					mutation := NewMutation(
						j.schema,
						table,
						j.workerPool(generator),
						j.status,
						j.workerRatios(),
						stopFlag,
						j.store,
						mode != WarmupMode,
						newSrc,
						log.Named("mutation").With(zap.String("table", table.Name), zap.Int("worker_id", i)),
					)
					mutation.limiter, mutation.workerIdx = j.limiter, i

					workerID := i
					g.Go(func() error {
						metrics.WorkersCurrent.WithLabelValues("mutation").Inc()
						defer metrics.WorkersCurrent.WithLabelValues("mutation").Dec()
						log.Debug("mutation worker started", zap.Int("worker_id", workerID))
						// A child of the errgroup context, so mutation workers are
						// canceled together with the rest of the job set (like
						// validation) and respect error propagation uniformly in
						// Mixed mode. See workerContext for why it is a child.
						workerCtx, cancelWorker := workerContext(gCtx)
						defer cancelWorker()
						err := mutation.Do(workerCtx)
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
					j.seedWorker(&newSrc)

					validation := NewValidation(
						j.schema,
						table,
						j.workerPool(generator),
						j.status,
						j.workerRatios(),
						stopFlag,
						j.store,
						newSrc,
						j.targetedConsumeRatio,
					)
					validation.limiter, validation.workerIdx = j.limiter, i

					workerID := i
					g.Go(func() error {
						metrics.WorkersCurrent.WithLabelValues("validation").Inc()
						defer metrics.WorkersCurrent.WithLabelValues("validation").Dec()

						log.Debug("validation worker started", zap.Int("worker_id", workerID))
						workerCtx, cancelWorker := workerContext(gCtx)
						defer cancelWorker()
						err := validation.Do(workerCtx)
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

// statementsGenerated holds statements_generated_total for every statement
// type, resolved once. The type label is "select" or "mutation", and the
// subtype label is the statement type.
var statementsGenerated = func() [typedef.StatementTypeCount]prometheus.Counter {
	var out [typedef.StatementTypeCount]prometheus.Counter
	for ty := range typedef.StatementTypeCount {
		category := "mutation"
		if ty.IsSelect() {
			category = "select"
		}
		out[ty] = metrics.StatementsGenerated.WithLabelValues(category, ty.String())
	}

	return out
}()

// workerContext gives a worker its own cancelable child of parent.
// context.WithValue and errgroup share the parent's Done channel, and gocql
// selects on ctx.Done() several times per query. With every worker on one
// channel, those selects queue on that channel's lock. A child has its own
// Done channel and still ends when parent ends.
func workerContext(parent context.Context) (context.Context, context.CancelFunc) {
	return context.WithCancel(parent)
}

// limiterInterval is how often the limiter adjusts the worker count.
const limiterInterval = time.Second
