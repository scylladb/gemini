// Copyright 2025 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scylla

import (
	"bufio"
	"context"
	"encoding/json/jsontext"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

const (
	committerBatchSize      = 64
	statementFileBufferSize = 32 * 1024
	statementDirPerm        = 0o755
	statementFilePerm       = 0o644
	// Delay before fetching statements for a job error to ensure all statements are persisted and ready
	statementErrorFetchDelay = 30 * time.Second
	// How long Close waits for queued statement fetches before it cancels them.
	statementFetchDrainTimeout = 2 * time.Minute
	// Per-batch timeout for inserting statement logs into the Scylla logger
	// cluster. Without this, a stalled Scylla cluster (e.g. during a nemesis)
	// hangs the committer forever, fills the LogStmt channel and freezes every
	// gocql observer callback — wedging all mutation/validation workers.
	committerBatchTimeout = 10 * time.Second
)

type (
	Logger struct {
		// fetchCtx bounds the error-statement fetches only. The committer must not
		// share it: it drains its channel to the end on shutdown.
		//nolint:containedctx
		fetchCtx context.Context
		logger   *zap.Logger
		channel  <-chan stmtlogger.Item
		// pairs carries the rows from the pairing stage to the committers.
		pairs chan logPair
		logs  logSession
		// statements holds one *cqlStatements per source table, keyed by
		// "keyspace.table". The logger is shared across every table in the
		// schema, so each item is routed to its own table's _logs table whose
		// partition-key columns match the item's PartitionKeys. Using a single
		// fixed table's columns (the old behavior) mis-bound and dropped every
		// statement from tables other than Tables[0].
		statements map[string]*cqlStatements
		// queries holds the CQL text of every query id the committers wrote.
		queries *queryDict
		// sinks holds the statements file per cluster side, or is empty when file
		// logging is off.
		sinks       map[stmtlogger.Type]*lineSink
		fetchCancel context.CancelFunc
		fetchHook   func(ctx context.Context, ty stmtlogger.Type, jobError *joberror.JobError, w io.Writer) (int64, error)
		// openHook is openStatementFile outside tests.
		openHook func(name string) (*bufio.Writer, func() error, error)
		wg       sync.WaitGroup
		// fetchDelay is statementErrorFetchDelay outside tests.
		fetchDelay        time.Duration
		malformedWarnOnce sync.Once
		curWorkers        atomic.Int32
	}

	// PartitionInfo represents a single partition with its keys and validation data
	PartitionInfo struct {
		PartitionKeys   map[string]any                          `json:"partitionKeys"`
		LastValidations map[string]joberror.PartitionValidation `json:"lastValidations,omitempty"`
	}

	Line struct {
		PartitionKeys     []PartitionInfo  `json:"partitionKeys"`
		Timestamp         time.Time        `json:"timestamp"`
		Err               string           `json:"err,omitempty"`
		Query             string           `json:"query"`
		Message           string           `json:"message"`
		MutationFragments []jsontext.Value `json:"mutationFragments"`
		Statements        []jsontext.Value `json:"statements"`
		// Partial marks a line whose history was cut short by a read failure.
		Partial bool `json:"partial,omitzero"`
	}
)

func New(
	schema *typedef.Schema,
	oracleSession, testSession func() (*gocql.Session, error),
	hosts []string,
	port int,
	dockerMode bool,
	username, password string,
	repl replication.Replication,
	ch <-chan stmtlogger.Item,
	oracleStatementsFile string,
	testStatementsFile string,
	e <-chan *joberror.JobError,
	l *zap.Logger,
) (*Logger, error) {
	originalKeyspace := schema.Keyspace.Name
	keyspace := GetScyllaStatementLogsKeyspace(originalKeyspace)

	l.Debug("creating scylla logger",
		zap.String("keyspace", originalKeyspace),
		zap.Int("tables", len(schema.Tables)),
	)

	session, err := newSession(hosts, port, dockerMode, username, password, l)
	if err != nil {
		return nil, err
	}

	// Drop and recreate the shared _logs keyspace once for a clean slate; the
	// per-table loop below only creates each table, not the keyspace.
	l.Debug("dropping existing statement logs keyspace", zap.String("keyspace", keyspace))
	if err = session.Query("DROP KEYSPACE IF EXISTS " + keyspace).Exec(); err != nil {
		session.Close()
		return nil, err
	}

	l.Debug("creating statement logs keyspace", zap.String("keyspace", keyspace))
	if err = session.Query(buildCreateKeyspaceQuery(keyspace, repl)).Exec(); err != nil {
		session.Close()
		return nil, err
	}

	if err = session.Query(buildCreateQueriesTableQuery(keyspace)).Exec(); err != nil {
		session.Close()
		return nil, err
	}

	queries := &queryDict{}

	// Build one _logs table + cqlStatements per source table. Binding each
	// item's partition keys against its own table's columns is what keeps
	// statements from tables other than Tables[0] from being mis-bound and
	// dropped (statement_logger_malformed_total).
	statements := make(map[string]*cqlStatements, len(schema.Tables))
	for _, sourceTable := range schema.Tables {
		l.Debug("creating statement logs table",
			zap.String("source_table", sourceTable.Name),
			zap.String("table", GetScyllaStatementLogsTable(sourceTable.Name)),
		)

		cqlStmts, stmtsErr := newTableStatements(session, oracleSession, testSession,
			keyspace, GetScyllaStatementLogsTable(sourceTable.Name),
			originalKeyspace,
			sourceTable.Name,
			sourceTable.PartitionKeys, repl)
		if stmtsErr != nil {
			session.Close()
			return nil, stmtsErr
		}

		cqlStmts.queries = queries
		statements[originalKeyspace+"."+sourceTable.Name] = cqlStmts
	}

	l.Debug("waiting for schema agreement")
	if err = session.AwaitSchemaAgreement(context.Background()); err != nil {
		session.Close()
		return nil, fmt.Errorf("failed to await schema agreement for keyspace logs: %w", err)
	}
	l.Debug("schema agreement reached")

	logger := &Logger{
		channel:    ch,
		logger:     l,
		logs:       gocqlLogSession{session: session},
		statements: statements,
		queries:    queries,
	}

	// Open the sinks before any worker exists. A failure here returns no logger,
	// so nothing is left to close the session or release workers blocked on ch.
	logFiles := oracleStatementsFile != "" || testStatementsFile != ""
	if logFiles {
		if err = logger.openSinks(oracleStatementsFile, testStatementsFile); err != nil {
			session.Close()
			return nil, err
		}
	} else {
		l.Debug("statement file logging disabled: no files provided")
	}

	// Start a fixed-size worker pool to insert statement logs into Scylla.
	l.Debug("starting committer worker pool")
	workerCount := max(runtime.GOMAXPROCS(0), 2)
	logger.startCommitters(workerCount)
	logger.curWorkers.Store(int32(workerCount))

	if logFiles {
		l.Debug("starting error logger goroutine")
		logger.wg.Add(1)

		go logger.fetchErrors(e)
	}

	return logger, nil
}

func (s *Logger) openSinks(oracleStatements, testStatements string) error {
	s.sinks = make(map[stmtlogger.Type]*lineSink, 2)
	s.fetchCtx, s.fetchCancel = context.WithCancel(context.Background())

	if s.fetchDelay == 0 {
		s.fetchDelay = statementErrorFetchDelay
	}

	// A slice, not a map: the second file must not open before the first, or a
	// failure would report the wrong file and the order would vary per run.
	files := []struct {
		ty   stmtlogger.Type
		name string
	}{
		{stmtlogger.TypeOracle, oracleStatements},
		{stmtlogger.TypeTest, testStatements},
	}

	for _, f := range files {
		if f.name == "" {
			continue
		}

		open := s.openHook
		if open == nil {
			open = s.openStatementFile
		}

		w, closer, err := open(f.name)
		if err != nil {
			s.logger.Error("failed to open statements file",
				zap.Error(err),
				zap.String("file", f.name),
				zap.String("type", string(f.ty)),
			)

			// Give back whatever already opened, or its descriptor is lost: the
			// caller gets no logger and cannot close it.
			s.closeSinks()
			s.sinks = nil
			s.fetchCancel()
			s.fetchCtx, s.fetchCancel = nil, nil //nolint:fatcontext // clears the field, no context is derived

			return err
		}

		s.sinks[f.ty] = newLineSink(f.name, w, closer)
	}

	return nil
}

func (s *Logger) closeSinks() {
	for ty, sink := range s.sinks {
		if err := sink.Close(); err != nil {
			s.logger.Error("failed to close statements file",
				zap.Error(err),
				zap.String("file", sink.name),
				zap.String("type", string(ty)),
			)
		}
	}
}

// writeErrorStatements streams one job error's statement history straight into
// the statements file for ty. Nothing between the CQL iterator and the file
// holds the whole history.
func (s *Logger) writeErrorStatements(ctx context.Context, ty stmtlogger.Type, jobError *joberror.JobError) {
	sink, ok := s.sinks[ty]
	if !ok {
		return
	}

	fetch := s.fetchHook

	if fetch == nil {
		cql := s.statementsForQuery(jobError.Query)
		if cql == nil {
			s.logger.Error("no statement logs table for job error; cannot fetch failed statements",
				zap.String("query", jobError.Query),
				zap.String("job_error_hash", jobError.HashHex()),
			)

			return
		}

		fetch = cql.FetchTo
	}

	var written int64

	err := sink.Write(ctx, func(w io.Writer) error {
		n, err := fetch(ctx, ty, jobError, w)
		written = n

		return err
	})

	switch {
	case err == nil:
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		switch {
		case written == 0:
			// Shutdown cut the queue short before the line started.
			s.logger.Debug("statement fetch cancelled before it reached the file",
				zap.String("type", string(ty)),
				zap.String("job_error_hash", jobError.HashHex()),
			)

			return
		case !isReadError(err):
			// The file stopped taking bytes part way through the line, so the
			// line is not a record of anything.
			s.logger.Error("shutdown cut the statements file short",
				zap.Error(err),
				zap.String("file", sink.name),
				zap.String("type", string(ty)),
				zap.String("job_error_hash", jobError.HashHex()),
			)

			return
		default:
			// The line is complete on disk and carries the partial marker, so it
			// counts like any other short history.
			s.logger.Warn("shutdown cut the statement history short",
				zap.Error(err),
				zap.String("type", string(ty)),
				zap.String("job_error_hash", jobError.HashHex()),
			)
		}
	case isReadError(err):
		// The lines that were written are complete and flushed. Only the content
		// of one partition is missing, so this is not a file failure.
		s.logger.Warn("failed to read statements for job error",
			zap.Error(err),
			zap.String("type", string(ty)),
			zap.String("job_error_hash", jobError.HashHex()),
		)
	default:
		// The file did not take the line, or the flush failed. Nothing here
		// reached the disk in a usable state, so nothing is counted.
		s.logger.Error("failed to write failed statements",
			zap.Error(err),
			zap.String("file", sink.name),
			zap.String("type", string(ty)),
			zap.String("job_error_hash", jobError.HashHex()),
		)

		return
	}

	if written == 0 {
		return
	}

	metrics.StatementLoggerFlushes.WithLabelValues(string(ty) + "_file").Inc()
	RecordErrorMetrics(jobError, string(ty))
}

func (s *Logger) fetchErrors(ch <-chan *joberror.JobError) {
	metrics.WorkersCurrent.WithLabelValues("scylla_logger_error_fetcher").Inc()
	defer metrics.WorkersCurrent.WithLabelValues("scylla_logger_error_fetcher").Dec()
	defer s.wg.Done()
	seen := make(map[[32]byte]bool)
	wg := &sync.WaitGroup{}

	defer func() {
		wg.Wait()
		s.closeSinks()
	}()

	storages := []stmtlogger.Type{stmtlogger.TypeTest, stmtlogger.TypeOracle}

	for jobErr := range ch {
		hash := jobErr.Hash()
		if val, ok := seen[hash]; ok && val {
			continue
		}

		seen[hash] = true
		// Wait before fetching statements related to this error to allow
		// all relevant statements to be synced and ready.
		for _, ty := range storages {
			wg.Add(1)

			go func(ty stmtlogger.Type) {
				defer wg.Done()

				timer := time.NewTimer(s.fetchDelay)
				defer timer.Stop()

				// The delay and the fetch both watch fetchCtx. A partition scan is
				// unbounded, so without this Close would wait for every queued fetch
				// to page through its whole history.
				select {
				case <-timer.C:
				case <-s.fetchCtx.Done():
					return
				}

				s.writeErrorStatements(s.fetchCtx, ty, jobErr)
			}(ty)
		}
	}
}

// statementsFor routes an item to the _logs statements for the table it came
// from. Returns nil when the item carries no/unknown table identity (the
// committer drops and counts such items).
func (s *Logger) statementsFor(it stmtlogger.Item) *cqlStatements {
	if s.statements == nil {
		return nil
	}
	return s.statements[it.Table]
}

// statementsForQuery routes a job error to the _logs statements for the table
// its query targets. Returns nil when the table cannot be determined.
func (s *Logger) statementsForQuery(query string) *cqlStatements {
	if s.statements == nil {
		return nil
	}
	return s.statements[utils.KeyspaceTableFromQuery(query)]
}

// maxArgsCap returns the largest per-table insert arity so committer buffers are
// sized to fit any table's bind values without re-growing.
func (s *Logger) maxArgsCap() int {
	maxCap := len(logColumns)
	for _, c := range s.statements {
		if n := c.argsCap(); n > maxCap {
			maxCap = n
		}
	}
	return maxCap
}

// boundRow is a row that bind made ready for a batch.
type boundRow struct {
	// statement is the CQL text of a query id that no committer wrote yet. The
	// caller writes its queries row with the row. It is empty otherwise.
	statement string
	cql       *cqlStatements
	args      []any
	id        int64
}

// bind routes it to its table's _logs statements and binds its arguments into
// dst. It returns true when the row is ready to enqueue. It returns false when
// the item must be skipped: schema and SELECT statements are skipped silently,
// while unknown-table or partition-key bind-arity mismatches are additionally
// counted as malformed (and warned once) before being dropped.
func (s *Logger) bind(pair logPair, dst []any) (boundRow, bool) {
	it := pair.shared()
	if it.StatementType.IsSchema() || it.StatementType.IsSelect() {
		return boundRow{args: dst[:0]}, false
	}

	cql := s.statementsFor(*it)
	if cql == nil {
		s.recordMalformed(*it)
		return boundRow{args: dst[:0]}, false
	}

	id := queryID(it.Statement)

	args, ok := cql.fillPairArgs(dst, pair, id)
	if !ok {
		s.recordMalformed(*it)
		return boundRow{args: dst[:0]}, false
	}

	row := boundRow{cql: cql, args: args, id: id}
	if s.queries != nil && s.queries.remember(id, it.Statement) {
		row.statement = it.Statement
	}

	return row, true
}

// startCommitters starts the pairing stage and workerCount committers that
// read its pairs.
func (s *Logger) startCommitters(workerCount int) {
	s.pairs = make(chan logPair, committerBatchSize*workerCount)

	s.wg.Add(1)
	go s.pairWorker()

	for range workerCount {
		s.wg.Add(1)
		go s.insertWorker()
	}
}

// pendingPair is a pair that waits for its second cluster's item.
type pendingPair struct {
	since time.Time
	pair  logPair
}

// pairWorker joins the test and oracle items of one execution into one row. It
// is the only reader of the item channel, so both items of an execution meet
// here whichever committer writes the row. Schema and SELECT items are not
// logged, so they are dropped here instead of waiting for a partner.
func (s *Logger) pairWorker() {
	defer s.wg.Done()
	defer close(s.pairs)

	pending := make(map[pairKey]*pendingPair)
	ticker := time.NewTicker(pairWindow / 4)
	defer ticker.Stop()

	emit := func(pair logPair) {
		if !pair.complete() {
			metrics.StatementLoggerUnpairedTotal.Inc()
		}
		s.pairs <- pair
	}

	for {
		select {
		case it, ok := <-s.channel:
			if !ok {
				for _, p := range pending {
					emit(p.pair)
				}
				return
			}

			metrics.StatementLoggerItems.Dec()
			metrics.StatementLoggerDequeuedTotal.Inc()

			if it.StatementType.IsSchema() || it.StatementType.IsSelect() {
				continue
			}

			if it.Origin == nil {
				emit(pairOf(it))
				continue
			}

			key := keyOf(it)
			p, held := pending[key]
			switch {
			case !held:
				pending[key] = &pendingPair{pair: pairOf(it), since: time.Now()}
			case !p.pair.set(it):
				// The same cluster reported this execution twice. Write the
				// held pair and wait for a partner for the new item.
				emit(p.pair)
				p.pair, p.since = pairOf(it), time.Now()
			case p.pair.complete():
				emit(p.pair)
				delete(pending, key)
			}
		case now := <-ticker.C:
			for key, p := range pending {
				if now.Sub(p.since) >= pairWindow {
					emit(p.pair)
					delete(pending, key)
				}
			}
		}
	}
}

// logSession writes the committer's batches and single rows to the _logs
// keyspace. gocqlLogSession is the only production implementation.
type logSession interface {
	makeBatch(ctx context.Context) *gocql.Batch
	execBatch(ctx context.Context, batch *gocql.Batch) error
	execQuery(ctx context.Context, stmt string, args ...any) error
}

// gocqlLogSession runs every write on the shared _logs session.
type gocqlLogSession struct {
	session *gocql.Session
}

func (g gocqlLogSession) makeBatch(ctx context.Context) *gocql.Batch {
	return g.session.BatchWithContext(ctx, gocql.UnloggedBatch)
}

func (g gocqlLogSession) execBatch(_ context.Context, batch *gocql.Batch) error {
	return g.session.ExecuteBatch(batch)
}

func (g gocqlLogSession) execQuery(ctx context.Context, stmt string, args ...any) error {
	q := g.session.QueryWithContext(ctx, stmt, args...)
	defer q.Release()
	return q.Exec()
}

// executeBatchWithFallback tries to execute the batch and, in case of failure,
// falls back to executing individual queries to reduce data loss.
func (s *Logger) executeBatchWithFallback(ctx context.Context, batch *gocql.Batch, held [][]any, heldInsert []string, count int) {
	if len(batch.Entries) == 0 {
		return
	}

	err := s.logs.execBatch(ctx, batch)

	if err == nil || errors.Is(err, context.Canceled) {
		return
	}

	// If the batch failed because its deadline elapsed (or the ctx is otherwise
	// already done) the downstream _logs cluster is stalled. The row-by-row
	// fallback below would fire up to committerBatchSize individual queries on
	// the SAME expired ctx — every one fails instantly, producing a burst of
	// log spam, and when the cluster is merely slow (not fully stalled) those
	// extra writes amplify load on the very cluster that is already behind.
	// Skip the fallback in that case; the items are dropped (bounded loss) and
	// the upstream overflow back-pressure handles the rest.
	if errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
		s.logger.Error("statements batch insert timed out; skipping row-by-row fallback (downstream _logs cluster stalled)",
			zap.Error(err),
			zap.Int("batch_size", len(batch.Entries)),
		)
		return
	}

	s.logger.Error("failed to insert batch into statements table",
		zap.Error(err),
		zap.Int("batch_size", len(batch.Entries)),
	)

	for i := range count {
		// Stop the moment the ctx expires mid-fallback — the remaining
		// per-item queries would only pile more doomed work onto a slow cluster.
		if ctx.Err() != nil {
			return
		}
		if err = s.logs.execQuery(ctx, heldInsert[i], held[i]...); err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Error("failed to insert item after batch failure", zap.Error(err))
		}
	}
}

func (s *Logger) insertWorker() {
	metrics.WorkersCurrent.WithLabelValues("scylla_logger_inserter").Inc()
	defer func() {
		s.logger.Debug("committer worker exiting")
		metrics.WorkersCurrent.WithLabelValues("scylla_logger_inserter").Dec()
		s.curWorkers.Add(-1)
		s.wg.Done()
	}()

	// Per-batch context deadline (committerBatchTimeout) is what keeps a
	// stalled Scylla logger cluster (e.g. during a nemesis) from blocking
	// the worker indefinitely. Before this, ExecuteBatch with
	// context.Background() would hang forever, fill the upstream LogStmt
	// channel, and freeze every gocql observer callback (and thus every
	// mutation/validation worker). Root cause of the 2026-04-30 hang.
	parentCtx := context.Background()

	s.logger.Debug("committer worker started")
	argsCap := s.maxArgsCap()
	// One spare slot: the last row of a batch can bring the queries row of its
	// statement with it.
	held := make([][]any, committerBatchSize+1)
	// heldInsert[i] records which per-table _logs INSERT held[i] was bound for,
	// so the row-by-row fallback re-executes each row against the right table.
	heldInsert := make([]string, len(held))
	for i := range held {
		held[i] = make([]any, 0, argsCap)
	}
	idx := 0

	for {
		// Block until a row arrives or the pairing stage closes its channel.
		first, ok := <-s.pairs
		if !ok {
			return
		}

		// Per-batch context with a hard deadline. Without this, a stalled
		// Scylla logger cluster causes ExecuteBatch to block forever,
		// filling LogStmt's channel and freezing every gocql observer
		// callback (and thus every mutation/validation worker).
		ctx, cancel := context.WithTimeout(parentCtx, committerBatchTimeout)
		batch := s.logs.makeBatch(ctx)

		// Helper to add an item to the batch
		addItem := func(pair logPair) {
			// Route the row to its own table's _logs statements and bind its
			// args. bind() drops (and counts) unknown-table and arity-mismatched
			// rows instead of poisoning the batch — a single bad row would make
			// gocql reject the whole batch client-side.
			row, valid := s.bind(pair, held[idx])
			held[idx] = row.args // retain any grown backing array for reuse
			if !valid {
				return
			}
			heldInsert[idx] = row.cql.insertStmt
			batch.Query(row.cql.insertStmt, held[idx]...)
			idx++

			if row.statement != "" {
				held[idx] = append(held[idx][:0], row.id, row.statement)
				heldInsert[idx] = row.cql.queriesInsert
				batch.Query(row.cql.queriesInsert, held[idx]...)
				idx++
			}
		}

		addItem(first)

		// Best-effort drain up to maxBatch-1 additional items without blocking
		exiting := false
		draining := true
		for draining && idx < committerBatchSize {
			select {
			case it, chOpen := <-s.pairs:
				if !chOpen {
					// channel closed, process what we have then exit after this batch
					exiting = true
					draining = false
					break
				}
				addItem(it)
			default:
				draining = false
			}
		}
		// Execute current batch with fallback
		s.executeBatchWithFallback(ctx, batch, held, heldInsert, idx)
		cancel()
		idx = 0
		if exiting {
			return
		}
	}
}

// insert inserts a single statement log entry. It is safe to call even when
// the logger is only partially initialized in tests. Schema and SELECT
// statements are ignored to mirror production behavior.
func (s *Logger) insert(it stmtlogger.Item) {
	// Update dequeued metrics as insertWorker does when consuming from channel.
	metrics.StatementLoggerItems.Dec()
	metrics.StatementLoggerDequeuedTotal.Inc()

	// Route to the item's table statements and bind its args. bind() skips
	// schema/select silently and drops (counts) unknown-table/arity-mismatch
	// items. Safe even when statements is nil (as in some unit tests).
	if it.StatementType.IsSchema() || it.StatementType.IsSelect() {
		return
	}

	row, ok := s.bind(pairOf(it), make([]any, 0, s.maxArgsCap()))
	if !ok {
		return
	}
	if row.statement != "" {
		_ = s.logs.execQuery(context.Background(), row.cql.queriesInsert, row.id, row.statement)
	}
	_ = s.logs.execQuery(context.Background(), row.cql.insertStmt, row.args...)
}

// recordMalformed counts a dropped log item whose partition-key bind arity did
// not match the _logs INSERT, and warns ONCE per process with the offending
// statement so the buggy upstream generator can be found. With the per-partition
// emission fix in pkg/store/observers.go this path should be unreachable; a
// non-zero statement_logger_malformed_total (or this warning) means a generator
// regressed and is emitting statements without fully-populated partition keys.
// The warning is one-shot to avoid log flooding; the metric counts every drop.
func (s *Logger) recordMalformed(it stmtlogger.Item) {
	metrics.StatementLoggerMalformedTotal.Inc()
	s.malformedWarnOnce.Do(func() {
		s.logger.Warn(
			"dropping statement log item: partition-key bind arity mismatch "+
				"(an upstream generator emitted a statement without fully-populated "+
				"partition keys); further drops counted by statement_logger_malformed_total",
			zap.String("statement", it.Statement),
			zap.String("statement_type", it.StatementType.String()),
		)
	})
}

func (s *Logger) openStatementFile(name string) (*bufio.Writer, func() error, error) {
	// Create parent directories if they don't exist
	dir := filepath.Dir(name)
	if err := os.MkdirAll(dir, statementDirPerm); err != nil {
		return nil, nil, fmt.Errorf("failed to create directory for statements file '%q': %w", name, err)
	}

	// NOTE: the perm argument must be a plain Unix permission. fs.ModeExclusive
	// (bit 1<<29) is NOT a permission bit; passing it here is a no-op on native
	// Linux/APFS (the kernel masks it) but makes some overlay/FUSE-backed mounts
	// — e.g. Docker Desktop bind mounts on macOS — reject the open with ENOENT,
	// which then panics the flusher and crashes the whole run. Use 0o644 only.
	file, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, statementFilePerm)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open statements file '%q': %w", name, err)
	}

	buffered := bufio.NewWriterSize(file, statementFileBufferSize)

	return buffered, func() error {
		defer func() {
			if err = file.Close(); err != nil {
				s.logger.Error("failed to close statements file", zap.String("file", name), zap.Error(err))
			}
		}()

		if err = buffered.Flush(); err != nil {
			return err
		}

		if err = file.Sync(); err != nil {
			s.logger.Error("failed to fsync statements file", zap.String("file", name), zap.Error(err))

			return err
		}

		return nil
	}, nil
}

func (s *Logger) Close() error {
	// NOTE: we deliberately do NOT cancel s.ctx here. The upstream
	// stmtlogger.Logger.Close() closes s.channel after flushing its
	// overflow buffer; insertWorker drains the channel until it sees the
	// close signal, guaranteeing no item is lost. The per-batch timeout
	// (committerBatchTimeout) is what keeps a stalled Scylla logger
	// cluster from blocking the worker indefinitely.
	if s.fetchCancel == nil {
		s.wg.Wait()
		return nil
	}

	// Give the queued statement fetches a bounded window to finish. Each one
	// pages through a whole partition under the sink lock, so without the cutoff
	// shutdown waits for the sum of every outstanding scan.
	done := make(chan struct{})

	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(statementFetchDrainTimeout):
		s.logger.Warn("statement fetches did not finish in time; cancelling",
			zap.Duration("timeout", statementFetchDrainTimeout),
		)
		s.fetchCancel()
		<-done
	}

	s.fetchCancel()

	return nil
}
