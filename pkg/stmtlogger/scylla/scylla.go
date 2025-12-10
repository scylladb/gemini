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
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
)

const (
	committerBatchSize            = 64
	statementChBuffer             = 1000
	statementFlusherFlushInterval = 750 * time.Millisecond
	statementFileBufferSize       = 32 * 1024
	statementDirPerm              = 0o755
	statementFilePerm             = 0o644
	// Delay before fetching statements for a job error to ensure all statements are persisted and ready
	statementErrorFetchDelay = 30 * time.Second
)

type (
	Logger struct {
		logger        *zap.Logger
		channel       <-chan stmtlogger.Item
		cqlStatements *cqlStatements
		fetchHook     func(ctx context.Context, ty stmtlogger.Type, jobError *joberror.JobError) (cqlDataMap, error)
		wg            sync.WaitGroup
		curWorkers    atomic.Int32
	}

	statementChData struct {
		// Use the binary hash as map key for internal processing. Do NOT try to JSON-encode
		// the whole structure directly in logs; only encode specific fields.
		Data  cqlDataMap         `json:"data"`
		Error *joberror.JobError `json:"error"`
		ty    stmtlogger.Type
	}

	Line struct {
		PartitionKeys     map[string][]any  `json:"partitionKeys"`
		Timestamp         time.Time         `json:"timestamp"`
		Err               string            `json:"err,omitempty"`
		Query             string            `json:"query"`
		Message           string            `json:"message"`
		MutationFragments []json.RawMessage `json:"mutationFragments"`
		Statements        []json.RawMessage `json:"statements"`
	}
)

func New(
	originalKeyspace string,
	originalTable string,
	oracleSession, testSession func() (*gocql.Session, error),
	hosts []string,
	username, password string,
	partitionKeys typedef.Columns,
	repl replication.Replication,
	ch <-chan stmtlogger.Item,
	oracleStatementsFile string,
	testStatementsFile string,
	e <-chan *joberror.JobError,
	l *zap.Logger,
) (*Logger, error) {
	l.Debug("creating scylla logger",
		zap.String("keyspace", originalKeyspace),
		zap.String("table", originalTable),
	)

	keyspace := GetScyllaStatementLogsKeyspace(originalKeyspace)
	table := GetScyllaStatementLogsTable(originalTable)

	createKeyspace, createTable := buildCreateTableQuery(keyspace, table, partitionKeys, repl)

	session, err := newSession(hosts, username, password, l)
	if err != nil {
		return nil, err
	}

	l.Debug("dropping existing statement logs keyspace", zap.String("keyspace", keyspace))
	if err = session.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", keyspace)).Exec(); err != nil {
		return nil, err
	}

	l.Debug("creating statement logs keyspace", zap.String("keyspace", keyspace))
	if err = session.Query(createKeyspace).Exec(); err != nil {
		return nil, err
	}

	l.Debug("creating statement logs table", zap.String("table", table))
	if err = session.Query(createTable).Exec(); err != nil {
		return nil, err
	}

	l.Debug("waiting for schema agreement")
	if err = session.AwaitSchemaAgreement(context.Background()); err != nil {
		return nil, errors.Wrap(err, "failed to await schema agreement for keyspace logs")
	}
	l.Debug("schema agreement reached")

	cqlStmts, err := newStatements(session, oracleSession, testSession,
		keyspace, table,
		originalKeyspace,
		originalTable,
		partitionKeys, repl)
	if err != nil {
		return nil, err
	}

	logger := &Logger{
		channel:       ch,
		logger:        l,
		cqlStatements: cqlStmts,
	}

	// Start a fixed-size worker pool to insert statement logs into Scylla.
	l.Debug("starting committer worker pool")
	workerCount := runtime.GOMAXPROCS(0)
	if workerCount < 2 {
		workerCount = 2
	}
	for range workerCount {
		go logger.insertWorker()
	}
	logger.curWorkers.Store(int32(workerCount))

	if oracleStatementsFile != "" || testStatementsFile != "" {
		l.Debug("starting error logger goroutine")
		statementCh := make(chan statementChData, statementChBuffer)
		go logger.fetchErrors(statementCh, e)
		go logger.statementFlusher(statementCh, oracleStatementsFile, testStatementsFile)
	} else {
		l.Debug("statement file logging disabled: no files provided")
	}

	return logger, nil
}

//nolint:gocyclo
func (s *Logger) statementFlusher(ch <-chan statementChData, oracleStatements, testStatements string) {
	s.wg.Add(1)
	defer s.wg.Done()

	metrics.WorkersCurrent.WithLabelValues("scylla_logger_statement_flusher").Inc()
	defer metrics.WorkersCurrent.WithLabelValues("scylla_logger_statement_flusher").Dec()

	var (
		oracleStatementsFile *bufio.Writer
		testStatementsFile   *bufio.Writer
		oracleCloser         func() error
		testCloser           func() error
		err                  error
	)

	if oracleStatements != "" {
		oracleStatementsFile, oracleCloser, err = s.openStatementFile(oracleStatements)
		if err != nil {
			s.logger.Error("failed to open oracle statements file",
				zap.Error(err),
				zap.String("file", oracleStatements),
				zap.String("type", string(stmtlogger.TypeOracle)),
			)
			panic(err)
		}
	}

	if testStatements != "" {
		testStatementsFile, testCloser, err = s.openStatementFile(testStatements)
		if err != nil {
			s.logger.Error("failed to open test statements file",
				zap.Error(err),
				zap.String("file", testStatements),
				zap.String("type", string(stmtlogger.TypeTest)),
			)
			panic(err)
		}
	}

	defer func() {
		if oracleCloser != nil {
			if err = oracleCloser(); err != nil {
				s.logger.Error("failed to close statements file",
					zap.Error(err),
					zap.String("file", oracleStatements),
					zap.String("type", string(stmtlogger.TypeOracle)),
				)
			}
		}

		if testCloser != nil {
			if err = testCloser(); err != nil {
				s.logger.Error("failed to close statements file",
					zap.Error(err),
					zap.String("file", testStatements),
					zap.String("type", string(stmtlogger.TypeTest)),
				)
			}
		}
	}()
	for {
		item, ok := <-ch

		if !ok {
			// finalize by flushing any remaining buffers
			if oracleStatementsFile != nil {
				if err = oracleStatementsFile.Flush(); err == nil {
					metrics.StatementLoggerFlushes.WithLabelValues("oracle_file").Inc()
				}
			}
			if testStatementsFile != nil {
				if err = testStatementsFile.Flush(); err == nil {
					metrics.StatementLoggerFlushes.WithLabelValues("test_file").Inc()
				}
			}
			return
		}

		for _, val := range item.Data {
			var bytes []byte
			errStr := ""
			if item.Error.Err != nil {
				errStr = item.Error.Err.Error()
			}
			l := Line{
				PartitionKeys:     val.partitionKeys,
				Timestamp:         item.Error.Timestamp,
				Err:               errStr,
				Query:             item.Error.Query,
				Message:           item.Error.Message,
				MutationFragments: val.mutationFragments,
				Statements:        val.statements,
			}

			bytes, err = json.Marshal(l)
			if err != nil {
				s.logger.Error("failed to marshal statement log",
					zap.Error(err),
					zap.String("type", string(item.ty)),
					zap.String("query", item.Error.Query),
					zap.String("message", item.Error.Message),
					zap.String("job_error_hash", item.Error.HashHex()),
				)
				continue
			}

			RecordErrorMetrics(item.Error, bytes, string(item.ty))
			var (
				writer   *bufio.Writer
				fileName string
			)

			switch item.ty {
			case stmtlogger.TypeOracle:
				writer = oracleStatementsFile
				fileName = oracleStatements
			case stmtlogger.TypeTest:
				writer = testStatementsFile
				fileName = testStatements
			default:
				s.logger.Error("unknown statement type", zap.String("type", string(item.ty)))
				continue
			}

			if writer == nil {
				continue // no file sink for this type
			}

			if _, err = writer.Write(bytes); err != nil {
				s.logger.Error("failed to write statement log",
					zap.Error(err),
					zap.String("type", string(item.ty)),
					zap.String("file", fileName),
					zap.String("data", string(bytes)),
				)
				continue
			}

			// Write newline for JSONL format
			if err = writer.WriteByte('\n'); err != nil {
				s.logger.Error("failed to write newline to statement log",
					zap.Error(err),
					zap.String("type", string(item.ty)),
					zap.String("file", fileName),
				)
				continue
			}
		}
	}
}

func (s *Logger) poolCallback(ctx context.Context, ty stmtlogger.Type, jobError *joberror.JobError) statementChData {
	var (
		data cqlDataMap
		err  error
	)
	if s.fetchHook != nil {
		data, err = s.fetchHook(ctx, ty, jobError)
	} else {
		data, err = s.cqlStatements.Fetch(ctx, ty, jobError)
	}
	if err != nil {
		s.logger.Error("failed to fetch failed statements",
			zap.Error(err),
			zap.Any("job_error", jobError),
		)

		return statementChData{}
	}

	return statementChData{
		ty:    ty,
		Data:  data,
		Error: jobError,
	}
}

func (s *Logger) fetchErrors(items chan<- statementChData, ch <-chan *joberror.JobError) {
	metrics.WorkersCurrent.WithLabelValues("scylla_logger_error_fetcher").Inc()
	defer metrics.WorkersCurrent.WithLabelValues("scylla_logger_error_fetcher").Dec()
	s.wg.Add(1)
	defer s.wg.Done()
	seen := make(map[[32]byte]bool)
	wg := &sync.WaitGroup{}

	defer func() {
		wg.Wait()
		close(items)
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
				time.Sleep(statementErrorFetchDelay)
				items <- s.poolCallback(context.Background(), ty, jobErr)
			}(ty)
		}
	}
}

// makeBatch creates a new gocql batch using test hooks when provided.
func (s *Logger) makeBatch(ctx context.Context) *gocql.Batch {
	if s.cqlStatements.newBatch != nil {
		return s.cqlStatements.newBatch(ctx)
	}

	return s.cqlStatements.session.BatchWithContext(ctx, gocql.UnloggedBatch)
}

// execBatch executes the provided batch using test hooks when provided.
func (s *Logger) execBatch(ctx context.Context, batch *gocql.Batch) error {
	if s.cqlStatements.execBatch != nil {
		return s.cqlStatements.execBatch(ctx, batch)
	}
	return s.cqlStatements.session.ExecuteBatch(batch)
}

// execQuery executes a single statement using test hooks when provided.
func (s *Logger) execQuery(ctx context.Context, stmt string, args ...any) error {
	if s.cqlStatements.execQuery != nil {
		return s.cqlStatements.execQuery(ctx, stmt, args...)
	}

	q := s.cqlStatements.session.QueryWithContext(ctx, stmt, args...)
	defer q.Release()
	return q.Exec()
}

// executeBatchWithFallback tries to execute the batch and, in case of failure,
// falls back to executing individual queries to reduce data loss.
func (s *Logger) executeBatchWithFallback(ctx context.Context, batch *gocql.Batch, held [][]any, count int) {
	if len(batch.Entries) == 0 {
		return
	}

	err := s.execBatch(ctx, batch)

	if err == nil || errors.Is(err, context.Canceled) {
		return
	}

	s.logger.Error("failed to insert batch into statements table",
		zap.Error(err),
		zap.Int("batch_size", len(batch.Entries)),
	)

	for i := range count {
		if err = s.execQuery(ctx, s.cqlStatements.insertStmt, held[i]...); err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Error("failed to insert item after batch failure", zap.Error(err))
		}
	}
}

func (s *Logger) insertWorker() {
	metrics.WorkersCurrent.WithLabelValues("scylla_logger_inserter").Inc()
	s.wg.Add(1)
	defer func() {
		s.logger.Debug("committer worker exiting")
		metrics.WorkersCurrent.WithLabelValues("scylla_logger_inserter").Dec()
		s.curWorkers.Add(-1)
		s.wg.Done()
	}()

	ctx := context.Background()

	s.logger.Debug("committer worker started")
	argsCap := s.cqlStatements.argsCap()
	held := make([][]any, committerBatchSize)
	for i := range held {
		held[i] = make([]any, 0, argsCap)
	}
	idx := 0

	for {
		// Block for at least one item
		first, ok := <-s.channel
		if !ok {
			return
		}

		batch := s.makeBatch(ctx)

		// Helper to add an item to the batch
		addItem := func(it stmtlogger.Item) {
			// decrement queue metric on consumption regardless of schema
			metrics.StatementLoggerItems.Dec()
			metrics.StatementLoggerDequeuedTotal.Inc()
			if it.StatementType.IsSchema() || it.StatementType.IsSelect() {
				return
			}
			held[idx] = s.cqlStatements.fillArgs(held[idx], it)
			batch.Query(s.cqlStatements.insertStmt, held[idx]...)
			idx++
		}

		addItem(first)

		// Best-effort drain up to maxBatch-1 additional items without blocking
		exiting := false
		draining := true
		for draining && idx < committerBatchSize {
			select {
			case it, chOpen := <-s.channel:
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
		s.executeBatchWithFallback(ctx, batch, held, idx)
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

	// Ignore schema and select statements
	if it.StatementType.IsSchema() || it.StatementType.IsSelect() {
		return
	}

	// If cqlStatements is not initialized (as in some unit tests), do nothing.
	if s == nil || s.cqlStatements == nil {
		return
	}

	// Build arguments and execute a single insert query.
	args := s.cqlStatements.fillArgs(make([]any, 0, s.cqlStatements.argsCap()), it)
	_ = s.execQuery(context.Background(), s.cqlStatements.insertStmt, args...)
}

func (s *Logger) openStatementFile(name string) (*bufio.Writer, func() error, error) {
	// Create parent directories if they don't exist
	dir := filepath.Dir(name)
	if err := os.MkdirAll(dir, statementDirPerm); err != nil {
		return nil, nil, errors.Wrapf(err, "failed to create directory for statements file '%q'", name)
	}

	file, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, statementFilePerm|fs.ModeExclusive)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to open test statements file '%q'", name)
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
	s.wg.Wait()
	return nil
}
