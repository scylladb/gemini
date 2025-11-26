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
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
)

type (
	Logger struct {
		logger        *zap.Logger
		channel       <-chan stmtlogger.Item
		cqlStatements *cqlStatements
		wg            sync.WaitGroup
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

	wd, err := os.Getwd()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get current working directory")
	}

	if oracleStatementsFile == "" {
		oracleStatementsFile = filepath.Clean(filepath.Join(wd, "oracle_statements.jsonl"))
	}

	if testStatementsFile == "" {
		testStatementsFile = filepath.Clean(filepath.Join(wd, "test_statements.jsonl"))
	}

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

	l.Debug("starting committer goroutine")
	go logger.insertStatements()
	l.Debug("starting error logger goroutine")
	statementCh := make(chan statementChData, 1000)
	go logger.fetchErrors(statementCh, e)
	go logger.statementFlusher(statementCh, oracleStatementsFile, testStatementsFile)

	return logger, nil
}

func (s *Logger) statementFlusher(ch <-chan statementChData, oracleStatements, testStatements string) {
	s.wg.Add(1)
	oracleStatementsFile, oracleCloser, err := s.openStatementFile(oracleStatements)
	if err != nil {
		s.logger.Error("failed to open oracle statements file",
			zap.Error(err),
			zap.String("file", oracleStatements),
			zap.String("type", string(stmtlogger.TypeOracle)),
		)
		panic(err)
	}

	testStatementsFile, testCloser, err := s.openStatementFile(testStatements)
	if err != nil {
		s.logger.Error("failed to open test statements file",
			zap.Error(err),
			zap.String("file", testStatements),
			zap.String("type", string(stmtlogger.TypeTest)),
		)
		panic(err)
	}

	defer func() {
		if err = oracleCloser(); err != nil {
			s.logger.Error("failed to close statements file",
				zap.Error(err),
				zap.String("file", oracleStatements),
				zap.String("type", string(stmtlogger.TypeOracle)),
			)
		}

		if err = testCloser(); err != nil {
			s.logger.Error("failed to close statements file",
				zap.Error(err),
				zap.String("file", testStatements),
				zap.String("type", string(stmtlogger.TypeTest)),
			)
		}

		s.wg.Done()
	}()
	for item := range ch {
		for _, val := range item.Data {
			var bytes []byte
			errStr := ""
			if item.Error.Err != nil {
				errStr = item.Error.Err.Error()
			}
			bytes, err = json.Marshal(Line{
				PartitionKeys:     val.partitionKeys,
				Timestamp:         item.Error.Timestamp,
				Err:               errStr,
				Query:             item.Error.Query,
				Message:           item.Error.Message,
				MutationFragments: val.mutationFragments,
				Statements:        val.statements,
			})
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

			// Flush after each write to ensure data is written to disk
			if err = writer.Flush(); err != nil {
				s.logger.Error("failed to flush statement log",
					zap.Error(err),
					zap.String("type", string(item.ty)),
					zap.String("file", fileName),
				)
			}
		}
	}
}

func (s *Logger) poolCallback(ctx context.Context, ty stmtlogger.Type, jobError *joberror.JobError) statementChData {
	data, err := s.cqlStatements.Fetch(ctx, ty, jobError)
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
	s.wg.Add(1)
	// Track seen errors by hex hash to avoid JSON-unsafe key types
	seen := make(map[[32]byte]bool)
	wg := &sync.WaitGroup{}

	defer func() {
		wg.Wait()
		close(items)
		s.wg.Done()
	}()

	storages := []stmtlogger.Type{stmtlogger.TypeTest, stmtlogger.TypeOracle}

	for jobErr := range ch {
		hash := jobErr.Hash()
		if val, ok := seen[hash]; ok && val {
			continue
		}

		seen[hash] = true
		for _, ty := range storages {
			wg.Add(1)
			go func(ty stmtlogger.Type) {
				defer wg.Done()
				items <- s.poolCallback(context.Background(), ty, jobErr)
			}(ty)
		}
	}
}

func (s *Logger) insertStatements() {
	s.wg.Add(1)
	defer func() {
		s.logger.Debug("committer goroutine exiting")
		s.wg.Done()
	}()

	s.logger.Debug("committer goroutine started")

	for item := range s.channel {
		s.insert(item)
	}
}

func (s *Logger) insert(item stmtlogger.Item) {
	if item.StatementType.IsSchema() {
		return
	}

	go func() {
		if err := s.cqlStatements.Insert(context.Background(), item); err != nil && !errors.Is(err, context.Canceled) {
			s.logger.Error("failed to insert into statements table",
				zap.Error(err),
				zap.Any("item", item),
			)
		}
	}()
}

func (s *Logger) openStatementFile(name string) (*bufio.Writer, func() error, error) {
	// Create parent directories if they don't exist
	dir := filepath.Dir(name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, nil, errors.Wrapf(err, "failed to create directory for statements file '%q'", name)
	}

	file, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644|fs.ModeExclusive)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to open test statements file '%q'", name)
	}

	buffered := bufio.NewWriterSize(file, 32*1024)

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
