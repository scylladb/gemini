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

package stmtlogger

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/samber/mo"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
	"github.com/scylladb/gemini/pkg/workpool"
)

const (
	additionalColumns       = "ddl,ts,ty,statement,values,host,attempt,gemini_attempt,error,dur"
	additionalSelectColumns = "ts,ty,statement,values,host,attempt,gemini_attempt,error,dur"
)

var (
	additionalColumnsCount        = len(strings.Split(additionalColumns, ","))
	additionalColumnsPlaceholders = strings.Trim(strings.Repeat("?,", additionalColumnsCount), ",")
)

type ScyllaLogger struct {
	session              *gocql.Session
	channel              <-chan Item
	errors               *joberror.ErrorList
	wg                   *sync.WaitGroup
	pool                 *workpool.Pool
	logger               *zap.Logger
	metrics              metrics.ChannelMetrics
	selectStatement      string
	oracleStatementsFile string
	testStatementsFile   string
	selectDDLStatements  string
	schemaChangeValues   typedef.Values
	compression          Compression
	partitionKeysCount   int
}

func newSession(hosts []string, username, password string, logger *zap.Logger) (*gocql.Session, error) {
	for i := range hosts {
		hosts[i] = strings.TrimSpace(hosts[i])
	}

	cluster := gocql.NewCluster(hosts...)
	cluster.Consistency = gocql.Quorum
	cluster.DefaultTimestamp = false
	cluster.Logger = zap.NewStdLog(logger.Named("statements-scylla"))
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.Timeout = 10 * time.Second
	cluster.ConnectTimeout = 60 * time.Second
	cluster.RetryPolicy = &gocql.ExponentialBackoffRetryPolicy{
		Min:        time.Second,
		Max:        60 * time.Second,
		NumRetries: 5,
	}
	cluster.MaxPreparedStmts = 5
	cluster.PageSize = 10_000
	cluster.Compressor = gocql.SnappyCompressor{}

	if username != "" && password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	return cluster.CreateSession()
}

func NewScyllaLoggerWithSession(
	schemaChangeValues typedef.Values,
	session *gocql.Session,
	partitionKeys typedef.Columns,
	repl replication.Replication,
	ch <-chan Item,
	oracleStatementsFile string,
	testStatementsFile string,
	compression Compression,
	e *joberror.ErrorList,
	pool *workpool.Pool,
	l *zap.Logger,
	chMetrics metrics.ChannelMetrics,
) (*ScyllaLogger, error) {
	createKeyspace, createTable, insertStatement, selectStatement, ddlStatements := buildCreateTableQuery(partitionKeys, repl)

	if err := session.Query("DROP KEYSPACE IF EXISTS logs;").Exec(); err != nil {
		return nil, err
	}

	if err := session.Query(createKeyspace).Exec(); err != nil {
		return nil, err
	}

	if err := session.Query(createTable).Exec(); err != nil {
		return nil, err
	}

	if err := session.AwaitSchemaAgreement(context.Background()); err != nil {
		return nil, errors.Wrap(err, "failed to await schema agreement for keyspace logs")
	}

	wg := &sync.WaitGroup{}

	logger := &ScyllaLogger{
		schemaChangeValues:   schemaChangeValues,
		session:              session,
		compression:          compression,
		channel:              ch,
		errors:               e,
		metrics:              chMetrics,
		wg:                   wg,
		selectStatement:      selectStatement,
		selectDDLStatements:  ddlStatements,
		pool:                 pool,
		logger:               l,
		oracleStatementsFile: oracleStatementsFile,
		testStatementsFile:   testStatementsFile,
		partitionKeysCount:   partitionKeys.LenValues(),
	}

	logger.wg.Add(1)
	go logger.commiter(insertStatement, partitionKeys.LenValues())

	return logger, nil
}

func NewScyllaLogger(
	ch <-chan Item,
	schemaChangeValues typedef.Values,
	schema *typedef.Schema,
	oracleStatementsFile string,
	testStatementsFile string,
	hosts []string,
	username, password string,
	compression Compression,
	e *joberror.ErrorList,
	pool *workpool.Pool,
	l *zap.Logger,
	chMetrics metrics.ChannelMetrics,
) (*ScyllaLogger, error) {
	session, err := newSession(hosts, username, password, l)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create Scylla session with hosts %v", hosts)
	}

	return NewScyllaLoggerWithSession(
		schemaChangeValues,
		session,
		schema.Tables[0].PartitionKeys,
		schema.Keyspace.OracleReplication,
		ch, oracleStatementsFile, testStatementsFile, compression,
		e, pool, l, chMetrics,
	)
}

func (s *ScyllaLogger) commiter(insert string, partitionKeysCount int) {
	defer s.wg.Done()

	ctx := context.Background()

	schemaChangeValues := make([]any, 0, partitionKeysCount+1)
	schemaChangeValues = append(schemaChangeValues, s.schemaChangeValues[:partitionKeysCount]...)
	schemaChangeValues = append(schemaChangeValues, true)

	for item := range s.channel {
		s.metrics.Dec(item)
		s.pool.SendWithoutResult(ctx, func(_ context.Context) {
			values := make([]any, 0, partitionKeysCount+additionalColumnsCount)

			if item.StatementType.IsSchemaChange() {
				values = append(values, schemaChangeValues...)
			} else {
				itemValues := item.Values.MustLeft()

				if len(itemValues) >= partitionKeysCount {
					values = append(values, itemValues[:partitionKeysCount]...)
					values = append(values, false)
				}
			}

			var err string
			if item.Error.IsLeft() {
				if errVal := item.Error.MustLeft(); errVal != nil {
					err = errVal.Error()
				}
			} else {
				err = item.Error.MustRight()
			}

			values = append(values,
				item.Start.Time,
				item.Type,
				item.Statement,
				prepareValues(item.Values),
				item.Host,
				item.Attempt,
				item.GeminiAttempt,
				err,
				item.Duration.Duration,
			)
			query := s.session.Query(insert, values...)
			defer query.Release()

			if err := query.Exec(); err != nil {
				s.logger.Error("failed to insert into statements table", zap.Error(err))
			}
		})
	}
}

func prepareValues(values mo.Either[typedef.Values, string]) string {
	if values.IsLeft() {
		data, _ := json.Marshal([]any(values.MustLeft()))
		return utils.UnsafeString(data)
	}

	return values.MustRight()
}

func buildCreateTableQuery(
	partitionKeys typedef.Columns,
	replication replication.Replication,
) (string, string, string, string, string) {
	createKeyspace := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS logs WITH replication=%s AND durable_writes = true;",
		replication.ToCQL(),
	)

	var builder bytes.Buffer

	partitions := strings.Join(partitionKeys.Names(), ",")

	builder.WriteString("CREATE TABLE IF NOT EXISTS logs.statements(")

	for _, col := range partitionKeys {
		builder.WriteString(col.Name)
		builder.WriteString(" ")
		builder.WriteString(col.Type.CQLDef())
		builder.WriteRune(',')
	}

	builder.WriteString("ddl boolean, ts timestamp, ty text, statement text, values text, host text, attempt smallint, gemini_attempt smallint, error text, dur duration, ")
	builder.WriteString("PRIMARY KEY ((")
	builder.WriteString(partitions)
	builder.WriteString(", ty), ddl, ts)) WITH caching={'enabled':'true'} AND compression={'sstable_compression':'ZstdCompressor'}")
	builder.WriteString(" AND tombstone_gc={'mode':'immediate'} AND comment='Table to store logs from Oracle and Test statements';")

	createTable := builder.String()

	builder.Reset()

	builder.WriteString("INSERT INTO logs.statements (")
	builder.WriteString(partitions)
	builder.WriteRune(',')
	builder.WriteString(additionalColumns)
	builder.WriteString(") VALUES(")

	for _, col := range partitionKeys {
		builder.WriteString(col.Type.CQLHolder())
		builder.WriteRune(',')
	}

	builder.WriteString(additionalColumnsPlaceholders)
	builder.WriteString(");")

	insertStatement := builder.String()

	builder.Reset()

	builder.WriteString("SELECT ")
	builder.WriteString(additionalSelectColumns)
	builder.WriteString(" FROM logs.statements WHERE ")

	for _, col := range partitionKeys {
		builder.WriteString(col.Name)
		builder.WriteRune('=')
		builder.WriteString(col.Type.CQLHolder())
		builder.WriteString(" AND ")
	}

	builder.WriteString("ty=? AND ddl=false ORDER BY ts;")
	selectStatement := builder.String()

	builder.Reset()

	builder.WriteString("SELECT ")
	builder.WriteString(additionalSelectColumns)
	builder.WriteString(" FROM logs.statements WHERE ")

	for _, col := range partitionKeys {
		builder.WriteString(col.Name)
		builder.WriteRune('=')
		builder.WriteString(col.Type.CQLHolder())
		builder.WriteString(" AND ")
	}

	builder.WriteString("ty=? AND ddl=true ORDER BY ts;")

	selectDDLs := builder.String()

	return createKeyspace, createTable, insertStatement, selectStatement, selectDDLs
}

func (s *ScyllaLogger) fetchData(ch chan<- Item, ty Type, statement string, values []any) error {
	query := s.session.Query(statement, values...)
	defer query.Release()

	iter := query.Iter()

	for range iter.NumRows() {
		m := make(map[string]any, additionalColumnsCount)

		if !iter.MapScan(m) {
			break
		}

		item := Item{
			Start:         Time{Time: m["ts"].(time.Time)},
			Error:         mo.Right[error, string](m["error"].(string)),
			Statement:     m["statement"].(string),
			Host:          m["host"].(string),
			Type:          ty,
			Values:        mo.Right[typedef.Values, string](m["values"].(string)),
			Duration:      Duration{Duration: time.Duration(m["dur"].(gocql.Duration).Nanoseconds)},
			Attempt:       int(m["attempt"].(int16)),
			GeminiAttempt: int(m["gemini_attempt"].(int16)),
		}

		ch <- item
	}

	return iter.Close()
}

func (s *ScyllaLogger) fetchFailedPartitions(ch chan<- Item, ty Type, errs []joberror.JobError) {
	defer close(ch)
	schemaChangeValues := make([]any, 0, s.partitionKeysCount+1)
	schemaChangeValues = append(schemaChangeValues, s.schemaChangeValues[:s.partitionKeysCount]...)
	schemaChangeValues = append(schemaChangeValues, string(ty))

	var wg sync.WaitGroup

	wg.Add(1)
	s.pool.SendWithoutResult(context.Background(), func(_ context.Context) {
		defer wg.Done()
		if err := s.fetchData(ch, ty, s.selectDDLStatements, schemaChangeValues); err != nil {
			s.logger.Error("failed to fetch schema change data", zap.Error(err))
		}
	})

	for _, jobError := range errs {
		if len(jobError.PartitionKeys) != s.partitionKeysCount {
			s.logger.Warn(
				"skipping job error with incorrect partition keys length, probably query with indexes",
				zap.Any("partition_keys", jobError.PartitionKeys),
			)
			continue
		}

		values := make([]any, 0, s.partitionKeysCount+1)
		values = append(values, jobError.PartitionKeys[:s.partitionKeysCount]...)
		values = append(values, string(ty))

		wg.Add(1)
		s.pool.SendWithoutResult(context.Background(), func(_ context.Context) {
			defer wg.Done()
			if err := s.fetchData(ch, ty, s.selectStatement, values); err != nil {
				s.logger.Error("failed to fetch failed partition data",
					zap.Error(err),
					zap.Any("partition_keys", values),
				)
			}
		})
	}

	wg.Wait()
}

func (s *ScyllaLogger) openStatementFile(name string) (io.Writer, func() error, error) {
	testFileFd, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, nil, errors.Wrapf(err, "failed to open test statements file %q", name)
	}

	testFile, closer, err := s.compression.newWriter(testFileFd)
	if err != nil {
		return nil, testFileFd.Close, errors.Wrapf(err, "failed to create test statements file writer %q", name)
	}

	return testFile, closer.Close, nil
}

func (s *ScyllaLogger) writeBrokenPartitionsToFile(errs []joberror.JobError) error {
	testFile, testCloser, err := s.openStatementFile(s.testStatementsFile)
	if err != nil {
		if testCloser != nil {
			if err = testCloser(); err != nil {
				s.logger.Error("failed to close test statements file", zap.Error(err))
			}
		}
		return errors.Wrapf(err, "failed to open test statements file %q", s.testStatementsFile)
	}

	defer func() {
		if err = testCloser(); err != nil {
			s.logger.Error("failed to close test statements file", zap.Error(err))
		}
	}()

	oracleFile, oracleCloser, err := s.openStatementFile(s.oracleStatementsFile)
	if err != nil {
		if oracleCloser != nil {
			if err = oracleCloser(); err != nil {
				s.logger.Error("failed to close oracle statements file", zap.Error(err))
			}
		}
		return errors.Wrapf(err, "failed to open oracle statements file %q", s.oracleStatementsFile)
	}

	defer func() {
		if err = oracleCloser(); err != nil {
			s.logger.Error("failed to close oracle statements file", zap.Error(err))
		}
	}()

	oracleCh := make(chan Item, 1000)
	testCh := make(chan Item, 1000)
	s.pool.SendWithoutResult(context.Background(), func(_ context.Context) {
		s.fetchFailedPartitions(oracleCh, TypeOracle, errs)
	})

	s.pool.SendWithoutResult(context.Background(), func(_ context.Context) {
		s.fetchFailedPartitions(testCh, TypeTest, errs)
	})

	testJSONEncoder := json.NewEncoder(testFile)
	testJSONEncoder.SetEscapeHTML(false)
	oracleJSONEncoder := json.NewEncoder(oracleFile)
	oracleJSONEncoder.SetEscapeHTML(false)

	for {
		select {
		case item, ok := <-oracleCh:
			if !ok {
				oracleCh = nil
				if testCh == nil {
					return nil
				}
				continue
			}

			if err = oracleJSONEncoder.Encode(item); err != nil {
				s.logger.Error("failed to encode oracle statement", zap.Error(err), zap.Any("item", item))
			}
		case item, ok := <-testCh:
			if !ok {
				testCh = nil
				if oracleCh == nil {
					return nil
				}
				continue
			}

			if err = testJSONEncoder.Encode(item); err != nil {
				s.logger.Error("failed to encode test statement", zap.Error(err), zap.Any("item", item))
			}
		}
	}
}

func (s *ScyllaLogger) Close() error {
	s.wg.Wait()

	errs := s.errors.Errors()
	if len(errs) == 0 {
		return nil
	}

	if err := s.writeBrokenPartitionsToFile(errs); err != nil {
		return errors.Wrap(err, "failed to write broken partitions to file")
	}

	return nil
}
