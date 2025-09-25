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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/samber/mo"
	"github.com/scylladb/gocqlx/v3/qb"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
	"github.com/scylladb/gemini/pkg/workpool"
)

const (
	additionalColumns       = "ddl,ts,ty,statement,values,driver_values,host,attempt,gemini_attempt,error,dur"
	selectAdditionalColumns = "ddl,ts,statement,values,driver_values,host,attempt,gemini_attempt,error,dur"
)

var (
	additionalColumnsArr       = strings.Split(additionalColumns, ",")
	selectAdditionalColumnsArr = strings.Split(selectAdditionalColumns, ",")
)

func GetScyllaStatementLogsKeyspace(originalKeyspace string) string {
	return fmt.Sprintf("%s_logs", originalKeyspace)
}

func GetScyllaStatementLogsTable(originalTable string) string {
	return fmt.Sprintf("%s_statements", originalTable)
}

type ScyllaLogger struct {
	metrics              metrics.ChannelMetrics
	logger               *zap.Logger
	channel              <-chan Item
	errors               *joberror.ErrorList
	wg                   *sync.WaitGroup
	pool                 *workpool.Pool
	cancel               context.CancelFunc
	session              *gocql.Session
	oracleStatementsFile string
	testStatementsFile   string
	schemaChangeValues   typedef.PartitionKeys
	keyspaceAndTable     string
	schemaPartitionKeys  typedef.Columns
	compression          Compression
	partitionKeysCount   int
}

func newSession(hosts []string, username, password string, logger *zap.Logger) (*gocql.Session, error) {
	cluster := gocql.NewCluster(slices.Clone(hosts)...)
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
	cluster.PageSize = 10_000
	cluster.Compressor = &gocql.SnappyCompressor{}

	if username != "" && password != "" {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	return cluster.CreateSession()
}

func NewScyllaLoggerWithSession(
	originalKeyspace string,
	originalTable string,
	schemaChangeValues typedef.PartitionKeys,
	session *gocql.Session,
	partitionKeys typedef.Columns,
	repl replication.Replication,
	ch chan Item,
	oracleStatementsFile string,
	testStatementsFile string,
	compression Compression,
	e *joberror.ErrorList,
	pool *workpool.Pool,
	l *zap.Logger,
	chMetrics metrics.ChannelMetrics,
) (*ScyllaLogger, error) {
	keyspace := GetScyllaStatementLogsKeyspace(originalKeyspace)
	table := GetScyllaStatementLogsTable(originalTable)

	createKeyspace, createTable := buildCreateTableQuery(keyspace, table, partitionKeys, repl)

	wd, err := os.Getwd()
	if err != nil {
		return nil, errors.Wrap(err, "failed to get current working directory")
	}

	if oracleStatementsFile == "" {
		oracleStatementsFile = filepath.Join(wd, "oracle_statements.jsonl")
	}

	if testStatementsFile == "" {
		testStatementsFile = filepath.Join(wd, "test_statements.jsonl")
	}

	if err = session.Query(fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", keyspace)).Exec(); err != nil {
		return nil, err
	}

	if err = session.Query(createKeyspace).Exec(); err != nil {
		return nil, err
	}

	if err = session.Query(createTable).Exec(); err != nil {
		return nil, err
	}

	if err = session.AwaitSchemaAgreement(context.Background()); err != nil {
		return nil, errors.Wrap(err, "failed to await schema agreement for keyspace logs")
	}

	wg := &sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())

	logger := &ScyllaLogger{
		cancel:               cancel,
		schemaChangeValues:   schemaChangeValues,
		session:              session,
		compression:          compression,
		channel:              ch,
		errors:               e,
		metrics:              chMetrics,
		wg:                   wg,
		pool:                 pool,
		logger:               l,
		oracleStatementsFile: filepath.Clean(oracleStatementsFile),
		testStatementsFile:   filepath.Clean(testStatementsFile),
		partitionKeysCount:   partitionKeys.LenValues(),
		schemaPartitionKeys:  partitionKeys,
		keyspaceAndTable:     fmt.Sprintf("%s.%s", keyspace, table),
	}

	logger.wg.Add(2)
	go logger.commiter(ctx, partitionKeys.LenValues())
	go logger.logErrors(ctx)

	return logger, nil
}

func NewScyllaLogger(
	originalKeyspace string,
	originalTable string,
	ch chan Item,
	schemaChangeValues typedef.PartitionKeys,
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
		originalKeyspace,
		originalTable,
		schemaChangeValues,
		session,
		schema.Tables[0].PartitionKeys,
		schema.Keyspace.OracleReplication,
		ch, oracleStatementsFile, testStatementsFile, compression,
		e, pool, l, chMetrics,
	)
}

func (s *ScyllaLogger) commiter(ctx context.Context, partitionKeysCount int) {
	defer s.wg.Done()

	schemaChangeValues := s.schemaChangeValues.Values.ToCQLValues(s.schemaPartitionKeys)
	schemaChangeValues = append(schemaChangeValues, true)

	insertBuilder := qb.Insert(s.keyspaceAndTable)

	for _, col := range s.schemaPartitionKeys {
		switch colType := col.Type.(type) {
		case *typedef.TupleType:
			insertBuilder.TupleColumn(col.Name, len(colType.ValueTypes))
		default:
			insertBuilder.Columns(col.Name)
		}
	}

	insertBuilder.Columns(additionalColumnsArr...)
	query, _ := insertBuilder.ToCql()

	logStatement := func(item Item) {
		s.metrics.Dec()
		values := make([]any, 0, partitionKeysCount+len(additionalColumnsArr))

		if item.StatementType.IsSchema() {
			values = append(values, schemaChangeValues...)
		} else {
			values = append(values, item.PartitionKeys.ToCQLValues(s.schemaPartitionKeys)...)
			values = append(values, false)
		}

		var itemErr string
		if item.Error.IsLeft() {
			if errVal := item.Error.MustLeft(); errVal != nil {
				itemErr = errVal.Error()
			}
		} else {
			itemErr = item.Error.MustRight()
		}

		values = append(values,
			item.Start.Time,
			string(item.Type),
			item.Statement,
			prepareValues(item.GeneratedValues),
			prepareValues(mo.Left[[]any, []byte](item.DriverValues)),
			item.Host,
			item.Attempt,
			item.GeminiAttempt,
			itemErr,
			item.Duration.Duration,
		)

		if len(values) != partitionKeysCount+len(additionalColumnsArr) {
			s.logger.Error(
				"invalid number of values for Scylla insert",
				zap.Int("expected", partitionKeysCount+len(additionalColumnsArr)),
				zap.Int("actual", len(values)),
				zap.Any("values", values),
				zap.Stringer("statement", item.StatementType),
			)
			return
		}

		s.pool.SendWithoutResult(ctx, func(_ context.Context) {
			q := s.session.Bind(query, func(_ *gocql.QueryInfo) ([]any, error) {
				return values, nil
			})
			defer q.Release()

			if err := q.Exec(); err != nil {
				s.logger.Error("failed to insert into statements table", zap.Error(err))
			}
		})
	}

	for {
		select {
		case <-ctx.Done():
			for item := range s.channel {
				logStatement(item)
			}

			return
		case item, more := <-s.channel:
			if !more {
				break
			}

			logStatement(item)
		}
	}
}

func (s *ScyllaLogger) logErrors(ctx context.Context) {
	defer s.wg.Done()
	if s.oracleStatementsFile == "" || s.testStatementsFile == "" {
		s.logger.Panic("statements files are not set",
			zap.String("oracle_statements_file", s.oracleStatementsFile),
			zap.String("test_statements_file", s.testStatementsFile),
		)
	}
	wg := &sync.WaitGroup{}

	storages := []struct {
		ch   chan []byte
		file string
		ty   Type
	}{{
		ch:   make(chan []byte, s.errors.Cap()+10),
		file: s.oracleStatementsFile,
		ty:   TypeOracle,
	}, {
		ch:   make(chan []byte, s.errors.Cap()+10),
		file: s.testStatementsFile,
		ty:   TypeTest,
	}}

	for _, storage := range storages {
		wg.Add(1)
		go func() {
			file, closer, err := s.openStatementFile(storage.file)
			if err != nil {
				s.logger.Panic("failed to open oracle statements file", zap.Error(err))
				return
			}

			defer func() {
				if err = closer(); err != nil {
					s.logger.Error("failed to close statements file",
						zap.String("type", string(storage.ty)),
						zap.String("file", storage.file),
						zap.Error(err),
					)
				}

				wg.Done()
			}()

			for {
				select {
				case <-ctx.Done():
					return
				case item, more := <-storage.ch:
					if !more {
						return
					}

					if _, err = file.Write(item); err != nil {
						s.logger.Error("failed to write statement",
							zap.String("type", string(storage.ty)),
							zap.String("statement", string(item)),
							zap.Error(err),
						)

						continue
					}

					// Since this is a new line, we can ignore the error
					// if we fail to write a new line, it will be caught by the next write
					// and will have two rows on the same line
					_, _ = file.WriteRune('\n')
				}
			}
		}()
	}

	pushErrors := func(jobErr *joberror.JobError, fetchSchema bool) {
		pushErr := &sync.WaitGroup{}
		for _, storage := range storages {
			pushErr.Add(1)
			go func() {
				pushErr.Done()
				if fetchSchema {
					s.fetchSchemaChanges(storage.ty, storage.ch)
				}

				if err := s.fetchFailedPartitions(storage.ch, storage.ty, jobErr); err != nil {
					s.logger.Error(
						"failed to fetch failed partitions",
						zap.String("type", string(storage.ty)),
						zap.Error(err),
					)
				}
			}()
		}

		pushErr.Wait()
	}

	errCh := s.errors.GetChannel()

	for iteration := 0; ; {
		select {
		case <-ctx.Done():
			_ = s.errors.Close()

			for jobErr := range errCh {
				pushErrors(jobErr, iteration == 0)
				iteration++
			}

			wg.Wait()
			return
		case jobErr := <-errCh:
			pushErrors(jobErr, iteration == 0)
			iteration++
		}
	}
}

func prepareValues(values mo.Either[[]any, []byte]) string {
	if values.IsLeft() {
		data, _ := json.Marshal(values.MustLeft())
		return utils.UnsafeString(data)
	}

	return utils.UnsafeString(values.MustRight())
}

func buildCreateTableQuery(
	keyspace string,
	table string,
	partitionKeys typedef.Columns,
	replication replication.Replication,
) (string, string) {
	createKeyspace := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH replication=%s AND durable_writes = true;",
		keyspace, replication.ToCQL(),
	)

	var builder bytes.Buffer

	partitions := strings.Join(partitionKeys.Names(), ",")

	builder.WriteString("CREATE TABLE IF NOT EXISTS ")
	builder.WriteString(keyspace)
	builder.WriteRune('.')
	builder.WriteString(table)
	builder.WriteString("(")

	for _, col := range partitionKeys {
		builder.WriteString(col.Name)
		builder.WriteString(" ")
		builder.WriteString(col.Type.CQLDef())
		builder.WriteRune(',')
	}

	builder.WriteString("ddl boolean, ts timestamp, ty text, statement text, values text, driver_values text, host text, attempt smallint, gemini_attempt smallint, error text, dur duration, ")
	builder.WriteString("PRIMARY KEY ((")
	builder.WriteString(partitions)
	builder.WriteString(", ty), ddl, ts, attempt, gemini_attempt)) WITH caching={'enabled':'true'} AND compression={'sstable_compression':'ZstdCompressor'}")
	builder.WriteString(" AND tombstone_gc={'mode':'immediate'} AND comment='Table to store logs from Oracle and Test statements';")

	createTable := builder.String()

	builder.Reset()

	return createKeyspace, createTable
}

func (s *ScyllaLogger) fetchData(ch chan<- []byte, statement string, values []any) error {
	query := s.session.Query(statement, values...)
	defer query.Release()

	iter := query.Iter()

	for range iter.NumRows() {
		var b []byte

		if !iter.Scan(&b) {
			break
		}

		ch <- b
	}

	return iter.Close()
}

func (s *ScyllaLogger) buildQuery(jobError *joberror.JobError, ty Type) ([]string, [][]any) {
	switch jobError.StmtType {
	case typedef.SelectStatementType, typedef.SelectRangeStatementType,
		typedef.InsertStatementType, typedef.InsertJSONStatementType,
		typedef.UpdateStatementType, typedef.DeleteWholePartitionType,
		typedef.DeleteSingleRowType, typedef.DeleteSingleColumnType:
		builder := qb.Select(s.keyspaceAndTable).
			Json().
			Columns(selectAdditionalColumnsArr...).
			OrderBy("ts", qb.ASC)

		values := make([]any, 0, len(s.schemaPartitionKeys))
		for _, pk := range s.schemaPartitionKeys {
			builder.Where(qb.Eq(pk.Name))
			values = append(values, jobError.PartitionKeys.Get(pk.Name)...)
		}

		builder.Where(
			qb.EqLit("ty", "'"+string(ty)+"'"),
			qb.EqLit("ddl", "false"),
		)

		query, _ := builder.ToCql()
		return []string{query}, [][]any{values}
	case typedef.SelectMultiPartitionType, typedef.SelectMultiPartitionRangeStatementType,
		typedef.DeleteMultiplePartitionsType:
		// TODO: Optimization for multi-partition queries
		// 	Calculate the Cartesian product of all partition keys
		//  By this we can split the query into multiple queries
		//  Without running into `cartesian product of IN list N`
		//  This naive implementation will run one query per partition key value

		iterations := len(jobError.PartitionKeys.Get(s.schemaPartitionKeys[0].Name))
		queries := make([]string, 0, iterations)
		values := make([][]any, 0, iterations)

		for i := range iterations {
			builder := qb.Select(s.keyspaceAndTable).
				Json().
				Columns(selectAdditionalColumnsArr...).
				OrderBy("ts", qb.ASC)

			vals := make([]any, 0, len(s.schemaPartitionKeys))
			for _, pk := range s.schemaPartitionKeys {
				builder.Where(qb.Eq(pk.Name))
				vals = append(vals, jobError.PartitionKeys.Get(pk.Name)[i])
			}

			builder.Where(
				qb.EqLit("ty", "'"+string(ty)+"'"),
				qb.EqLit("ddl", "false"),
			)

			query, _ := builder.ToCql()
			queries = append(queries, query)
			values = append(values, vals)
		}

		return queries, values

	case typedef.SelectByIndexStatementType, typedef.SelectFromMaterializedViewStatementType:
		s.logger.Warn(
			"select by index or materialized view is not supported, skipping job error",
			zap.Any("partition_keys", jobError.PartitionKeys),
		)
		return nil, nil
	default:
		return nil, nil
	}
}

func (s *ScyllaLogger) fetchSchemaChanges(ty Type, ch chan<- []byte) {
	builder := qb.Select(s.keyspaceAndTable).
		Json().
		Columns(selectAdditionalColumnsArr...).
		OrderBy("ts", qb.ASC)

	for _, pk := range s.schemaPartitionKeys {
		builder.Where(qb.Eq(pk.Name))
	}

	ddlStatementsQuery, _ := builder.Where(qb.EqLit("ty", "'"+string(ty)+"'")).
		Where(qb.EqLit("ddl", "true")).
		ToCql()

	schemaChangeValues := make([]any, 0, s.partitionKeysCount)
	schemaChangeValues = append(schemaChangeValues, s.schemaChangeValues.Values.ToCQLValues(s.schemaPartitionKeys)...)

	if fetchErr := s.fetchData(ch, ddlStatementsQuery, schemaChangeValues); fetchErr != nil {
		s.logger.Error("failed to fetch schema change data", zap.Error(fetchErr))
	}
}

func (s *ScyllaLogger) fetchFailedPartitions(ch chan<- []byte, ty Type, jobError *joberror.JobError) error {
	queries, values := s.buildQuery(jobError, ty)
	if len(queries) == 0 || len(values) == 0 {
		return errors.New("unsupported type to fetch from statement logs")
	}

	wg := &sync.WaitGroup{}

	for i, query := range queries {
		wg.Add(1)
		go func(values []any) {
			defer wg.Done()
			if fetchErr := s.fetchData(ch, query, values); fetchErr != nil {
				s.logger.Error("failed to fetch failed partition data",
					zap.Error(fetchErr),
					zap.Any("partition_keys", values),
				)
			}
		}(values[i])
	}

	wg.Wait()

	return nil
}

func (s *ScyllaLogger) openStatementFile(name string) (*bufio.Writer, func() error, error) {
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

func (s *ScyllaLogger) Close() error {
	s.cancel()
	s.wg.Wait()
	return nil
}
