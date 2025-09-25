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
	"io"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/pkg/errors"
	"github.com/samber/mo"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
	"github.com/scylladb/gemini/pkg/workpool"
)

const (
	defaultChanSize = 8192
)

const (
	TypeOracle Type = "oracle"
	TypeTest   Type = "test"
)

type (
	Type string

	Item struct {
		Start           Time                     `json:"s"`
		PartitionKeys   *typedef.Values          `json:"partitionKeys"`
		Error           mo.Either[error, string] `json:"e,omitempty"`
		Statement       string                   `json:"q"`
		Host            string                   `json:"h"`
		Type            Type                     `json:"-"`
		GeneratedValues mo.Either[[]any, []byte] `json:"v"`
		DriverValues    []any                    `json:"d_v"`
		Duration        Duration                 `json:"d"`
		Attempt         int                      `json:"d_a"`
		GeminiAttempt   int                      `json:"g_a"`
		StatementType   typedef.StatementType    `json:"-"`
	}

	Duration struct {
		Duration time.Duration
	}

	Time struct {
		Time time.Time
	}

	Logger struct {
		closer  io.Closer
		channel atomic.Pointer[chan<- Item]
		metrics metrics.ChannelMetrics
	}

	options struct {
		logger      mo.Either[*ScyllaLogger, *IOWriterLogger]
		channelSize int
	}

	Option func(*options, chan Item, metrics.ChannelMetrics) error
)

func WithChannelSize(size int) Option {
	return func(o *options, _ chan Item, _ metrics.ChannelMetrics) error {
		if size <= 0 {
			return errors.New("channel size must be greater than 0")
		}
		o.channelSize = size
		return nil
	}
}

func WithScyllaLogger(
	schemaChangesValues typedef.PartitionKeys,
	schema *typedef.Schema,
	oracleStatementsFile string,
	testStatementsFile string,
	hosts []string,
	username, password string,
	compression Compression,
	e *joberror.ErrorList,
	pool *workpool.Pool,
	l *zap.Logger,
) Option {
	return func(o *options, ch chan Item, chMetrics metrics.ChannelMetrics) error {
		logger, err := NewScyllaLogger(
			schema.Keyspace.Name,
			schema.Tables[0].Name,
			ch,
			schemaChangesValues,
			schema,
			oracleStatementsFile,
			testStatementsFile,
			hosts,
			username,
			password,
			compression,
			e,
			pool,
			l,
			chMetrics,
		)
		if err != nil {
			return err
		}

		o.logger = mo.Left[*ScyllaLogger, *IOWriterLogger](logger)

		return nil
	}
}

func WithIOWriterLogger(name string, input io.Writer, compression Compression, l *zap.Logger) Option {
	return func(o *options, ch chan Item, _ metrics.ChannelMetrics) error {
		logger, err := NewIOWriterLogger(ch, name, input, compression, l)
		if err != nil {
			return err
		}

		o.logger = mo.Right[*ScyllaLogger, *IOWriterLogger](logger)

		return nil
	}
}

func WithFileLogger(filePath string, compression Compression, l *zap.Logger) Option {
	return func(o *options, ch chan Item, _ metrics.ChannelMetrics) error {
		logger, err := NewFileLogger(ch, filePath, compression, l)
		if err != nil {
			return err
		}

		o.logger = mo.Right[*ScyllaLogger, *IOWriterLogger](logger)

		return nil
	}
}

func NewLogger(opts ...Option) (*Logger, error) {
	o := options{
		channelSize: defaultChanSize,
	}

	chMetrics := metrics.NewChannelMetrics("statement_logger", "statement_logger")
	ch := make(chan Item, o.channelSize)

	for _, opt := range opts {
		if err := opt(&o, ch, chMetrics); err != nil {
			return nil, err
		}
	}

	l := &Logger{
		metrics: chMetrics,
	}

	l.init(ch, o.logger)

	return l, nil
}

func (l *Logger) init(ch chan<- Item, logger mo.Either[*ScyllaLogger, *IOWriterLogger]) {
	if logger.IsLeft() {
		l.closer = logger.MustLeft()
	} else {
		l.closer = logger.MustRight()
	}

	l.channel.Store(&ch)
}

func (l *Logger) LogStmt(item Item) error {
	if ch := l.channel.Load(); ch != nil {
		*ch <- item
		l.metrics.Inc()
	}

	return nil
}

func (l *Logger) Close() error {
	old := l.channel.Swap(nil)
	close(*old)
	return l.closer.Close()
}

func (i Item) MemoryFootprint() uint64 {
	size := uint64(unsafe.Sizeof(Item{})) + utils.Sizeof(i.Statement) +
		utils.Sizeof(i.Host)

	return size
}
