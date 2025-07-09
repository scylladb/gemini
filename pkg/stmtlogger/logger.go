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
	"encoding/json"
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
		Start         Time                     `json:"s"`
		PartitionKeys *typedef.Values          `json:"partitionKeys"`
		Error         mo.Either[error, string] `json:"e,omitempty"`
		Statement     string                   `json:"q"`
		Host          string                   `json:"h"`
		Type          Type                     `json:"-"`
		Values        mo.Either[[]any, []byte] `json:"v"`
		Duration      Duration                 `json:"d"`
		Attempt       int                      `json:"d_a"`
		GeminiAttempt int                      `json:"g_a"`
		StatementType typedef.StatementType    `json:"-"`
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

type itemMarshal struct {
	Values        any             `json:"values"`
	PartitionKeys *typedef.Values `json:"partition_keys"`
	Start         Time            `json:"time"`
	Error         *string         `json:"error,omitempty"`
	Statement     string          `json:"query"`
	Host          string          `json:"host"`
	Duration      Duration        `json:"duration"`
	Attempt       int             `json:"driver_attempt"`
	GeminiAttempt int             `json:"gemini_attempt"`
}

func (i Item) MarshalJSON() ([]byte, error) {
	var errorString *string

	if i.Error.IsLeft() {
		if val := i.Error.MustLeft(); val != nil {
			str := val.Error()
			errorString = &str
		}
	} else {
		str := i.Error.MustRight()
		errorString = &str
	}

	var values any

	if i.Values.IsLeft() {
		values = i.Values.MustLeft()
	} else {
		values = utils.UnsafeString(i.Values.MustRight())
	}

	return json.Marshal(itemMarshal{
		Start:         i.Start,
		Values:        values,
		Error:         errorString,
		Statement:     i.Statement,
		Host:          i.Host,
		Duration:      i.Duration,
		Attempt:       i.Attempt,
		GeminiAttempt: i.GeminiAttempt,
		PartitionKeys: i.PartitionKeys,
	})
}

func (i *Item) UnmarshalJSON(data []byte) error {
	var im itemMarshal
	if err := json.Unmarshal(data, &im); err != nil {
		return errors.Wrap(err, "failed to unmarshal item")
	}

	i.Start = im.Start
	i.PartitionKeys = im.PartitionKeys

	if im.Error != nil {
		i.Error = mo.Right[error, string](*im.Error)
	} else {
		i.Error = mo.Left[error, string](nil)
	}

	i.Statement = im.Statement
	i.Host = im.Host
	i.Duration = im.Duration
	i.Attempt = im.Attempt
	i.GeminiAttempt = im.GeminiAttempt

	if values, ok := im.Values.([]any); ok {
		i.Values = mo.Left[[]any, []byte](values)
	} else {
		i.Values = mo.Right[[]any, []byte]([]byte(im.Values.(string)))
	}

	return nil
}

func (t Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Time.Format(time.RFC3339Nano))
}

func (t *Time) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}

	parsedTime, err := time.Parse(time.RFC3339Nano, str)
	if err != nil {
		return errors.Wrap(err, "failed to parse time")
	}

	t.Time = parsedTime
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

func (d *Duration) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}

	parsedDuration, err := time.ParseDuration(str)
	if err != nil {
		return errors.Wrap(err, "failed to parse duration")
	}

	d.Duration = parsedDuration
	return nil
}

func (i Item) MemoryFootprint() uint64 {
	size := uint64(unsafe.Sizeof(Item{})) + utils.Sizeof(i.Statement) +
		utils.Sizeof(i.Host)

	return size
}
