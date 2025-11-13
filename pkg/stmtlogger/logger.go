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

	"github.com/samber/mo"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
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
		channel atomic.Pointer[chan Item]
		metrics metrics.ChannelMetrics
	}

	options struct {
		channel     chan Item
		innerLogger io.Closer
	}

	Option func(*options) error
)

func WithChannel(ch chan Item) Option {
	return func(o *options) error {
		if o.channel != nil {
			close(o.channel)
		}
		o.channel = ch
		return nil
	}
}

func WithLogger(logger io.Closer, err error) Option {
	return func(o *options) error {
		if err != nil {
			return err
		}

		o.innerLogger = logger
		return nil
	}
}

func NewLogger(opts ...Option) (*Logger, error) {
	o := options{
		channel: make(chan Item, defaultChanSize),
	}

	chMetrics := metrics.NewChannelMetrics("statement_logger", "statement_logger")

	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}

	l := &Logger{
		metrics: chMetrics,
		closer:  o.innerLogger,
	}

	l.channel.Store(&o.channel)

	return l, nil
}

func (l *Logger) LogStmt(item Item) error {
	if l.closer == nil {
		return nil
	}

	if ch := l.channel.Load(); ch != nil {
		*ch <- item
		l.metrics.Inc()
	}

	return nil
}

func (l *Logger) Close() error {
	old := l.channel.Swap(nil)
	close(*old)

	if l.closer != nil {
		return l.closer.Close()
	}

	return nil
}

func (i Item) MemoryFootprint() uint64 {
	size := uint64(unsafe.Sizeof(Item{})) + utils.Sizeof(i.Statement) +
		utils.Sizeof(i.Host)

	return size
}
