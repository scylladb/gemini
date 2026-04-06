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
	"sync"
	"time"
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/mo"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

const (
	defaultChanSize = 131072 // 128K items — large buffer to avoid blocking query goroutines
)

const (
	TypeOracle Type = "oracle"
	TypeTest   Type = "test"
)

type (
	Type string

	Item struct {
		Start          Time                     `json:"s"`
		Error          mo.Either[error, string] `json:"e,omitempty"`
		Type           Type                     `json:"-"`
		Statement      string                   `json:"q"`
		Host           string                   `json:"h"`
		Values         mo.Either[[]any, []byte] `json:"v"`
		RecentSuccess  []uint64                 `json:"rs,omitempty"`
		PartitionKeys  typedef.PartitionKeys    `json:"partitionKeys"`
		Duration       Duration                 `json:"d"`
		FirstSuccessNS uint64                   `json:"fs,omitempty"`
		LastSuccessNS  uint64                   `json:"ls,omitempty"`
		LastFailureNS  uint64                   `json:"lf,omitempty"`
		Attempt        int                      `json:"d_a"`
		GeminiAttempt  int                      `json:"g_a"`
		StatementType  typedef.StatementType    `json:"-"`
	}

	Duration struct {
		Duration time.Duration
	}

	Time struct {
		Time time.Time
	}

	ValidationHuman struct {
		First   string   `json:"first,omitempty"`
		Last    string   `json:"last,omitempty"`
		Failure string   `json:"failure,omitempty"`
		Recent  []string `json:"recent,omitempty"`
	}

	Logger struct {
		closer       io.Closer
		itemsMetric  prometheus.Gauge
		enqueueTotal prometheus.Counter
		dropTotal    prometheus.Counter
		ch           chan Item
		done         chan struct{}
		closeOnce    sync.Once
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

	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return nil, err
		}
	}

	l := &Logger{
		closer:       o.innerLogger,
		ch:           o.channel,
		done:         make(chan struct{}),
		itemsMetric:  metrics.StatementLoggerItems,
		enqueueTotal: metrics.StatementLoggerEnqueuedTotal,
		dropTotal:    metrics.StatementLoggerDropped,
	}

	return l, nil
}

func (l *Logger) LogStmt(item Item) error {
	if item.StatementType.IsSchema() {
		return nil
	}
	if item.StatementType.IsSelect() && item.FirstSuccessNS == 0 && item.LastSuccessNS == 0 && item.LastFailureNS == 0 {
		return nil
	}

	if l.closer == nil {
		return nil
	}

	select {
	case <-l.done:
		return nil
	default:
	}

	// Block until the item is accepted or the logger is closed.
	select {
	case l.ch <- item:
		l.itemsMetric.Inc()
		l.enqueueTotal.Inc()
	case <-l.done:
		// Logger closed while we were waiting — discard.
	}

	return nil
}

func (l *Logger) Close() error {
	var innerErr error

	l.closeOnce.Do(func() {
		// Signal all in-flight and future LogStmt calls to bail out.
		close(l.done)

		// Close the data channel so downstream consumers see EOF.
		if l.ch != nil {
			close(l.ch)
		}

		if l.closer != nil {
			innerErr = l.closer.Close()
		}
	})

	return innerErr
}

func (i Item) MemoryFootprint() uint64 {
	size := uint64(unsafe.Sizeof(Item{})) + utils.Sizeof(i.Statement) +
		utils.Sizeof(i.Host)

	return size
}
