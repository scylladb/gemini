// Copyright 2019 ScyllaDB
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
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/gocql/gocql"
	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

const (
	defaultChanSize   = 8192
	errorsOnFileLimit = 5

	bufioWriterSize = 8192

	FlushRate = 10_000
)

type (
	stater interface {
		Stat() (os.FileInfo, error)
	}

	flusher interface {
		io.Writer
		Flush() error
	}

	Item struct {
		Start         Time                  `json:"s"`
		Error         error                 `json:"e,omitempty"`
		Statement     string                `json:"q"`
		Host          string                `json:"h"`
		Values        typedef.Values        `json:"v"`
		Duration      Duration              `json:"d"`
		Attempt       int                   `json:"d_a"`
		GeminiAttempt int                   `json:"g_a"`
		ID            gocql.UUID            `json:"id"`
		Type          typedef.StatementType `json:"-"`
	}

	StmtToFile interface {
		LogStmt(Item) error
		Close() error
	}

	Duration struct {
		Duration time.Duration
	}

	Time struct {
		Time time.Time
	}

	Logger struct {
		wg      *sync.WaitGroup
		channel chan<- Item
		metrics metrics.ChannelMetrics
		active  atomic.Bool
	}
)

func NewFileLogger(filename string, compression Compression, logger *zap.Logger) (*Logger, error) {
	if filename == "" {
		return nil, nil
	}

	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}

	return NewLogger(filename, fd, compression, logger)
}

func NewLogger(name string, w io.Writer, compression Compression, logger *zap.Logger) (*Logger, error) {
	var writer flusher
	var closer io.Closer
	switch compression {
	case ZSTDCompression:
		zstdWriter, err := zstd.NewWriter(w,
			zstd.WithEncoderLevel(zstd.SpeedBestCompression),
			zstd.WithEncoderCRC(true),
			zstd.WithEncoderConcurrency(2),
			zstd.WithWindowSize(zstd.MaxWindowSize),
			zstd.WithLowerEncoderMem(true),
		)
		if err != nil {
			return nil, err
		}

		writer = bufio.NewWriterSize(zstdWriter, bufioWriterSize)
		closer = zstdWriter
	case GZIPCompression:
		gzipWriter, err := gzip.NewWriterLevel(w, gzip.BestSpeed)
		if err != nil {
			return nil, err
		}

		writer = bufio.NewWriterSize(gzipWriter, bufioWriterSize)
		closer = gzipWriter
	default:
		if c, ok := w.(io.Closer); ok {
			closer = c
		}
		writer = bufio.NewWriterSize(w, bufioWriterSize)
	}

	chMetrics := metrics.NewChannelMetrics[Item]("statement_logger", name, defaultChanSize)
	ch := make(chan Item, defaultChanSize)
	wg := &sync.WaitGroup{}
	out := &Logger{
		metrics: chMetrics,
		channel: ch,
		wg:      wg,
	}

	out.wg.Add(1)
	go committer(name, wg, ch, writer, closer, chMetrics, logger)

	out.active.Store(true)

	return out, nil
}

func (fl *Logger) LogStmt(item Item) error {
	if fl.active.Load() {
		item.Values = item.Values.Copy()
		fl.channel <- item
		fl.metrics.Inc(item)
	}

	return nil
}

func committer(
	name string,
	wg *sync.WaitGroup,
	ch <-chan Item,
	writer flusher,
	closer io.Closer,
	chMetrics metrics.ChannelMetrics,
	logger *zap.Logger,
) {
	defer wg.Done()

	encoder := json.NewEncoder(writer)
	encoder.SetEscapeHTML(false)

	timer := time.NewTicker(5 * time.Second)
	defer func() {
		timer.Stop()
		if err := writer.Flush(); err != nil {
			logger.Error("failed to flush writer",
				zap.Error(err),
			)
		}

		if err := closer.Close(); err != nil {
			logger.Error("failed to close the file",
				zap.Error(err),
			)
		}
	}()

	for {
		select {
		case <-timer.C:
			if f, ok := writer.(stater); ok {
				info, err := f.Stat()
				if err != nil {
					continue
				}

				metrics.FileSizeMetrics.WithLabelValues(name).Set(float64(info.Size()))
			}
		case rec, more := <-ch:
			if !more {
				return
			}
			chMetrics.Dec(rec)

			if err := encoder.Encode(rec); err != nil {
				if errors.Is(err, os.ErrClosed) {
					return
				}

				logger.Error("failed to write to writer",
					zap.Error(err),
					zap.Any("data", rec),
				)
			}
		}
	}
}

func (fl *Logger) Close() error {
	fl.active.Swap(false)
	close(fl.channel)
	fl.wg.Wait()

	return nil
}

func (t Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Time.UnixMicro())
}

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.Duration.String())
}

func (i Item) MemoryFootprint() uint64 {
	return uint64(unsafe.Sizeof(Item{})) + utils.Sizeof(i.Statement) +
		utils.Sizeof(i.Host) + i.Values.MemoryFootprint()
}
