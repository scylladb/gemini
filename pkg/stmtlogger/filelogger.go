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
	"bytes"
	"compress/gzip"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
	"github.com/samber/mo"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
)

const (
	defaultChanSize   = 1024
	defaultBufferSize = 2048
	errorsOnFileLimit = 5

	bufioWriterSize = 8192

	FlushRate = 10_000
)

type (
	stater interface {
		Name() string
		Stat() (os.FileInfo, error)
	}

	flusher interface {
		io.Writer
		Flush() error
	}

	StmtToFile interface {
		LogStmt(stmt *typedef.Stmt, ts mo.Option[time.Time]) error
		Close() error
	}

	logger struct {
		pool    sync.Pool
		closer  io.Closer
		channel chan []byte
		wg      *sync.WaitGroup
		metrics metrics.ChannelMetrics
		active  atomic.Bool
	}
)

func NewFileLogger(filename string, compression Compression) (StmtToFile, error) {
	if filename == "" {
		return &nopFileLogger{}, nil
	}

	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}

	return NewLogger(filename, fd, compression)
}

func NewLogger(name string, w io.Writer, compression Compression) (StmtToFile, error) {
	var writer flusher
	var closer io.Closer
	switch compression {
	case ZSTDCompression:
		zstdWriter, err := zstd.NewWriter(w,
			zstd.WithEncoderLevel(zstd.SpeedFastest),
			zstd.WithAllLitEntropyCompression(true),
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

	chMetrics := metrics.NewChannelMetrics[[]byte]("statement_logger", name, defaultChanSize)
	ch := make(chan []byte, defaultChanSize)
	wg := &sync.WaitGroup{}

	out := &logger{
		closer:  closer,
		metrics: chMetrics,
		channel: ch,
		wg:      wg,
		pool: sync.Pool{
			New: func() any {
				return bytes.NewBuffer(make([]byte, 0, defaultBufferSize))
			},
		},
	}
	out.active.Store(true)

	wg.Add(1)

	go committer(ch, wg, writer, chMetrics)
	if f, ok := w.(stater); ok {
		go fileSizeReporter(f)
	}

	return out, nil
}

func fileSizeReporter(f stater) {
	timer := time.NewTicker(1 * time.Second)
	defer timer.Stop()

	name := f.Name()

	for range timer.C {
		info, err := f.Stat()
		if err != nil {
			continue
		}

		metrics.FileSizeMetrics.WithLabelValues(name).Set(float64(info.Size()))
	}
}

func (fl *logger) LogStmt(stmt *typedef.Stmt, ts mo.Option[time.Time]) error {
	buffer := fl.pool.Get().(*bytes.Buffer)
	buffer.Reset()

	if err := stmt.PrettyCQLBuffered(buffer); err != nil {
		fl.pool.Put(buffer)
		return err
	}

	if ts.IsPresent() {
		opType := stmt.QueryType.OpType()
		if opType == typedef.OpInsert || opType == typedef.OpUpdate || opType == typedef.OpDelete {
			_, _ = buffer.WriteString(" USING TIMESTAMP ")
			_, _ = buffer.WriteString(strconv.FormatInt(ts.MustGet().UnixMicro(), 10))
		}
	}
	_, _ = buffer.WriteString(";\n")

	data := make([]byte, buffer.Len())
	copy(data, buffer.Bytes())
	fl.pool.Put(buffer)

	if fl.active.Load() {
		fl.channel <- data
		fl.metrics.Inc(data)
	}

	return nil
}

func (fl *logger) Close() error {
	fl.active.Swap(false)
	close(fl.channel)

	fl.wg.Wait()

	if fl.closer != nil {
		if err := fl.closer.Close(); err != nil {
			return err
		}
	}

	return nil
}

func committer(ch <-chan []byte, wg *sync.WaitGroup, writer io.Writer, chMetrics metrics.ChannelMetrics) {
	defer wg.Done()
	errsAtRow := 0

	counter := 0
	for rec := range ch {
		chMetrics.Dec(rec)

		if _, err := writer.Write(rec); err != nil {
			if errors.Is(err, os.ErrClosed) || errsAtRow > errorsOnFileLimit {
				return
			}

			errsAtRow++
			log.Printf("failed to write to writer %+v", err)
		} else {
			errsAtRow = 0
		}

		if counter%FlushRate == 0 {
			if err := writer.(flusher).Flush(); err != nil {
				log.Printf("failed to flush writer %+v", err)
			}
		}
		counter++
	}
}

type nopFileLogger struct{}

func (n *nopFileLogger) LogStmt(_ *typedef.Stmt, _ mo.Option[time.Time]) error { return nil }

func (n *nopFileLogger) Close() error { return nil }
