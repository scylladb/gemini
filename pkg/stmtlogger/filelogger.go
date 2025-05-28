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
	"context"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
)

const (
	defaultChanSize   = 1024
	defaultBufferSize = 2048
	errorsOnFileLimit = 5

	bufioWriterSize = 8192 * 4
)

type (
	flusher interface {
		io.Writer
		Flush() error
	}

	StmtToFile interface {
		LogStmt(stmt *typedef.Stmt, ts ...time.Time) error
		Close() error
	}

	logger struct {
		pool    sync.Pool
		closer  io.Closer
		channel chan []byte
		metrics metrics.ChannelMetrics
		wg      *sync.WaitGroup
		active  atomic.Bool
	}
)

func NewFileLogger(ctx context.Context, filename string, compression Compression) (StmtToFile, error) {
	if filename == "" {
		return &nopFileLogger{}, nil
	}

	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}

	return NewLogger(ctx, filename, fd, compression)
}

func NewLogger(ctx context.Context, name string, w io.Writer, compression Compression) (StmtToFile, error) {
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
	if f, ok := w.(*os.File); ok {
		go fileSizeReporter(ctx, f)
	}

	return out, nil
}

func fileSizeReporter(ctx context.Context, f *os.File) {
	timer := time.NewTicker(1 * time.Second)
	defer timer.Stop()

	name := f.Name()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			info, err := f.Stat()
			if err != nil {
				continue
			}

			metrics.FileSizeMetrics.WithLabelValues(name).Set(float64(info.Size()))
		}
	}
}

func (fl *logger) LogStmt(stmt *typedef.Stmt, ts ...time.Time) error {
	buffer := fl.pool.Get().(*bytes.Buffer)
	defer func() {
		buffer.Reset()
		fl.pool.Put(buffer)
	}()

	if err := stmt.PrettyCQLBuffered(buffer); err != nil {
		return err
	}

	opType := stmt.QueryType.OpType()

	if len(ts) > 0 && !ts[0].IsZero() &&
		(opType == typedef.OpInsert || opType == typedef.OpUpdate || opType == typedef.OpDelete) {
		_, _ = buffer.WriteString(" USING TIMESTAMP ")
		_, _ = buffer.WriteString(strconv.FormatInt(ts[0].UnixMicro(), 10))
	}

	_, _ = buffer.WriteString(";\n")

	data := make([]byte, buffer.Len())
	copy(data, buffer.Bytes())

	if fl.active.Load() {
		fl.channel <- data
		fl.metrics.Inc(data)
	}

	return nil
}

func (fl *logger) Close() error {
	fl.active.Swap(false)
	close(fl.channel)

	// Wait for commiter to drain the channel
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

	drain := func(rec []byte) {
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
	}

	counter := 0
	for rec := range ch {
		drain(rec)
		if counter%1000 == 0 {
			if err := writer.(flusher).Flush(); err != nil {
				log.Printf("failed to flush writer %+v", err)
			}
		}
		counter++
	}
}

type nopFileLogger struct{}

func (n *nopFileLogger) LogStmt(_ *typedef.Stmt, _ ...time.Time) error { return nil }

func (n *nopFileLogger) Close() error { return nil }
