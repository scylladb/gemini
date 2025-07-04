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
	"encoding/json"
	"io"
	"os"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/utils"
)

const (
	bufioWriterSize = 8192
)

type (
	stater interface {
		Stat() (os.FileInfo, error)
	}

	flusher interface {
		io.Writer
		Flush() error
	}

	IOWriterLogger struct {
		wg     *sync.WaitGroup
		ch     <-chan Item
		closer io.Closer
	}
)

func NewFileLogger(ch <-chan Item, filename string, compression Compression, logger *zap.Logger) (*IOWriterLogger, error) {
	w, err := utils.CreateFile(filename, false)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create file %q", filename)
	}

	return NewIOWriterLogger(ch, filename, w, compression, logger)
}

func NewIOWriterLogger(ch <-chan Item, name string, input io.Writer, compression Compression, logger *zap.Logger) (*IOWriterLogger, error) {
	writer, closer, err := compression.newWriter(input)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create writer for %q with compression %s", name, compression)
	}

	ioWriterLogger := &IOWriterLogger{
		wg:     &sync.WaitGroup{},
		closer: closer,
		ch:     ch,
	}

	ioWriterLogger.wg.Add(1)
	go ioWriterLogger.committer(
		name,
		ioWriterLogger.wg,
		input,
		writer,
		metrics.NewChannelMetrics("statement_logger", name),
		logger,
	)

	return ioWriterLogger, nil
}

func (i *IOWriterLogger) committer(
	name string,
	wg *sync.WaitGroup,
	originalWriter io.Writer,
	writer flusher,
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
	}()

	fdStater, ok := originalWriter.(stater)
	if !ok {
		logger.Warn("writer does not support stat, file size metrics will not be available",
			zap.String("name", name),
		)

		timer.Stop()
	}

	fileSize := metrics.FileSizeMetrics.WithLabelValues(name)

	for {
		select {
		case <-timer.C:
			if !ok {
				continue
			}

			info, err := fdStater.Stat()
			if err != nil {
				continue
			}

			fileSize.Set(float64(info.Size()))
		case rec, more := <-i.ch:
			if !more {
				return
			}
			chMetrics.Dec()

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

func (i *IOWriterLogger) Close() error {
	i.wg.Wait()

	return i.closer.Close()
}
