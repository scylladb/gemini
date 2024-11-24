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
	"context"
	"io"
	"log"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"go.uber.org/multierr"

	"github.com/scylladb/gemini/pkg/typedef"
)

const (
	defaultChanSize   = 1024
	defaultBufferSize = 2048
	errorsOnFileLimit = 5
)

type (
	StmtToFile interface {
		LogStmt(stmt *typedef.Stmt, ts ...time.Time) error
		Close() error
	}

	logger struct {
		writer  *bufio.Writer
		fd      io.Writer
		channel chan *bytes.Buffer
		cancel  context.CancelFunc
		pool    sync.Pool
		wg      sync.WaitGroup
		active  atomic.Bool
	}
)

func NewFileLogger(filename string) (StmtToFile, error) {
	if filename == "" {
		return &nopFileLogger{}, nil
	}

	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}

	return NewLogger(fd)
}

func NewLogger(w io.Writer) (StmtToFile, error) {
	ctx, cancel := context.WithCancel(context.Background())

	out := &logger{
		writer:  bufio.NewWriterSize(w, 8192),
		fd:      w,
		channel: make(chan *bytes.Buffer, defaultChanSize),
		cancel:  cancel,
		pool: sync.Pool{
			New: func() any {
				return bytes.NewBuffer(make([]byte, 0, defaultBufferSize))
			},
		},
	}
	out.active.Store(true)

	go out.committer(ctx)
	return out, nil
}

func (fl *logger) LogStmt(stmt *typedef.Stmt, ts ...time.Time) error {
	buffer := fl.pool.Get().(*bytes.Buffer)
	if err := stmt.PrettyCQLBuffered(buffer); err != nil {
		return err
	}

	opType := stmt.QueryType.OpType()

	if len(ts) > 0 && !ts[0].IsZero() && (opType == typedef.OpInsert || opType == typedef.OpUpdate || opType == typedef.OpDelete) {
		buffer.WriteString(" USING TIMESTAMP ")
		buffer.WriteString(strconv.FormatInt(ts[0].UnixMicro(), 10))
	}

	buffer.WriteString(";\n")

	if fl.active.Load() {
		fl.channel <- buffer
	}

	return nil
}

func (fl *logger) Close() error {
	fl.cancel()
	fl.active.Swap(false)
	close(fl.channel)

	// Wait for commiter to drain the channel
	fl.wg.Wait()

	err := multierr.Append(nil, fl.writer.Flush())

	if closer, ok := fl.fd.(io.Closer); ok {
		err = multierr.Append(err, closer.Close())
	}

	return err
}

func (fl *logger) committer(ctx context.Context) {
	fl.wg.Add(1)
	defer fl.wg.Done()
	errsAtRow := 0

	drain := func(rec *bytes.Buffer) {
		defer func() {
			rec.Reset()
			fl.pool.Put(rec)
		}()

		if _, err := rec.WriteTo(fl.writer); err != nil {
			if errors.Is(err, os.ErrClosed) || errsAtRow > errorsOnFileLimit {
				return
			}
			errsAtRow++
			log.Printf("failed to write to writer %+v", err)
		} else {
			errsAtRow = 0
		}
	}

	for {
		select {
		case <-ctx.Done():
			for rec := range fl.channel {
				drain(rec)
			}
			return
		case rec, ok := <-fl.channel:
			if !ok {
				return
			}

			drain(rec)
		}
	}
}

type nopFileLogger struct{}

func (n *nopFileLogger) LogStmt(_ *typedef.Stmt, _ ...time.Time) error { return nil }

func (n *nopFileLogger) Close() error { return nil }
