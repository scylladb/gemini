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
	"io"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/scylladb/gemini/pkg/typedef"
)

const (
	defaultChanSize   = 1000
	errorsOnFileLimit = 5
)

type StmtToFile interface {
	LogStmt(*typedef.Stmt)
	LogStmtWithTimeStamp(stmt *typedef.Stmt, ts time.Time)
	Close() error
}

type logger struct {
	fd                   io.Writer
	activeChannel        atomic.Pointer[loggerChan]
	channel              loggerChan
	isFileNonOperational bool
}

type loggerChan chan logRec

type logRec struct {
	stmt *typedef.Stmt
	ts   time.Time
}

func (fl *logger) LogStmt(stmt *typedef.Stmt) {
	ch := fl.activeChannel.Load()
	if ch != nil {
		*ch <- logRec{
			stmt: stmt,
		}
	}
}

func (fl *logger) LogStmtWithTimeStamp(stmt *typedef.Stmt, ts time.Time) {
	ch := fl.activeChannel.Load()
	if ch != nil {
		*ch <- logRec{
			stmt: stmt,
			ts:   ts,
		}
	}
}

func (fl *logger) Close() error {
	if closer, ok := fl.fd.(io.Closer); ok {
		return closer.Close()
	}

	return nil
}

func (fl *logger) committer() {
	var err2 error

	defer func() {
		fl.activeChannel.Swap(nil)
		close(fl.channel)
	}()

	errsAtRow := 0

	for rec := range fl.channel {
		if fl.isFileNonOperational {
			continue
		}

		query, err := rec.stmt.PrettyCQL()
		if err != nil {
			log.Printf("failed to pretty print query: %s", err)
			continue
		}

		_, err1 := fl.fd.Write([]byte(query))
		opType := rec.stmt.QueryType.OpType()
		if rec.ts.IsZero() || !(opType == typedef.OpInsert || opType == typedef.OpUpdate || opType == typedef.OpDelete) {
			_, err2 = fl.fd.Write([]byte(";\n"))
		} else {
			_, err2 = fl.fd.Write([]byte(" USING TIMESTAMP " + strconv.FormatInt(rec.ts.UnixNano()/1000, 10) + ";\n"))
		}
		if err2 == nil && err1 == nil {
			errsAtRow = 0
			continue
		}

		if errors.Is(err2, os.ErrClosed) || errors.Is(err1, os.ErrClosed) {
			fl.isFileNonOperational = true
			return
		}

		errsAtRow++
		if errsAtRow > errorsOnFileLimit {
			fl.isFileNonOperational = true
		}

		if err2 != nil {
			err1 = err2
		}

		log.Printf("failed to write to writer %v", err1)
		return
	}
}

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
	out := &logger{
		fd:      w,
		channel: make(loggerChan, defaultChanSize),
	}
	out.activeChannel.Store(&out.channel)

	go out.committer()
	return out, nil
}

type nopFileLogger struct{}

func (n *nopFileLogger) LogStmtWithTimeStamp(_ *typedef.Stmt, _ time.Time) {}

func (n *nopFileLogger) Close() error { return nil }

func (n *nopFileLogger) LogStmt(_ *typedef.Stmt) {}
