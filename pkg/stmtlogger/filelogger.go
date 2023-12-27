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

type fileLogger struct {
	fd                   *os.File
	activeChannel        atomic.Pointer[loggerChan]
	channel              loggerChan
	filename             string
	isFileNonOperational bool
}

type loggerChan chan logRec

type logRec struct {
	stmt *typedef.Stmt
	ts   time.Time
}

func (fl *fileLogger) LogStmt(stmt *typedef.Stmt) {
	ch := fl.activeChannel.Load()
	if ch != nil {
		*ch <- logRec{
			stmt: stmt,
		}
	}
}

func (fl *fileLogger) LogStmtWithTimeStamp(stmt *typedef.Stmt, ts time.Time) {
	ch := fl.activeChannel.Load()
	if ch != nil {
		*ch <- logRec{
			stmt: stmt,
			ts:   ts,
		}
	}
}

func (fl *fileLogger) Close() error {
	return fl.fd.Close()
}

func (fl *fileLogger) committer() {
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

		_, err1 := fl.fd.Write([]byte(rec.stmt.PrettyCQL()))
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
		log.Printf("failed to write to file %q: %s", fl.filename, err1)
		return
	}
}

func NewFileLogger(filename string) (StmtToFile, error) {
	if filename == "" {
		return &nopFileLogger{}, nil
	}
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	out := &fileLogger{
		filename: filename,
		fd:       fd,
		channel:  make(loggerChan, defaultChanSize),
	}
	out.activeChannel.Store(&out.channel)

	go out.committer()
	return out, nil
}

type nopFileLogger struct{}

func (n *nopFileLogger) LogStmtWithTimeStamp(_ *typedef.Stmt, _ time.Time) {}

func (n *nopFileLogger) Close() error { return nil }

func (n *nopFileLogger) LogStmt(_ *typedef.Stmt) {}
