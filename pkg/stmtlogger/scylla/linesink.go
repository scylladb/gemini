// Copyright 2026 ScyllaDB
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

package scylla

import (
	"bufio"
	"io"
	"sync"
)

// lineSink is one statements file. Fetches run concurrently and stream straight
// into it, so the mutex is what keeps two job errors from interleaving their
// JSONL lines. It is held for a whole line, which serializes writers on a file
// but keeps memory at one row instead of one partition history.
type lineSink struct {
	w      *bufio.Writer
	closer func() error
	name   string
	mu     sync.Mutex
	closed bool
}

func newLineSink(name string, w *bufio.Writer, closer func() error) *lineSink {
	return &lineSink{name: name, w: w, closer: closer}
}

// Write runs fn with the file locked, then flushes. Flushing per line keeps the
// file usable for triage while the run is still going.
func (s *lineSink) Write(fn func(io.Writer) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	if err := fn(s.w); err != nil {
		return err
	}

	return s.w.Flush()
}

func (s *lineSink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return nil
	}

	s.closed = true

	return s.closer()
}
