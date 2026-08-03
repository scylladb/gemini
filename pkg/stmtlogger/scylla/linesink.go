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
	"context"
	"io"
)

// lineSink is one statements file. Fetches run concurrently and stream straight
// into it, so admission is what keeps two job errors from interleaving their
// JSONL lines. A writer holds the sink for a whole line, which serializes
// writers on a file but keeps memory at one row instead of one partition
// history.
//
// Admission is a size-one channel instead of a mutex because a queued writer
// waits behind a whole partition scan. A mutex makes that wait uninterruptible,
// so shutdown could not bound it. The channel lets a waiter leave when its
// context is cancelled.
type lineSink struct {
	w      *bufio.Writer
	closer func() error
	// sem admits one writer at a time and is selectable against a context.
	sem chan struct{}
	// failed latches the first write or flush failure. The file cannot take any
	// more bytes after one, so later writers are rejected before they read a
	// history out of _logs that nothing can store.
	failed error
	name   string
	closed bool
}

func newLineSink(name string, w *bufio.Writer, closer func() error) *lineSink {
	return &lineSink{name: name, w: w, closer: closer, sem: make(chan struct{}, 1)}
}

// Write runs fn with the file held, then flushes. Flushing per line keeps the
// file usable for triage while the run is still going.
//
// The flush happens even when fn fails. A failed fetch still writes a complete
// line for the partitions it did read, and that line is only useful once it
// reaches the disk. A flush error wins over fn's error, because it says the file
// itself is broken.
//
// Read errors do not latch: they cost one partition its content and leave the
// file intact. Everything else does.
func (s *lineSink) Write(ctx context.Context, fn func(io.Writer) error) error {
	select {
	case s.sem <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	defer func() { <-s.sem }()

	if s.closed {
		return nil
	}

	if s.failed != nil {
		return s.failed
	}

	err := fn(s.w)

	if flushErr := s.w.Flush(); flushErr != nil {
		s.failed = flushErr

		return flushErr
	}

	if err != nil && !isReadError(err) {
		s.failed = err
	}

	return err
}

func (s *lineSink) Close() error {
	s.sem <- struct{}{}
	defer func() { <-s.sem }()

	if s.closed {
		return nil
	}

	s.closed = true

	return s.closer()
}
