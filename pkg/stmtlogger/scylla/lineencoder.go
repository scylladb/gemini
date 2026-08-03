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
	"bytes"
	"encoding/json"
	"io"
	"time"
)

// lineEncoder writes one Line as JSON straight to w, so a partition's statement
// history never has to be held in memory. It matches the field order and tags of
// the Line struct, which stays the type the files are read back into.
//
// Errors are sticky: the first write error stops all later output and comes back
// from Close.
type lineEncoder struct {
	w   io.Writer
	err error
	n   int64
}

func newLineEncoder(w io.Writer) *lineEncoder {
	return &lineEncoder{w: w}
}

func (e *lineEncoder) raw(s string) {
	if e.err != nil {
		return
	}

	n, err := io.WriteString(e.w, s)
	e.n += int64(n)
	e.err = err
}

func (e *lineEncoder) value(v any) {
	if e.err != nil {
		return
	}

	bs, err := json.Marshal(v)
	if err != nil {
		e.err = err
		return
	}

	n, err := e.w.Write(bs)
	e.n += int64(n)
	e.err = err
}

func (e *lineEncoder) field(name string, v any) {
	e.raw(`"` + name + `":`)
	e.value(v)
	e.raw(`,`)
}

// head opens the object and writes every scalar field of Line. The two array
// fields follow, streamed by array/row/endArray.
//
// The fields are marshaled into a buffer before anything reaches w. A marshal
// failure after the opening brace would leave a line with no closing brace, and
// because it does not poison the underlying writer, the next line would be
// appended to it and both would be unreadable.
func (e *lineEncoder) head(partitions []PartitionInfo, ts time.Time, errStr, query, message string) {
	if e.err != nil {
		return
	}

	var buf bytes.Buffer

	head := &lineEncoder{w: &buf}

	head.raw(`{`)
	head.field("partitionKeys", partitions)
	head.field("timestamp", ts)

	if errStr != "" {
		head.field("err", errStr)
	}

	head.field("query", query)
	head.field("message", message)

	if head.err != nil {
		e.err = head.err
		return
	}

	n, err := e.w.Write(buf.Bytes())
	e.n += int64(n)
	e.err = err
}

func (e *lineEncoder) array(name string) {
	e.raw(`"` + name + `":[`)
}

func (e *lineEncoder) row(first bool, raw json.RawMessage) {
	if !first {
		e.raw(`,`)
	}

	if e.err != nil {
		return
	}

	n, err := e.w.Write(raw)
	e.n += int64(n)
	e.err = err
}

func (e *lineEncoder) endArray(last bool) {
	if last {
		e.raw(`]`)
		return
	}

	e.raw(`],`)
}

func (e *lineEncoder) end() {
	e.raw("}\n")
}

// Close returns the first error hit and the number of bytes written.
func (e *lineEncoder) Close() (int64, error) {
	return e.n, e.err
}
