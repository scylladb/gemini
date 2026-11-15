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

// Package compression writes the statement log files compressed, and reads
// them back whatever compression wrote them.
package compression

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
)

// Algorithm names a compression of the statement log files.
type Algorithm string

const (
	None Algorithm = "none"
	Gzip Algorithm = "gzip"
	Zstd Algorithm = "zstd"
)

var (
	gzipMagic = []byte{0x1f, 0x8b}
	zstdMagic = []byte{0x28, 0xb5, 0x2f, 0xfd}
)

// Parse returns the Algorithm that name names. An empty name is None.
func Parse(name string) (Algorithm, error) {
	switch Algorithm(name) {
	case "", None:
		return None, nil
	case Gzip, Zstd:
		return Algorithm(name), nil
	default:
		return "", fmt.Errorf("unknown compression %q: use none, gzip or zstd", name)
	}
}

// NewWriter returns a writer that compresses into w with algorithm. Close
// flushes the compressed stream. It does not close w.
func NewWriter(w io.Writer, algorithm Algorithm) (io.WriteCloser, error) {
	switch algorithm {
	case "", None:
		return nopCloser{w}, nil
	case Gzip:
		return gzip.NewWriter(w), nil
	case Zstd:
		return zstd.NewWriter(w)
	default:
		return nil, fmt.Errorf("unknown compression %q", algorithm)
	}
}

// NewReader returns a reader of the plain content of r. It detects gzip and
// zstd by the magic bytes at the start of the stream, so a file reads the same
// whatever compression wrote it.
func NewReader(r io.Reader) (io.ReadCloser, error) {
	buffered := bufio.NewReader(r)

	head, err := buffered.Peek(len(zstdMagic))
	if err != nil && len(head) == 0 {
		return io.NopCloser(buffered), nil
	}

	switch {
	case bytes.HasPrefix(head, gzipMagic):
		gz, gzErr := gzip.NewReader(buffered)
		if gzErr != nil {
			return nil, gzErr
		}

		return gz, nil
	case bytes.HasPrefix(head, zstdMagic):
		zr, zErr := zstd.NewReader(buffered)
		if zErr != nil {
			return nil, zErr
		}

		return zr.IOReadCloser(), nil
	default:
		return io.NopCloser(buffered), nil
	}
}

type nopCloser struct {
	io.Writer
}

func (nopCloser) Close() error { return nil }
