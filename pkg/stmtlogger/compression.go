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
	"compress/gzip"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/pkg/errors"
)

//go:generate go tool stringer -type Compression -trimprefix Compression
type Compression int

const (
	CompressionNone Compression = iota
	CompressionZSTD
	CompressionGZIP
)

func ParseCompression(value string) (Compression, error) {
	switch value {
	case "none", "":
		return CompressionNone, nil
	case "zstd":
		return CompressionZSTD, nil
	case "gzip":
		return CompressionGZIP, nil
	default:
		return CompressionNone, errors.Errorf("unknown compression %q", value)
	}
}

func (c Compression) newWriter(input io.Writer) (flusher, io.Closer, error) {
	var writer flusher
	var closer io.Closer
	switch c {
	case CompressionZSTD:
		zstdWriter, err := zstd.NewWriter(
			input,
			zstd.WithEncoderLevel(zstd.SpeedBestCompression),
			zstd.WithEncoderCRC(true),
			zstd.WithWindowSize(zstd.MaxWindowSize),
			zstd.WithLowerEncoderMem(false),
		)
		if err != nil {
			return nil, nil, err
		}

		writer = zstdWriter
		closer = zstdWriter
	case CompressionGZIP:
		gzipWriter, err := gzip.NewWriterLevel(input, gzip.BestSpeed)
		if err != nil {
			return nil, nil, err
		}

		writer = gzipWriter
		closer = gzipWriter
	default:
		if cl, ok := input.(io.Closer); ok {
			closer = cl
		}
		writer = bufio.NewWriterSize(input, bufioWriterSize)
	}

	return writer, closerWrapper{
		closer:  closer,
		flusher: writer,
	}, nil
}

type closerWrapper struct {
	closer  io.Closer
	flusher flusher
}

func (c closerWrapper) Close() error {
	if err := c.flusher.Flush(); err != nil {
		return err
	}

	return c.closer.Close()
}
