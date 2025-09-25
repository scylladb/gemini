// Copyright 2025 ScyllaDB
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
	"compress/gzip"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/samber/mo"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/testutils"
)

var CompressionTests = []struct {
	ReadData    func(tb testing.TB, f io.Reader) string
	Compression Compression
}{
	{
		Compression: CompressionNone,
		ReadData: func(tb testing.TB, f io.Reader) string {
			tb.Helper()

			data, err := io.ReadAll(f)
			if err != nil {
				tb.Fatalf("Failed to read file: %s", err)
			}

			return string(data)
		},
	},
	{
		Compression: CompressionGZIP,
		ReadData: func(tb testing.TB, f io.Reader) string {
			tb.Helper()

			reader, err := gzip.NewReader(f)
			if err != nil {
				tb.Fatalf("Failed to read file: %s", err)
			}

			data, err := io.ReadAll(reader)
			if err != nil {
				tb.Fatalf("Failed to read data from file: %s", err)
			}

			return string(data)
		},
	},
	{
		Compression: CompressionZSTD,
		ReadData: func(tb testing.TB, f io.Reader) string {
			tb.Helper()
			reader, err := zstd.NewReader(f)
			if err != nil {
				tb.Fatalf("Failed to read file: %s", err)
			}

			data, err := io.ReadAll(reader)
			if err != nil {
				tb.Fatalf("Failed to read data from file: %s", err)
			}

			return string(data)
		},
	},
}

func TestOutputToFile(t *testing.T) {
	t.Parallel()

	for _, item := range CompressionTests {
		t.Run("Compression_"+item.Compression.String(), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			file := filepath.Join(dir, "test.log")

			logger, err := NewLogger(WithFileLogger(file, item.Compression, zap.NewNop()))
			if err != nil {
				t.Fatalf("Failed to initialize the logger %s", err)
			}

			data := Item{
				Statement:       "INSERT INTO ks1.table1(pk1) VALUES(?)",
				GeneratedValues: mo.Left[[]any, []byte]([]any{1}),
				Error:           mo.Left[error, string](nil),
				Duration:        Duration{Duration: 10 * time.Second},
				Host:            "test_host",
				Start:           Time{Time: time.Now()},
				Type:            TypeTest,
			}

			if err = logger.LogStmt(data); err != nil {
				t.Fatalf("Failed to write log %s", err)
			}

			time.Sleep(1 * time.Second)

			if err = logger.Close(); err != nil {
				t.Fatalf("Failed to close logger %s", err)
			}

			toCompare := strings.Trim(item.ReadData(t, testutils.Must(os.Open(file))), "\n")

			expected := string(testutils.Must(json.Marshal(data)))
			if toCompare != expected {
				t.Fatalf("Query not expected\nExpected: %s\nActual: %s\n", toCompare, expected)
			}
		})
	}
}

func BenchmarkLogger(b *testing.B) {
	runs := []Compression{
		CompressionNone,
		CompressionGZIP,
		CompressionZSTD,
	}

	for _, compression := range runs {
		b.Run(compression.String(), func(b *testing.B) {
			b.ReportAllocs()
			file := filepath.Join(b.TempDir(), "test.json")
			logger := testutils.Must(NewLogger(WithFileLogger(file, compression, zap.NewNop())))
			rows := &atomic.Int64{}

			data := Item{
				Statement:       "INSERT INTO ks1.table1(pk1) VALUES(?)",
				GeneratedValues: mo.Left[[]any, []byte]([]any{1}),
				Error:           mo.Left[error, string](nil),
				Duration:        Duration{Duration: 10 * time.Second},
				Host:            "test_host",
				Start:           Time{Time: time.Now()},
				Type:            TypeTest,
			}

			b.SetParallelism(100)

			b.ResetTimer()
			b.RunParallel(func(p *testing.PB) {
				for p.Next() {
					if err := logger.LogStmt(data); err != nil {
						b.Fatalf("Failed to write to log file: %s", err)
					}

					rows.Add(1)
				}
			})

			if err := logger.Close(); err != nil {
				b.Fatalf("Failed to close logger: %s", err)
			}

			info, _ := os.Stat(file)

			b.ReportMetric(float64(info.Size())/1024/1024, "FS/MB")
			b.ReportMetric(float64(rows.Load()), "Rows")
		})
	}
}
