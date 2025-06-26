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

package stmtlogger_test

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

	"github.com/gocql/gocql"
	"github.com/klauspost/compress/zstd"
	"github.com/samber/mo"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

func TestOutputToFile(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ReadData    func(tb testing.TB, f io.Reader) string
		Compression stmtlogger.Compression
	}{
		{
			Compression: stmtlogger.NoCompression,
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
			Compression: stmtlogger.GZIPCompression,
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
			Compression: stmtlogger.ZSTDCompression,
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

	for _, item := range tests {
		t.Run("Compression_"+item.Compression.String(), func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			file := filepath.Join(dir, "test.log")

			logger, err := stmtlogger.NewLogger(stmtlogger.WithFileLogger(file, item.Compression, zap.NewNop()))
			if err != nil {
				t.Fatalf("Failed to initialize the logger %s", err)
			}

			data := stmtlogger.Item{
				ID:        gocql.TimeUUID(),
				Statement: "INSERT INTO ks1.table1(pk1) VALUES(?)",
				Values:    mo.Left[typedef.Values, string]([]any{1}),
				Error:     mo.Left[error, string](nil),
				Duration:  stmtlogger.Duration{Duration: 10 * time.Second},
				Host:      "test_host",
				Start:     stmtlogger.Time{Time: time.Now()},
				Type:      stmtlogger.TypeTest,
			}

			if err = logger.LogStmt(data); err != nil {
				t.Fatalf("Failed to write log %s", err)
			}

			time.Sleep(1 * time.Second)

			if err = logger.Close(); err != nil {
				t.Fatalf("Failed to close logger %s", err)
			}

			toCompare := strings.Trim(item.ReadData(t, utils.Must(os.Open(file))), "\n")

			expected := string(utils.Must(json.Marshal(data)))
			if toCompare != expected {
				t.Fatalf("Query not expected\nExpected: %s\nActual: %s\n", toCompare, expected)
			}
		})
	}
}

func BenchmarkLogger(b *testing.B) {
	runs := []stmtlogger.Compression{
		stmtlogger.NoCompression,
		stmtlogger.GZIPCompression,
		stmtlogger.ZSTDCompression,
	}

	for _, compression := range runs {
		b.Run(compression.String(), func(b *testing.B) {
			b.ReportAllocs()
			file := filepath.Join(b.TempDir(), "test.json")
			logger := utils.Must(stmtlogger.NewLogger(stmtlogger.WithFileLogger(file, compression, zap.NewNop())))
			rows := &atomic.Int64{}

			data := stmtlogger.Item{
				ID:        gocql.TimeUUID(),
				Statement: "INSERT INTO ks1.table1(pk1) VALUES(?)",
				Values:    mo.Left[typedef.Values, string]([]any{1}),
				Error:     mo.Left[error, string](nil),
				Duration:  stmtlogger.Duration{Duration: 10 * time.Second},
				Host:      "test_host",
				Start:     stmtlogger.Time{Time: time.Now()},
				Type:      stmtlogger.TypeTest,
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
