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
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
)

func Must[T any](data T, err error) T {
	if err != nil {
		panic(err)
	}

	return data
}

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

			logger, err := stmtlogger.NewFileLogger(file, item.Compression)
			if err != nil {
				t.Fatalf("Failed to initialize the logger %s", err)
			}

			stmt := typedef.SimpleStmt("INSERT INTO ks1.table1(pk1) VALUES(1)", typedef.InsertStatementType)

			if err = logger.LogStmt(stmt); err != nil {
				t.Fatalf("Failed to write log %s", err)
			}

			time.Sleep(1 * time.Second)

			if err = logger.Close(); err != nil {
				t.Fatalf("Failed to close logger %s", err)
			}

			toCompare := item.ReadData(t, Must(os.Open(file)))

			if toCompare != "INSERT INTO ks1.table1(pk1) VALUES(1);\n" {
				t.Fatalf("Query not expected: %s", toCompare)
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
			file := filepath.Join(b.TempDir(), "test.log")
			logger := Must(stmtlogger.NewFileLogger(file, compression))
			rows := &atomic.Int64{}

			stmt := typedef.SimpleStmt("SELECT * FROM ks1.table1 WHERE pk1 = data", typedef.SelectStatementType)
			b.SetParallelism(100)

			b.ResetTimer()
			b.RunParallel(func(p *testing.PB) {
				for p.Next() {
					if err := logger.LogStmt(stmt); err != nil {
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
