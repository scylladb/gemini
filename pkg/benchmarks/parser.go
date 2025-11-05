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

package benchmarks

import (
	"bufio"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
)

// Go benchmark output format:
// BenchmarkName-8         1000000              1234 ns/op             456 B/op          78 allocs/op
var benchmarkRegex = regexp.MustCompile(`^(Benchmark\S+)(?:-(\d+))?\s+(\d+)\s+([\d.]+)\s+ns/op(?:\s+([\d.]+)\s+MB/s)?(?:\s+(\d+)\s+B/op)?(?:\s+(\d+)\s+allocs/op)?`)

// ParseBenchmarkOutput parses Go benchmark output and returns benchmark results
func ParseBenchmarkOutput(reader io.Reader) ([]BenchmarkResult, error) {
	var results []BenchmarkResult
	scanner := bufio.NewScanner(reader)

	for scanner.Scan() {
		line := scanner.Text()

		// Skip non-benchmark lines
		if !strings.HasPrefix(line, "Benchmark") {
			continue
		}

		result, err := parseBenchmarkLine(line)
		if err != nil {
			// Skip lines that don't match expected format
			continue
		}

		results = append(results, result)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading benchmark output: %w", err)
	}

	return results, nil
}

func parseBenchmarkLine(line string) (BenchmarkResult, error) {
	matches := benchmarkRegex.FindStringSubmatch(line)
	if matches == nil {
		return BenchmarkResult{}, fmt.Errorf("line does not match benchmark format: %s", line)
	}

	result := BenchmarkResult{
		Name: matches[1],
	}

	// Parse parallelism (e.g., -8 means GOMAXPROCS=8)
	if matches[2] != "" {
		parallelism, err := strconv.Atoi(matches[2])
		if err == nil {
			result.Parallelism = parallelism
		}
	}

	// Parse iterations
	if matches[3] != "" {
		iterations, err := strconv.Atoi(matches[3])
		if err == nil {
			result.Iterations = iterations
		}
	}

	// Parse ns/op
	if matches[4] != "" {
		nsPerOp, err := strconv.ParseFloat(matches[4], 64)
		if err == nil {
			result.NsPerOp = nsPerOp
			// Calculate ops/sec
			if nsPerOp > 0 {
				result.OpsPerSec = 1_000_000_000 / nsPerOp
			}
		}
	}

	// Parse MB/s (optional)
	if matches[5] != "" {
		mbPerSec, err := strconv.ParseFloat(matches[5], 64)
		if err == nil {
			result.MBPerSec = mbPerSec
		}
	}

	// Parse B/op (optional)
	if matches[6] != "" {
		bytesPerOp, err := strconv.ParseInt(matches[6], 10, 64)
		if err == nil {
			result.BytesPerOp = bytesPerOp
		}
	}

	// Parse allocs/op (optional)
	if matches[7] != "" {
		allocsPerOp, err := strconv.ParseInt(matches[7], 10, 64)
		if err == nil {
			result.AllocsPerOp = allocsPerOp
		}
	}

	return result, nil
}
