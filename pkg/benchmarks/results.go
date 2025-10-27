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
//nolint:forbidigo

package benchmarks

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// BenchmarkResult represents a single benchmark test result
type BenchmarkResult struct {
	Name        string  `json:"name"`          // Benchmark name
	NsPerOp     float64 `json:"ns_per_op"`     // Nanoseconds per operation
	OpsPerSec   float64 `json:"ops_per_sec"`   // Operations per second
	AllocsPerOp int64   `json:"allocs_per_op"` // Allocations per operation
	BytesPerOp  int64   `json:"bytes_per_op"`  // Bytes allocated per operation
	MBPerSec    float64 `json:"mb_per_sec"`    // MB/s throughput (if applicable)
	Iterations  int     `json:"iterations"`    // Number of iterations run
	Parallelism int     `json:"parallelism"`   // Parallelism level (GOMAXPROCS)
}

// BenchmarkRun represents a complete benchmark run with metadata
type BenchmarkRun struct {
	Timestamp time.Time         `json:"timestamp"`
	Tags      map[string]string `json:"tags"`
	GitCommit string            `json:"git_commit"`
	GitBranch string            `json:"git_branch"`
	GoVersion string            `json:"go_version"`
	OS        string            `json:"os"`
	Arch      string            `json:"arch"`
	CPU       string            `json:"cpu"`
	Notes     string            `json:"notes"`
	Results   []BenchmarkResult `json:"results"`
}

// BenchmarkHistory stores historical benchmark runs
type BenchmarkHistory struct {
	Runs []BenchmarkRun `json:"runs"`
}

// Save saves the benchmark run to a JSON file
func (br *BenchmarkRun) Save(filepath string) error {
	// Load existing history
	history := &BenchmarkHistory{}
	if data, err := os.ReadFile(filepath); err == nil {
		if err = json.Unmarshal(data, history); err != nil {
			return fmt.Errorf("failed to parse existing history: %w", err)
		}
	}

	// Append new run
	history.Runs = append(history.Runs, *br)

	// Save to file
	data, err := json.MarshalIndent(history, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal history: %w", err)
	}

	if err = os.WriteFile(filepath, data, 0o644); err != nil {
		return fmt.Errorf("failed to write history file: %w", err)
	}

	return nil
}

// LoadHistory loads benchmark history from a JSON file
func LoadHistory(filepath string) (*BenchmarkHistory, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		if os.IsNotExist(err) {
			return &BenchmarkHistory{Runs: []BenchmarkRun{}}, nil
		}
		return nil, fmt.Errorf("failed to read history file: %w", err)
	}

	var history BenchmarkHistory
	if err = json.Unmarshal(data, &history); err != nil {
		return nil, fmt.Errorf("failed to parse history: %w", err)
	}

	return &history, nil
}

// ComparisonResult represents the comparison between two benchmark results
type ComparisonResult struct {
	Name               string  `json:"name"`
	OldNsPerOp         float64 `json:"old_ns_per_op"`
	NewNsPerOp         float64 `json:"new_ns_per_op"`
	SpeedupPercent     float64 `json:"speedup_percent"` // Positive means faster
	OldAllocsPerOp     int64   `json:"old_allocs_per_op"`
	NewAllocsPerOp     int64   `json:"new_allocs_per_op"`
	AllocsDeltaPercent float64 `json:"allocs_delta_percent"` // Negative means fewer allocations
	OldBytesPerOp      int64   `json:"old_bytes_per_op"`
	NewBytesPerOp      int64   `json:"new_bytes_per_op"`
	BytesDeltaPercent  float64 `json:"bytes_delta_percent"` // Negative means less memory
	IsRegression       bool    `json:"is_regression"`       // True if performance degraded
}

// CompareRuns compares two benchmark runs and returns comparison results
func CompareRuns(oldRun, newRun *BenchmarkRun, regressionThreshold float64) []ComparisonResult {
	// Create a map of old results for quick lookup
	oldResults := make(map[string]BenchmarkResult)
	for _, result := range oldRun.Results {
		oldResults[result.Name] = result
	}

	comparisons := make([]ComparisonResult, 0, len(newRun.Results))

	for _, newResult := range newRun.Results {
		oldResult, exists := oldResults[newResult.Name]
		if !exists {
			// New benchmark, skip comparison
			continue
		}

		comparison := ComparisonResult{
			Name:           newResult.Name,
			OldNsPerOp:     oldResult.NsPerOp,
			NewNsPerOp:     newResult.NsPerOp,
			OldAllocsPerOp: oldResult.AllocsPerOp,
			NewAllocsPerOp: newResult.AllocsPerOp,
			OldBytesPerOp:  oldResult.BytesPerOp,
			NewBytesPerOp:  newResult.BytesPerOp,
		}

		// Calculate speedup (positive means faster, negative means slower)
		if oldResult.NsPerOp > 0 {
			comparison.SpeedupPercent = ((oldResult.NsPerOp - newResult.NsPerOp) / oldResult.NsPerOp) * 100
		}

		// Calculate allocation delta (negative means fewer allocations)
		if oldResult.AllocsPerOp > 0 {
			comparison.AllocsDeltaPercent = ((float64(newResult.AllocsPerOp) - float64(oldResult.AllocsPerOp)) / float64(oldResult.AllocsPerOp)) * 100
		}

		// Calculate bytes delta (negative means less memory)
		if oldResult.BytesPerOp > 0 {
			comparison.BytesDeltaPercent = ((float64(newResult.BytesPerOp) - float64(oldResult.BytesPerOp)) / float64(oldResult.BytesPerOp)) * 100
		}

		// Determine if this is a regression
		// Regression if: slower by threshold OR more allocations OR more memory
		comparison.IsRegression = comparison.SpeedupPercent < -regressionThreshold ||
			comparison.AllocsDeltaPercent > regressionThreshold ||
			comparison.BytesDeltaPercent > regressionThreshold

		comparisons = append(comparisons, comparison)
	}

	return comparisons
}

// PrintComparison prints a human-readable comparison report
//
//nolint:forbidigo
func PrintComparison(comparisons []ComparisonResult) {
	if len(comparisons) == 0 {
		fmt.Println("No comparable benchmarks found.")
		return
	}

	fmt.Println("\n=== Benchmark Comparison ===")

	hasRegressions := false
	for _, comp := range comparisons {
		fmt.Printf("Benchmark: %s\n", comp.Name)
		fmt.Printf("  Speed:       %.2f ns/op → %.2f ns/op (%.2f%% %s)\n",
			comp.OldNsPerOp, comp.NewNsPerOp, abs(comp.SpeedupPercent), speedLabel(comp.SpeedupPercent))

		if comp.OldAllocsPerOp > 0 || comp.NewAllocsPerOp > 0 {
			fmt.Printf("  Allocations: %d → %d (%.2f%%)\n",
				comp.OldAllocsPerOp, comp.NewAllocsPerOp, comp.AllocsDeltaPercent)
		}

		if comp.OldBytesPerOp > 0 || comp.NewBytesPerOp > 0 {
			fmt.Printf("  Memory:      %d B → %d B (%.2f%%)\n",
				comp.OldBytesPerOp, comp.NewBytesPerOp, comp.BytesDeltaPercent)
		}

		if comp.IsRegression {
			fmt.Println("  ⚠️  REGRESSION DETECTED")
			hasRegressions = true
		}
		fmt.Println()
	}

	if hasRegressions {
		_, _ = fmt.Println("⚠️  WARNING: Performance regressions detected!")
	} else {
		fmt.Println("✓ No regressions detected")
	}
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func speedLabel(speedupPercent float64) string {
	if speedupPercent > 0 {
		return "faster"
	} else if speedupPercent < 0 {
		return "slower"
	}
	return "same"
}
