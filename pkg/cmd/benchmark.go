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

package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/scylladb/gemini/pkg/benchmarks"
)

var (
	benchHistoryFile         string
	benchmarkPattern         string
	benchPackagePattern      string
	benchTime                string
	benchMem                 bool
	benchCompareWith         string
	benchRegressionThreshold float64
	benchTags                string
	benchNotes               string
	benchCPUInfo             string
)

func Benchmark() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "benchmark",
		Short: "Run benchmarks and track results over time",
		Long: `Run Go benchmarks for specified packages and track results in a history file.
Supports comparison with previous runs and regression detection.`,
		RunE: runBenchmark,
	}

	cmd.Flags().StringVar(&benchHistoryFile, "history", "benchmark_history.json", "Path to benchmark history file")
	cmd.Flags().StringVar(&benchmarkPattern, "bench", ".", "Benchmark pattern to run (default: all)")
	cmd.Flags().StringVar(&benchPackagePattern, "pkg", "./...", "Package pattern to test")
	cmd.Flags().StringVar(&benchTime, "benchtime", "1s", "Benchmark time (e.g., 1s, 10x)")
	cmd.Flags().BoolVar(&benchMem, "benchmem", true, "Include memory allocation statistics")
	cmd.Flags().StringVar(&benchCompareWith, "compare", "", "Compare with specific run (index in history, or 'last')")
	cmd.Flags().Float64Var(&benchRegressionThreshold, "threshold", 10.0, "Regression threshold percentage (default: 10%)")
	cmd.Flags().StringVar(&benchTags, "tags", "", "Comma-separated tags for this run")
	cmd.Flags().StringVar(&benchNotes, "notes", "", "Optional notes about this benchmark run")
	cmd.Flags().StringVar(&benchCPUInfo, "cpu", "", "CPU info (auto-detected if not provided)")

	return cmd
}

func runBenchmark(_ *cobra.Command, _ []string) error {
	// Build benchmark command
	testArgs := []string{"test", benchPackagePattern, "-bench=" + benchmarkPattern, "-benchtime=" + benchTime, "-run=^$"}
	if benchMem {
		testArgs = append(testArgs, "-benchmem")
	}

	// Run benchmark
	benchCmd := exec.Command("go", testArgs...)
	var stdout bytes.Buffer
	benchCmd.Stdout = &stdout
	benchCmd.Stderr = os.Stderr

	if err := benchCmd.Run(); err != nil {
		return fmt.Errorf("benchmark execution failed: %w", err)
	}

	// Parse results
	results, err := benchmarks.ParseBenchmarkOutput(&stdout)
	if err != nil {
		return fmt.Errorf("failed to parse benchmark output: %w", err)
	}

	if len(results) == 0 {
		return fmt.Errorf("no benchmark results found")
	}

	// Get system information
	gitCommit := getGitCommit()
	gitBranch := getGitBranch()
	goVersion := runtime.Version()
	cpu := benchCPUInfo
	if cpu == "" {
		cpu = getCPUInfo()
	}

	// Parse tags
	tagMap := make(map[string]string)
	if benchTags != "" {
		for _, tag := range strings.Split(benchTags, ",") {
			parts := strings.SplitN(tag, "=", 2)
			if len(parts) == 2 {
				tagMap[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			}
		}
	}

	// Create benchmark run
	benchRun := benchmarks.BenchmarkRun{
		Timestamp: time.Now(),
		GitCommit: gitCommit,
		GitBranch: gitBranch,
		GoVersion: goVersion,
		OS:        runtime.GOOS,
		Arch:      runtime.GOARCH,
		CPU:       cpu,
		Results:   results,
		Tags:      tagMap,
		Notes:     benchNotes,
	}

	// Save results
	if err = benchRun.Save(benchHistoryFile); err != nil {
		return fmt.Errorf("failed to save benchmark results: %w", err)
	}

	// Perform comparison if requested
	if benchCompareWith != "" {
		if err = performBenchmarkComparison(&benchRun); err != nil {
			return fmt.Errorf("comparison failed: %w", err)
		}
	}

	return nil
}

func performBenchmarkComparison(newRun *benchmarks.BenchmarkRun) error {
	history, err := benchmarks.LoadHistory(benchHistoryFile)
	if err != nil {
		return err
	}

	if len(history.Runs) < 2 {
		_, _ = fmt.Fprintln(os.Stderr, "\nNot enough history for comparison (need at least 2 runs)")
		return nil
	}

	var oldRun *benchmarks.BenchmarkRun

	if benchCompareWith == "last" {
		// Compare with previous run (second to last)
		oldRun = &history.Runs[len(history.Runs)-2]
	} else if len(history.Runs) >= 2 {
		oldRun = &history.Runs[len(history.Runs)-2]
	}

	if oldRun == nil {
		return fmt.Errorf("could not find run to compare with")
	}

	comparisons := benchmarks.CompareRuns(oldRun, newRun, benchRegressionThreshold)
	benchmarks.PrintComparison(comparisons)

	// Exit with error if regressions detected
	for _, comp := range comparisons {
		if comp.IsRegression {
			return fmt.Errorf("performance regressions detected")
		}
	}

	return nil
}

func getGitCommit() string {
	cmd := exec.Command("git", "rev-parse", "HEAD")
	output, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(output))
}

func getGitBranch() string {
	cmd := exec.Command("git", "rev-parse", "--abbrev-ref", "HEAD")
	output, err := cmd.Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(output))
}

func getCPUInfo() string {
	// Simple CPU detection
	switch runtime.GOOS {
	case "linux":
		cmd := exec.Command("sh", "-c", "cat /proc/cpuinfo | grep 'model name' | head -1 | cut -d':' -f2")
		output, err := cmd.Output()
		if err == nil {
			return strings.TrimSpace(string(output))
		}
	case "darwin":
		cmd := exec.Command("sysctl", "-n", "machdep.cpu.brand_string")
		output, err := cmd.Output()
		if err == nil {
			return strings.TrimSpace(string(output))
		}
	}
	return "unknown"
}
