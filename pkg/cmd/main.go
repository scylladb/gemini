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

package main

import (
	"os"
	"runtime"
	"runtime/debug"
	"runtime/trace"
	"time"

	"github.com/scylladb/gemini/pkg/utils"
)

func main() {
	runtime.SetMutexProfileFraction(1)
	runtime.SetBlockProfileRate(1)

	recorder := trace.NewFlightRecorder(trace.FlightRecorderConfig{
		MaxBytes: 1 << 24, // 16 MiB
		MinAge:   1 * time.Hour,
	})

	// Enable flight recorder for continuous tracing
	if err := recorder.Start(); err != nil {
		// Non-fatal: continue execution even if flight recorder fails to start
		_, _ = os.Stderr.WriteString("Warning: failed to start flight recorder: " + err.Error() + "\n")
	}

	defer recorder.Stop()

	status := 0

	if err := rootCmd.Execute(); err != nil {
		status = 1
	}

	utils.ExecuteFinalizers()
	os.Exit(status) //nolint:gocritic
}

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				commit = setting.Value
			case "vcs.time":
				date = setting.Value
			}
		}
	}
}
