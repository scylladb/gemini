// Copyright 2025 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package services

import (
	"flag"
	"math/rand/v2"
	"os"
	"testing"

	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/utils"
)

var (
	NoopLogger   bool
	LoggingLevel string
)

func TestMain(m *testing.M) {
	flag.BoolVar(&NoopLogger, "noop-logger", false, "Use noop logger for tests")
	flag.StringVar(&LoggingLevel, "logging-level", "debug", "Logging level for the tests")
	utils.PreallocateRandomString(rand.New(rand.NewPCG(1, 1)), 1<<20)

	flag.Parse()
	os.Exit(m.Run())
}

func getLogger(tb testing.TB) *zap.Logger {
	tb.Helper()
	if NoopLogger {
		return zap.NewNop()
	}

	lvl, err := zap.ParseAtomicLevel(LoggingLevel)
	if err != nil {
		tb.Fatalf("Failed to parse logging level: %v", err)
	}

	config := zap.NewDevelopmentConfig()
	config.Level = lvl
	config.DisableCaller = false
	config.DisableStacktrace = false
	config.Encoding = "console"

	logger, err := config.Build()
	if err != nil {
		tb.Fatalf("Failed to create logger: %v", err)
	}

	return logger
}
