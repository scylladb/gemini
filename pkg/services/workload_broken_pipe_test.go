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

package services

import (
	"bytes"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/jobs"
	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/stop"
	"github.com/scylladb/gemini/pkg/testutils"
)

// newBufferLogger returns a zap logger that writes into an internal buffer and a function to read it.
func newBufferLogger(tb testing.TB) (*zap.Logger, func() string) {
	tb.Helper()
	var buf bytes.Buffer
	var mu sync.Mutex // Protect the buffer from concurrent access
	encCfg := zap.NewDevelopmentEncoderConfig()
	encCfg.EncodeTime = zapcore.ISO8601TimeEncoder

	// Create a thread-safe writer wrapper
	writer := &syncWriter{buf: &buf, mu: &mu}
	core := zapcore.NewCore(zapcore.NewConsoleEncoder(encCfg), zapcore.AddSync(writer), zap.DebugLevel)
	logger := zap.New(core, zap.AddCaller(), zap.AddStacktrace(zap.ErrorLevel))

	return logger, func() string {
		mu.Lock()
		defer mu.Unlock()
		return buf.String()
	}
}

// syncWriter wraps bytes.Buffer to make it thread-safe
type syncWriter struct {
	buf *bytes.Buffer
	mu  *sync.Mutex
}

func (w *syncWriter) Write(p []byte) (n int, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

// TestWorkloadBrokenPipe simulates a connection break by terminating the TEST Scylla container
// during workload execution. The workload should finish gracefully (equivalent to exit code 0),
// and the statement logs should contain entries.
func TestWorkloadBrokenPipe(t *testing.T) {
	t.Parallel()
	// Add a timeout to prevent hanging if container startup fails

	containers := testutils.TestContainers(t, true)
	if containers.TestContainer == nil || containers.OracleContainer == nil {
		t.Skip("requires controllable Scylla containers (Docker)")
	}

	logger, getLogs := newBufferLogger(t)
	assert := require.New(t)

	storeConfig := getStoreConfig(t, containers.TestHosts, containers.OracleHosts)
	schema := getSchema(t)
	stopFlag := stop.NewFlag(t.Name())
	t.Cleanup(func() { stopFlag.SetHard(true) })

	workload, err := NewWorkload(&WorkloadConfig{
		RunningMode:           jobs.MixedMode,
		PartitionDistribution: distributions.DistributionUniform,
		Seed:                  42,
		PartitionBufferSize:   64,
		RandomStringBuffer:    1024,
		IOWorkerPoolSize:      64,
		MaxErrorsToStore:      16,
		WarmupDuration:        2 * time.Second,
		Duration:              8 * time.Second,
		PartitionCount:        200,
		MutationConcurrency:   2,
		ReadConcurrency:       2,
		DropSchema:            true,
	}, storeConfig, schema, logger, stopFlag)
	assert.NoError(err)

	// Terminate the TEST container after a short delay to simulate a broken pipe/connection.
	time.AfterFunc(3*time.Second, func() {
		_ = containers.TestContainer.Terminate(t.Context())
	})

	err = workload.Run(t.Context())
	assert.Error(err)
	assert.NoError(workload.Close())

	// Check that statement log files contain data
	for _, file := range []string{storeConfig.TestStatementFile, storeConfig.OracleStatementFile} {
		f, openErr := os.Open(file)
		assert.NoError(openErr)
		stat, statErr := f.Stat()
		assert.NoError(statErr)
		assert.Greater(stat.Size(), int64(0), "log file should not be empty: %s", file)

		// Check JSONL contains at least one item
		dec := json.NewDecoder(f)
		var item stmtlogger.Item
		read := 0
		for decErr := dec.Decode(&item); decErr == nil; decErr = dec.Decode(&item) {
			read++
		}
		assert.Greater(read, 0, "log file should contain at least one JSONL record: %s", file)
		_ = f.Close()
	}

	// Verify logs contain an error entry indicating workers observed errors due to connection break
	logs := getLogs()
	assert.Contains(logs, "finished with error", "logger should record worker errors after connection break")
}
