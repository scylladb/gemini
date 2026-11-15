// Copyright 2026 ScyllaDB
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

package scylla

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/compression"
)

// TestStatementFileCompression: --statement-log-file-compression must reach
// the statements file, and the file must read back whole after Close.
func TestStatementFileCompression(t *testing.T) {
	t.Parallel()

	content := strings.Repeat(`{"partitionKeys":[],"statements":[]}`+"\n", 50)

	for _, algorithm := range []compression.Algorithm{compression.None, compression.Gzip, compression.Zstd} {
		path := filepath.Join(t.TempDir(), "statements.jsonl")
		lg := &Logger{logger: zap.NewNop(), fileCompression: algorithm}

		w, closeFile, err := lg.openStatementFile(path)
		require.NoError(t, err)
		_, err = w.WriteString(content)
		require.NoError(t, err)
		require.NoError(t, closeFile())

		raw, err := os.ReadFile(path)
		require.NoError(t, err)
		if algorithm != compression.None {
			assert.Less(t, len(raw), len(content), "%s must compress the file", algorithm)
		}

		f, err := os.Open(path)
		require.NoError(t, err)
		t.Cleanup(func() { _ = f.Close() })

		r, err := compression.NewReader(f)
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		assert.Equal(t, content, string(got), "%s must read back whole", algorithm)
	}
}
