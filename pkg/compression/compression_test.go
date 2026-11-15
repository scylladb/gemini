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

package compression

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRoundTripEveryAlgorithm(t *testing.T) {
	t.Parallel()

	content := strings.Repeat(`{"statement":"INSERT INTO ks.t (a) VALUES (?)"}`+"\n", 100)

	for _, algorithm := range []Algorithm{None, Gzip, Zstd} {
		var buf bytes.Buffer

		w, err := NewWriter(&buf, algorithm)
		require.NoError(t, err)
		_, err = io.WriteString(w, content)
		require.NoError(t, err)
		require.NoError(t, w.Close())

		if algorithm != None {
			assert.Less(t, buf.Len(), len(content), "%s must compress", algorithm)
		}

		r, err := NewReader(&buf)
		require.NoError(t, err)
		got, err := io.ReadAll(r)
		require.NoError(t, err)
		require.NoError(t, r.Close())

		assert.Equal(t, content, string(got), "%s must read back unchanged", algorithm)
	}
}

func TestReaderOfEmptyFile(t *testing.T) {
	t.Parallel()

	r, err := NewReader(bytes.NewReader(nil))
	require.NoError(t, err)

	got, err := io.ReadAll(r)
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestParse(t *testing.T) {
	t.Parallel()

	for name, want := range map[string]Algorithm{"": None, "none": None, "gzip": Gzip, "zstd": Zstd} {
		got, err := Parse(name)
		require.NoError(t, err)
		assert.Equal(t, want, got)
	}

	_, err := Parse("lz4")
	require.Error(t, err)
}
