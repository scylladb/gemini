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

package stmtlogger

import (
	"testing"

	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func createTestScyllaLoggerForPrepareValues() *ScyllaLogger {
	return &ScyllaLogger{
		logger: zap.NewNop(),
	}
}

func TestPrepareValuesOptimized(t *testing.T) {
	t.Parallel()
	logger := createTestScyllaLoggerForPrepareValues()

	tests := []struct {
		name     string
		values   mo.Either[[]any, []byte]
		expected []string
	}{
		{
			name:     "RightBytes",
			values:   mo.Right[[]any, []byte]([]byte("test")),
			expected: []string{"test"},
		},
		{
			name:     "LeftValues_Nil",
			values:   mo.Left[[]any, []byte](nil),
			expected: nil,
		},
		{
			name:     "LeftValues_WithData",
			values:   mo.Left[[]any, []byte]([]any{"string", 123, true}),
			expected: []string{`"string"`, "123", "true"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := logger.prepareValuesOptimized(tt.values)
			assert.Equal(t, tt.expected, result)
		})
	}
}
