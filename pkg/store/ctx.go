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

package store

import (
	"context"

	"github.com/scylladb/gemini/pkg/typedef"
)

type QueryContextKey string

const ContextDataKey QueryContextKey = "QueryContextData"

type ContextData struct {
	Statement     *typedef.Stmt
	GeminiAttempt int
}

func WithContextData(ctx context.Context, data *ContextData) context.Context {
	return context.WithValue(ctx, ContextDataKey, data)
}

func GetContextData(ctx context.Context) (*ContextData, bool) {
	data, ok := ctx.Value(ContextDataKey).(*ContextData)

	if !ok {
		return nil, false
	}

	return data, true
}

func MustGetContextData(ctx context.Context) *ContextData {
	data, ok := ctx.Value(ContextDataKey).(*ContextData)

	if !ok {
		panic("context does not contain QueryContextData")
	}

	return data
}
