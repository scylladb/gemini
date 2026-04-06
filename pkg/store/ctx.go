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
	"sync"
	"time"

	"github.com/scylladb/gemini/pkg/typedef"
)

type QueryContextKey string

const ContextDataKey QueryContextKey = "QueryContextData"

type ContextData struct {
	Statement     *typedef.Stmt
	Timestamp     time.Time
	GeminiAttempt int
}

var contextDataPool = sync.Pool{
	New: func() any {
		return &ContextData{}
	},
}

// AcquireContextData gets a ContextData from the pool and initializes it.
func AcquireContextData(stmt *typedef.Stmt, attempt int) *ContextData {
	cd := contextDataPool.Get().(*ContextData)
	cd.Statement = stmt
	cd.Timestamp = time.Now().UTC()
	cd.GeminiAttempt = attempt
	return cd
}

// ReleaseContextData returns a ContextData to the pool.
func ReleaseContextData(cd *ContextData) {
	cd.Statement = nil
	contextDataPool.Put(cd)
}

func WithContextData(ctx context.Context, data *ContextData) context.Context {
	return context.WithValue(ctx, ContextDataKey, data)
}

func MustGetContextData(ctx context.Context) *ContextData {
	value := ctx.Value(ContextDataKey)

	if value == nil {
		return nil
	}

	data, ok := value.(*ContextData)

	if !ok {
		panic("context does not contain QueryContextData")
	}

	return data
}
