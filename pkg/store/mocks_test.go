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

	"github.com/stretchr/testify/mock"

	"github.com/scylladb/gemini/pkg/typedef"
)

// mockStoreLoader is a mock implementation of storeLoader interface
type mockStoreLoader struct {
	mock.Mock
}

func (m *mockStoreLoader) mutate(ctx context.Context, stmt *typedef.Stmt) error {
	args := m.Called(ctx, stmt)
	return args.Error(0)
}

func (m *mockStoreLoader) load(ctx context.Context, stmt *typedef.Stmt) (Rows, error) {
	args := m.Called(ctx, stmt)
	return args.Get(0).(Rows), args.Error(1)
}

func (m *mockStoreLoader) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockStoreLoader) name() string {
	args := m.Called()
	return args.String(0)
}
