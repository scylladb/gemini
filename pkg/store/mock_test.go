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
