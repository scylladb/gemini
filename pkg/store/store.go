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

package store

import (
	"context"

	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/typedef"
)

type (
	Driver interface {
		Execute(ctx context.Context, stmt *typedef.Stmt) error
		Fetch(ctx context.Context, stmt *typedef.Stmt) ([]map[string]any, error)
	}

	Store interface {
		Create(context.Context, *typedef.Stmt, *typedef.Stmt) error
		Mutate(context.Context, *typedef.Stmt) error
		Check(context.Context, *typedef.Table, *typedef.Stmt, bool) error
	}
)

func New(logger *zap.Logger, testStore, oracleStore Driver) (*ValidatingStore, error) {
	return &ValidatingStore{
		testStore:   testStore,
		oracleStore: oracleStore,
		logger:      logger.Named("delegating_store"),
	}, nil
}
