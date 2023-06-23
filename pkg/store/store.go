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
	"fmt"
	"math/big"
	"os"
	"sort"
	"time"

	"go.uber.org/zap"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/v2/qb"
	"go.uber.org/multierr"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/typedef"
)

type loader interface {
	load(context.Context, qb.Builder, []interface{}) ([]map[string]interface{}, error)
}

type storer interface {
	mutate(context.Context, qb.Builder, ...interface{}) error
}

type storeLoader interface {
	storer
	loader
	close() error
	name() string
}

type Store interface {
	Create(context.Context, qb.Builder, qb.Builder) error
	Mutate(context.Context, qb.Builder, ...interface{}) error
	Check(context.Context, *typedef.Table, qb.Builder, ...interface{}) error
	Close() error
}

type Config struct {
	MaxRetriesMutate        int
	MaxRetriesMutateSleep   time.Duration
	UseServerSideTimestamps bool
}

func New(schema *typedef.Schema, testCluster, oracleCluster *gocql.ClusterConfig, cfg Config, traceOut *os.File, logger *zap.Logger) (Store, error) {
	ops := promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gemini_cql_requests",
		Help: "How many CQL requests processed, partitioned by system and CQL query type aka 'method' (batch, delete, insert, update).",
	}, []string{"system", "method"},
	)

	var oracleStore storeLoader
	var validations bool
	if oracleCluster != nil {
		oracleSession, err := newSession(oracleCluster, traceOut)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to connect to oracle cluster")
		}
		oracleStore = &cqlStore{
			session:                 oracleSession,
			schema:                  schema,
			system:                  "oracle",
			ops:                     ops,
			maxRetriesMutate:        cfg.MaxRetriesMutate + 10,
			maxRetriesMutateSleep:   cfg.MaxRetriesMutateSleep,
			useServerSideTimestamps: cfg.UseServerSideTimestamps,
			logger:                  logger,
		}
		validations = true
	} else {
		oracleStore = &noOpStore{
			system: "oracle",
		}
	}

	testSession, err := newSession(testCluster, traceOut)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to connect to oracle cluster")
	}

	return &delegatingStore{
		testStore: &cqlStore{
			session:                 testSession,
			schema:                  schema,
			system:                  "test",
			ops:                     ops,
			maxRetriesMutate:        cfg.MaxRetriesMutate,
			maxRetriesMutateSleep:   cfg.MaxRetriesMutateSleep,
			useServerSideTimestamps: cfg.UseServerSideTimestamps,
			logger:                  logger,
		},
		oracleStore: oracleStore,
		validations: validations,
		logger:      logger.Named("delegating_store"),
	}, nil
}

type noOpStore struct {
	system string
}

func (n *noOpStore) mutate(context.Context, qb.Builder, ...interface{}) error {
	return nil
}

func (n *noOpStore) load(context.Context, qb.Builder, []interface{}) ([]map[string]interface{}, error) {
	return nil, nil
}

func (n *noOpStore) Close() error {
	return nil
}

func (n *noOpStore) name() string {
	return n.system
}

func (n *noOpStore) close() error {
	return nil
}

type delegatingStore struct {
	oracleStore storeLoader
	testStore   storeLoader
	logger      *zap.Logger
	validations bool
}

func (ds delegatingStore) Create(ctx context.Context, testBuilder, oracleBuilder qb.Builder) error {
	if err := mutate(ctx, ds.oracleStore, oracleBuilder, []interface{}{}); err != nil {
		return errors.Wrap(err, "oracle failed store creation")
	}
	if err := mutate(ctx, ds.testStore, testBuilder, []interface{}{}); err != nil {
		return errors.Wrap(err, "test failed store creation")
	}
	return nil
}

func (ds delegatingStore) Mutate(ctx context.Context, builder qb.Builder, values ...interface{}) error {
	if err := mutate(ctx, ds.oracleStore, builder, values...); err != nil {
		// Oracle failed, transition cannot take place
		ds.logger.Info("oracle failed mutation, transition to next state impossible so continuing with next mutation", zap.Error(err))
		return nil
	}
	return mutate(ctx, ds.testStore, builder, values...)
}

func mutate(ctx context.Context, s storeLoader, builder qb.Builder, values ...interface{}) error {
	if err := s.mutate(ctx, builder, values...); err != nil {
		return errors.Wrapf(err, "unable to apply mutations to the %s store", s.name())
	}
	return nil
}

func (ds delegatingStore) Check(ctx context.Context, table *typedef.Table, builder qb.Builder, values ...interface{}) error {
	testRows, err := ds.testStore.load(ctx, builder, values)
	if err != nil {
		return errors.Wrapf(err, "unable to load check data from the test store")
	}
	oracleRows, err := ds.oracleStore.load(ctx, builder, values)
	if err != nil {
		return errors.Wrapf(err, "unable to load check data from the oracle store")
	}
	if !ds.validations {
		return nil
	}
	if len(testRows) == 0 && len(oracleRows) == 0 {
		return nil
	}
	if len(testRows) != len(oracleRows) {
		testSet := strset.New(pks(table, testRows)...)
		oracleSet := strset.New(pks(table, oracleRows)...)
		missingInTest := strset.Difference(oracleSet, testSet).List()
		missingInOracle := strset.Difference(testSet, oracleSet).List()
		return fmt.Errorf("row count differ (test has %d rows, oracle has %d rows, test is missing rows: %s, oracle is missing rows: %s)",
			len(testRows), len(oracleRows), missingInTest, missingInOracle)
	}
	sort.SliceStable(testRows, func(i, j int) bool {
		return lt(testRows[i], testRows[j])
	})
	sort.SliceStable(oracleRows, func(i, j int) bool {
		return lt(oracleRows[i], oracleRows[j])
	})
	for i, oracleRow := range oracleRows {
		testRow := testRows[i]
		cmp.AllowUnexported()
		diff := cmp.Diff(oracleRow, testRow,
			cmpopts.SortMaps(func(x, y *inf.Dec) bool {
				return x.Cmp(y) < 0
			}),
			cmp.Comparer(func(x, y *inf.Dec) bool {
				return x.Cmp(y) == 0
			}), cmp.Comparer(func(x, y *big.Int) bool {
				return x.Cmp(y) == 0
			}))
		if diff != "" {
			return fmt.Errorf("rows differ (-%v +%v): %v", oracleRow, testRow, diff)
		}
	}
	return nil
}

func (ds delegatingStore) Close() (err error) {
	err = multierr.Append(err, ds.testStore.close())
	err = multierr.Append(err, ds.oracleStore.close())
	return
}
