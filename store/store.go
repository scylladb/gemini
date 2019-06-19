package store

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/scylladb/gemini"
	"github.com/scylladb/go-set/strset"
	"github.com/scylladb/gocqlx/qb"
	"go.uber.org/multierr"
	"gopkg.in/inf.v0"
)

type loader interface {
	load(context.Context, qb.Builder, []interface{}) ([]map[string]interface{}, error)
}

type storer interface {
	mutate(context.Context, qb.Builder, time.Time, ...interface{}) error
}

type storeLoader interface {
	storer
	loader
	close() error
	name() string
}

type Store interface {
	Mutate(context.Context, qb.Builder, ...interface{}) error
	Check(context.Context, *gemini.Table, qb.Builder, ...interface{}) error
	Close() error
}

func New(schema *gemini.Schema, testCluster *gocql.ClusterConfig, oracleCluster *gocql.ClusterConfig) Store {
	ops := promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gemini_cql_requests",
		Help: "How many CQL requests processed, partitioned by system and CQL query type aka 'method' (batch, delete, insert, update).",
	}, []string{"system", "method"},
	)
	return &delegatingStore{
		testStore: &cqlStore{
			session: newSession(testCluster),
			schema:  schema,
			system:  "test",
			ops:     ops,
		},
		oracleStore: &cqlStore{
			session: newSession(oracleCluster),
			schema:  schema,
			system:  "oracle",
			ops:     ops,
		},
	}
}

type delegatingStore struct {
	oracleStore storeLoader
	testStore   storeLoader
}

func (ds delegatingStore) Mutate(ctx context.Context, builder qb.Builder, values ...interface{}) (err error) {
	ts := time.Now()
	err = multierr.Append(err, mutate(ctx, ds.testStore, ts, builder, values...))
	err = multierr.Append(err, mutate(ctx, ds.oracleStore, ts, builder, values...))
	return
}

func mutate(ctx context.Context, s storeLoader, ts time.Time, builder qb.Builder, values ...interface{}) error {
	if err := s.mutate(ctx, builder, ts, values...); err != nil {
		return errors.Wrapf(err, "unable to apply mutations to the %s store", s.name())
	}
	return nil
}

func (ds delegatingStore) Check(ctx context.Context, table *gemini.Table, builder qb.Builder, values ...interface{}) error {
	testRows, err := load(ctx, ds.testStore, builder, values)
	if err != nil {
		return errors.Wrapf(err, "unable to load check data from the test store")
	}
	oracleRows, err := load(ctx, ds.oracleStore, builder, values)
	if err != nil {
		return errors.Wrapf(err, "unable to load check data from the oracle store")
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

func load(ctx context.Context, l loader, builder qb.Builder, values []interface{}) ([]map[string]interface{}, error) {
	return l.load(ctx, builder, values)
}

func (ds delegatingStore) Close() (err error) {
	err = multierr.Append(err, ds.testStore.close())
	err = multierr.Append(err, ds.oracleStore.close())
	return
}
