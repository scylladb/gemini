package store

import (
	"context"
	"fmt"
	"math/big"
	"sort"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-set/strset"
	"go.uber.org/multierr"
	"gopkg.in/inf.v0"

	"github.com/pkg/errors"
	"github.com/scylladb/gemini"
	"github.com/scylladb/gocqlx/qb"
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
}

type Store interface {
	Mutate(context.Context, qb.Builder, ...interface{}) error
	Check(context.Context, *gemini.Table, qb.Builder, ...interface{}) error
	Close() error
}

func New(schema *gemini.Schema, testHosts []string, oracleHosts []string) Store {
	return &delegatingStore{
		testStore: &cqlStore{
			session: newSession(testHosts),
			schema:  schema,
		},
		oracleStore: &cqlStore{
			session: newSession(oracleHosts),
			schema:  schema,
		},
	}
}

type delegatingStore struct {
	oracleStore storeLoader
	testStore   storeLoader
}

func (ds delegatingStore) Mutate(ctx context.Context, builder qb.Builder, values ...interface{}) error {
	ts := time.Now()
	if err := ds.testStore.mutate(ctx, builder, ts, values...); err != nil {
		return errors.Wrapf(err, "unable to apply mutations to the test store")
	}
	if err := ds.oracleStore.mutate(ctx, builder, ts, values...); err != nil {
		return errors.Wrapf(err, "unable to apply mutations to the oracle store")
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
		return errors.Wrapf(err, "unable to load check data from the test store")
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
