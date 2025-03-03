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
	"reflect"
	"sort"
	"sync"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/pkg/errors"
	"github.com/scylladb/go-set/strset"
	"go.uber.org/zap"
	"gopkg.in/inf.v0"

	"github.com/scylladb/gemini/pkg/typedef"
)

type ValidatingStore struct {
	oracleStore Driver
	testStore   Driver
	logger      *zap.Logger
}

func (ds ValidatingStore) Create(ctx context.Context, testBuilder, oracleBuilder *typedef.Stmt) error {
	if err := ds.testStore.Execute(ctx, testBuilder); err != nil {
		return errors.Wrap(err, "test db failed store creation")
	}

	if err := ds.oracleStore.Execute(ctx, oracleBuilder); err != nil {
		return errors.Wrap(err, "oracle db failed store creation")
	}

	return nil
}

func (ds ValidatingStore) Mutate(ctx context.Context, stmt *typedef.Stmt) error {
	var (
		wg        sync.WaitGroup
		oracleErr error
	)

	if ds.oracleStore != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			oracleErr = ds.oracleStore.Execute(ctx, stmt)
		}()
	}

	if err := ds.testStore.Execute(ctx, stmt); err != nil {
		return errors.Wrap(err, "unable to apply mutations to the test store")
	}

	wg.Wait()

	if oracleErr != nil {
		return errors.Wrap(oracleErr, "unable to apply mutations to the oracle store")
	}

	return nil
}

func (ds ValidatingStore) Check(ctx context.Context, table *typedef.Table, stmt *typedef.Stmt, detailedDiff bool) error {
	var (
		oracleErr  error
		oracleRows []map[string]any
		wg         sync.WaitGroup
	)

	if ds.oracleStore != nil {
		wg.Add(1)

		go func() {
			defer wg.Done()
			oracleRows, oracleErr = ds.oracleStore.Fetch(ctx, stmt)
		}()
	}

	testRows, testErr := ds.testStore.Fetch(ctx, stmt)

	if testErr != nil {
		return errors.Wrap(testErr, "unable to Load check data from the test store")
	}

	wg.Wait()

	if oracleErr != nil {
		return errors.Wrap(oracleErr, "unable to Load check data from the oracle store")
	}

	if len(testRows) == 0 && len(oracleRows) == 0 {
		return nil
	}
	if len(testRows) != len(oracleRows) {
		if !detailedDiff {
			return fmt.Errorf("rows count differ (test store rows %d, oracle store rows %d, detailed information will be at last attempt)", len(testRows), len(oracleRows))
		}
		testSet := strset.New(pks(table, testRows)...)
		oracleSet := strset.New(pks(table, oracleRows)...)
		missingInTest := strset.Difference(oracleSet, testSet).List()
		missingInOracle := strset.Difference(testSet, oracleSet).List()
		return fmt.Errorf("row count differ (test has %d rows, oracle has %d rows, test is missing rows: %s, oracle is missing rows: %s)",
			len(testRows), len(oracleRows), missingInTest, missingInOracle)
	}
	if reflect.DeepEqual(testRows, oracleRows) {
		return nil
	}
	if !detailedDiff {
		return fmt.Errorf("test and oracle store have difference, detailed information will be at last attempt")
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
