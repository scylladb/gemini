package gemini

import (
	"fmt"
	"math/big"
	"sort"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/go-set/strset"
	"go.uber.org/multierr"
	"gopkg.in/inf.v0"
)

type Session struct {
	testSession   *gocql.Session
	oracleSession *gocql.Session
	schema        *Schema
}

type JobError struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
	Query     string    `json:"query"`
}

func NewSession(testClusterHost []string, oracleClusterHost []string) *Session {
	testCluster := gocql.NewCluster(testClusterHost...)
	testCluster.Timeout = 5 * time.Second
	testSession, err := testCluster.CreateSession()
	if err != nil {
		panic(err)
	}

	oracleCluster := gocql.NewCluster(oracleClusterHost...)
	oracleCluster.Timeout = 5 * time.Second
	oracleSession, err := oracleCluster.CreateSession()
	if err != nil {
		panic(err)
	}

	return &Session{
		testSession:   testSession,
		oracleSession: oracleSession,
	}
}

func (s *Session) Close() {
	s.testSession.Close()
	s.oracleSession.Close()
}

func (s *Session) Mutate(query string, values ...interface{}) error {
	if err := s.testSession.Query(query, values...).Exec(); err != nil {
		return fmt.Errorf("%v [cluster = test, query = '%s']", err, query)
	}
	if err := s.oracleSession.Query(query, values...).Exec(); err != nil {
		return fmt.Errorf("%v [cluster = oracle, query = '%s']", err, query)
	}
	return nil
}

func (s *Session) Check(table Table, query string, values ...interface{}) (err error) {
	testIter := s.testSession.Query(query, values...).Iter()
	oracleIter := s.oracleSession.Query(query, values...).Iter()
	defer func() {
		if e := testIter.Close(); e != nil {
			err = multierr.Append(err, errors.Errorf("test system failed: %s", err.Error()))
		}
		if e := oracleIter.Close(); e != nil {
			err = multierr.Append(err, errors.Errorf("oracle failed: %s", err.Error()))
		}
	}()

	testRows := loadSet(testIter)
	oracleRows := loadSet(oracleIter)
	if len(testRows) == 0 && len(oracleRows) == 0 {
		// Both empty is fine
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

func pks(t Table, rows []map[string]interface{}) []string {
	var keySet []string
	for _, row := range rows {
		keys := make([]string, 0, len(t.PartitionKeys)+len(t.ClusteringKeys))
		keys = extractRowValues(keys, t.PartitionKeys, row)
		keys = extractRowValues(keys, t.ClusteringKeys, row)
		keySet = append(keySet, strings.Join(keys, ", 	"))
	}
	return keySet
}

func extractRowValues(values []string, columns []ColumnDef, row map[string]interface{}) []string {
	for _, pk := range columns {
		values = append(values, fmt.Sprintf(pk.Name+"=%v", row[pk.Name]))
	}
	return values
}

func lt(mi, mj map[string]interface{}) bool {
	switch mis := mi["pk0"].(type) {
	case []byte:
		mjs, _ := mj["pk0"].([]byte)
		return string(mis) < string(mjs)
	case string:
		mjs, _ := mj["pk0"].(string)
		return mis < mjs
	case int:
		mjs, _ := mj["pk0"].(int)
		return mis < mjs
	case gocql.UUID:
		mjs, _ := mj["pk0"].(gocql.UUID)
		return mis.String() < mjs.String()
	case time.Time:
		mjs, _ := mj["pk0"].(time.Time)
		return mis.UnixNano() < mjs.UnixNano()
	default:
		panic(fmt.Sprintf("unhandled type %T!\n", mis))
	}
}

func loadSet(iter *gocql.Iter) []map[string]interface{} {
	var rows []map[string]interface{}
	for {
		row := make(map[string]interface{})
		if !iter.MapScan(row) {
			break
		}
		rows = append(rows, row)
	}
	return rows
}
