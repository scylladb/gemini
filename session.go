package gemini

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/go-set/strset"
)

type Session struct {
	testSession   *gocql.Session
	oracleSession *gocql.Session
	schema        *Schema
}

var (
	ErrReadNoDataReturned = errors.New("read: no data returned")
)

func NewSession(testClusterHost string, oracleClusterHost string) *Session {
	testCluster := gocql.NewCluster(testClusterHost)
	testCluster.Timeout = 5 * time.Second
	testSession, err := testCluster.CreateSession()
	if err != nil {
		panic(err)
	}

	oracleCluster := gocql.NewCluster(oracleClusterHost)
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

func (s *Session) Check(table Table, query string, values ...interface{}) error {
	testIter := s.testSession.Query(query, values...).Iter()
	oracleIter := s.oracleSession.Query(query, values...).Iter()
	defer func() {
		testIter.Close()
		oracleIter.Close()
	}()

	testRows := loadSet(testIter)
	oracleRows := loadSet(oracleIter)
	if len(testRows) == 0 && len(oracleRows) == 0 {
		return ErrReadNoDataReturned
	}
	if len(testRows) != len(oracleRows) {
		testSet := strset.New(pks(table, testRows)...)
		oracleSet := strset.New(pks(table, oracleRows)...)
		fmt.Printf("Missing in Test: %s\n", strset.Difference(oracleSet, testSet).List())
		fmt.Printf("Missing in Oracle: %s\n", strset.Difference(testSet, oracleSet).List())
		return fmt.Errorf("row count differ (%d ne %d)", len(testRows), len(oracleRows))
	}
	sort.SliceStable(testRows, func(i, j int) bool {
		return lt(testRows[i], testRows[j])
	})
	sort.SliceStable(oracleRows, func(i, j int) bool {
		return lt(oracleRows[i], oracleRows[j])
	})
	for i, oracleRow := range oracleRows {
		testRow := testRows[i]
		diff := cmp.Diff(oracleRow, testRow)
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
		cv := row[pk.Name]
		switch pk.Type {
		case "int":
			v, _ := cv.(int)
			values = append(values, pk.Name+"="+strconv.Itoa(v))
		case "bigint":
			v, _ := cv.(int64)
			values = append(values, pk.Name+"="+strconv.FormatInt(v, 10))
		case "uuid":
			v, _ := cv.(gocql.UUID)
			values = append(values, pk.Name+"="+v.String())
		case "blob":
			v, _ := cv.([]byte)
			values = append(values, pk.Name+"="+string(v))
		case "text", "varchar":
			v, _ := cv.(string)
			values = append(values, pk.Name+"="+v)
		case "timestamp", "date":
			v, _ := cv.(time.Time)
			values = append(values, pk.Name+"="+v.String())
		default:
			panic(fmt.Sprintf("not supported type %s", pk))
		}
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
