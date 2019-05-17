package store

import (
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/scylladb/gemini"
	"github.com/scylladb/gocqlx/qb"
	"go.uber.org/multierr"
)

type cqlStore struct {
	session *gocql.Session
	schema  *gemini.Schema
}

func (cs *cqlStore) mutate(builder qb.Builder, ts time.Time, values ...interface{}) error {
	query, _ := builder.ToCql()
	var tsUsec int64 = ts.UnixNano() / 1000
	if err := cs.session.Query(query, values...).WithTimestamp(tsUsec).Exec(); err != nil {
		return errors.Errorf("%v [cluster = test, query = '%s']", err, query)
	}
	return nil
}

func (cs *cqlStore) load(builder qb.Builder, values []interface{}) (result []map[string]interface{}, err error) {
	query, _ := builder.ToCql()
	testIter := cs.session.Query(query, values...).Iter()
	oracleIter := cs.session.Query(query, values...).Iter()
	defer func() {
		if e := testIter.Close(); e != nil {
			err = multierr.Append(err, errors.Errorf("test system failed: %s", e.Error()))
		}
		if e := oracleIter.Close(); e != nil {
			err = multierr.Append(err, errors.Errorf("oracle failed: %s", e.Error()))
		}
	}()
	result = loadSet(testIter)
	return
}

func (cs cqlStore) close() error {
	cs.session.Close()
	return nil
}

func newSession(hosts []string) *gocql.Session {
	cluster := gocql.NewCluster(hosts...)
	cluster.Timeout = 5 * time.Second
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	return session
}
