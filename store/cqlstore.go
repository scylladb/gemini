package store

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/scylladb/gemini"
	"github.com/scylladb/gocqlx/qb"
	"go.uber.org/multierr"
)

type cqlStore struct {
	session *gocql.Session
	schema  *gemini.Schema
	system  string
	ops     *prometheus.CounterVec
}

func (cs *cqlStore) name() string {
	return cs.system
}

func (cs *cqlStore) mutate(ctx context.Context, builder qb.Builder, ts time.Time, values ...interface{}) error {
	query, _ := builder.ToCql()
	var tsUsec int64 = ts.UnixNano() / 1000
	if err := cs.session.Query(query, values...).WithContext(ctx).WithTimestamp(tsUsec).Exec(); err != nil {
		if err == context.DeadlineExceeded {
			fmt.Printf("system=%s has exceeded it's dealine for mutation query='%s', error=%s", cs.system, query, err)
		}
		if !ignore(err) {
			return errors.Errorf("%v [cluster = %s, query = '%s']", err, cs.system, query)
		}
	}
	cs.ops.WithLabelValues(cs.system, opType(builder)).Inc()
	return nil
}

func (cs *cqlStore) load(ctx context.Context, builder qb.Builder, values []interface{}) (result []map[string]interface{}, err error) {
	query, _ := builder.ToCql()
	iter := cs.session.Query(query, values...).WithContext(ctx).Iter()
	cs.ops.WithLabelValues(cs.system, opType(builder)).Inc()
	defer func() {
		if e := iter.Close(); err != nil {
			if e == context.DeadlineExceeded {
				fmt.Printf("system=%s has exceeded it's dealine for load query='%s', error=%s", cs.system, query, e)
			}
			if !ignore(e) {
				err = multierr.Append(err, errors.Errorf("system failed: %s", e.Error()))
			}
		}
	}()
	result = loadSet(iter)
	return
}

func (cs cqlStore) close() error {
	cs.session.Close()
	return nil
}

func newSession(cluster *gocql.ClusterConfig) *gocql.Session {
	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	return session
}

func ignore(err error) bool {
	if err == nil {
		return true
	}
	switch err {
	case context.Canceled, context.DeadlineExceeded:
		return true
	default:
		return false
	}
}

func opType(builder qb.Builder) string {
	switch builder.(type) {
	case *qb.InsertBuilder:
		return "insert"
	case *qb.DeleteBuilder:
		return "delete"
	case *qb.UpdateBuilder:
		return "update"
	case *qb.SelectBuilder:
		return "select"
	case *qb.BatchBuilder:
		return "batch"
	default:
		return "unknown"
	}
}
