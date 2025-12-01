// Copyright 2025 ScyllaDB
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
	"errors"
	"net"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
)

func TestNewObserver(t *testing.T) {
	t.Parallel()

	initializer := func(_ string, ty typedef.StatementType) int {
		return int(ty)
	}

	obs := newObserver(initializer)

	assert.NotNil(t, obs)
	assert.NotNil(t, obs.data)
	assert.NotNil(t, obs.initializer)
}

func TestObserver_Initialize(t *testing.T) {
	t.Parallel()

	initializer := func(host string, ty typedef.StatementType) string {
		return host + "-" + ty.String()
	}

	obs := newObserver(initializer)
	host := "127.0.0.1"

	arr := obs.initialize(host)

	assert.Len(t, arr, int(typedef.StatementTypeCount))
	assert.Contains(t, obs.data, host)

	// Verify each statement type was initialized
	for i := range typedef.StatementTypeCount {
		expected := host + "-" + i.String()
		assert.Equal(t, expected, arr[i])
	}
}

func TestObserver_Get(t *testing.T) {
	t.Parallel()

	initializer := func(_ string, ty typedef.StatementType) int {
		return int(ty) * 10
	}

	obs := newObserver(initializer)
	host := "192.168.1.1"

	// First call should initialize
	value := obs.Get(host, typedef.SelectStatementType)
	assert.Equal(t, int(typedef.SelectStatementType)*10, value)

	// Second call should retrieve from cache
	value2 := obs.Get(host, typedef.SelectStatementType)
	assert.Equal(t, value, value2)

	// Different host should initialize separately
	host2 := "192.168.1.2"
	value3 := obs.Get(host2, typedef.InsertStatementType)
	assert.Equal(t, int(typedef.InsertStatementType)*10, value3)
}

func TestNewClusterObserver(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()
	clusterName := stmtlogger.TypeOracle

	obs := NewClusterObserver(nil, logger, clusterName)

	assert.NotNil(t, obs)
	assert.Equal(t, clusterName, obs.clusterName)
	assert.Equal(t, logger, obs.appLogger)
	assert.NotNil(t, obs.goCQLBatchQueries)
	assert.NotNil(t, obs.goCQLBatches)
	assert.NotNil(t, obs.goCQLQueryErrors)
	assert.NotNil(t, obs.goCQLQueries)
	assert.NotNil(t, obs.goCQLQueryTime)
	assert.NotNil(t, obs.goCQLConnections)
}

func TestClusterObserver_ObserveBatch_Success(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()
	clusterName := stmtlogger.TypeTest
	obs := NewClusterObserver(nil, logger, clusterName)

	ctx := WithContextData(t.Context(), &ContextData{
		Statement: typedef.SimpleStmt("INSERT INTO test (id) VALUES (?)", typedef.InsertStatementType),
	})

	host := &gocql.HostInfo{}

	batch := gocql.ObservedBatch{
		Host:     host,
		Err:      nil,
		Start:    time.Now().Add(-100 * time.Millisecond),
		End:      time.Now(),
		Keyspace: "test_keyspace",
		Statements: []string{
			"INSERT INTO test (id) VALUES (?)",
		},
	}

	// Should not panic with nil context data
	obs.ObserveBatch(ctx, batch)
}

func TestClusterObserver_ObserveBatch_WithError(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()
	clusterName := stmtlogger.TypeTest
	obs := NewClusterObserver(nil, logger, clusterName)

	ctx := WithContextData(t.Context(), &ContextData{
		Statement: typedef.SimpleStmt("INSERT INTO test (id) VALUES (?)", typedef.InsertStatementType),
	})

	host := &gocql.HostInfo{}

	testCases := []struct {
		err  error
		name string
	}{
		{
			name: "connection closed error",
			err:  gocql.ErrConnectionClosed,
		},
		{
			name: "host down error",
			err:  gocql.ErrHostDown,
		},
		{
			name: "no connections error",
			err:  gocql.ErrNoConnections,
		},
		{
			name: "generic error",
			err:  errors.New("generic error"),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			batch := gocql.ObservedBatch{
				Host:     host,
				Err:      tc.err,
				Start:    time.Now().Add(-100 * time.Millisecond),
				End:      time.Now(),
				Keyspace: "test_keyspace",
				Statements: []string{
					"INSERT INTO test (id) VALUES (?)",
				},
			}

			// Should not panic
			obs.ObserveBatch(ctx, batch)
		})
	}
}

func TestClusterObserver_ObserveBatch_NilContextData(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()
	obs := NewClusterObserver(nil, logger, stmtlogger.TypeTest)

	// Context without ContextData
	ctx := t.Context()

	host := &gocql.HostInfo{}

	batch := gocql.ObservedBatch{
		Host:     host,
		Err:      nil,
		Start:    time.Now(),
		End:      time.Now(),
		Keyspace: "test",
		Statements: []string{
			"SELECT * FROM test",
		},
	}

	// Should return early without panic
	obs.ObserveBatch(ctx, batch)
}

func TestClusterObserver_ObserveQuery(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()
	clusterName := stmtlogger.TypeTest
	obs := NewClusterObserver(nil, logger, clusterName)

	ctx := WithContextData(t.Context(), &ContextData{
		Statement: typedef.SimpleStmt("SELECT * FROM test WHERE id = ?", typedef.SelectStatementType),
	})

	query := gocql.ObservedQuery{
		Host:      &gocql.HostInfo{},
		Err:       nil,
		Start:     time.Now().Add(-50 * time.Millisecond),
		End:       time.Now(),
		Keyspace:  "test_keyspace",
		Statement: "SELECT * FROM test WHERE id = ?",
	}

	// Should not panic
	obs.ObserveQuery(ctx, query)
}

func TestClusterObserver_ObserveQuery_WithError(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()
	obs := NewClusterObserver(nil, logger, stmtlogger.TypeOracle)

	ctx := WithContextData(t.Context(), &ContextData{
		Statement: typedef.SimpleStmt("SELECT * FROM test", typedef.SelectStatementType),
	})

	host := &gocql.HostInfo{}

	query := gocql.ObservedQuery{
		Host:      host,
		Err:       errors.New("query timeout"),
		Start:     time.Now().Add(-1 * time.Second),
		End:       time.Now(),
		Keyspace:  "test",
		Statement: "SELECT * FROM test",
	}

	// Should not panic
	obs.ObserveQuery(ctx, query)
}

func TestClusterObserver_ObserveQuery_NilContextData(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()
	obs := NewClusterObserver(nil, logger, stmtlogger.TypeTest)

	ctx := t.Context()

	host := &gocql.HostInfo{}

	query := gocql.ObservedQuery{
		Host:      host,
		Err:       nil,
		Start:     time.Now(),
		End:       time.Now(),
		Keyspace:  "test",
		Statement: "SELECT * FROM test",
	}

	// Should return early without panic
	obs.ObserveQuery(ctx, query)
}

func TestClusterObserver_ObserveConnect(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()
	obs := NewClusterObserver(nil, logger, stmtlogger.TypeTest)

	host := &gocql.HostInfo{}
	host.SetHostID("test-host-id")
	host.SetConnectAddress(net.ParseIP("127.0.0.1"))

	connect := gocql.ObservedConnect{
		Host:  host,
		Err:   nil,
		Start: time.Now().Add(-10 * time.Millisecond),
		End:   time.Now(),
	}

	// Should not panic
	obs.ObserveConnect(connect)
}

func TestClusterObserver_ObserveConnect_WithError(t *testing.T) {
	t.Parallel()

	logger := zap.NewNop()
	obs := NewClusterObserver(nil, logger, stmtlogger.TypeOracle)

	host := &gocql.HostInfo{}
	host.SetHostID("test-host-id")
	host.SetConnectAddress(net.ParseIP("192.168.1.1"))

	connect := gocql.ObservedConnect{
		Host:  host,
		Err:   errors.New("connection refused"),
		Start: time.Now().Add(-100 * time.Millisecond),
		End:   time.Now(),
	}

	// Should not panic
	obs.ObserveConnect(connect)
}

func TestObserver_Concurrency(t *testing.T) {
	t.Parallel()

	initializer := func(host string, ty typedef.StatementType) int {
		return len(host) + int(ty)
	}

	obs := newObserver(initializer)

	// Test concurrent access to the observer
	hosts := []string{"host1", "host2", "host3", "host4", "host5"}
	done := make(chan bool)

	for _, host := range hosts {
		host := host // capture loop variable
		go func() {
			for i := range typedef.StatementTypeCount {
				obs.Get(host, i)
			}
			done <- true
		}()
	}

	// Wait for all goroutines to complete
	for range hosts {
		<-done
	}

	// Verify all hosts were initialized
	for _, host := range hosts {
		assert.Contains(t, obs.data, host)
	}
}
