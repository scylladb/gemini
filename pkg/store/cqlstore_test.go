package store

import (
	"testing"
	"time"

	"github.com/samber/mo"
	"github.com/stretchr/testify/mock"

	"github.com/scylladb/gemini/pkg/typedef"
)

type mockStmtLogger struct {
	mock.Mock
}

func (m *mockStmtLogger) LogStmt(stmt *typedef.Stmt, ts mo.Option[time.Time]) error {
	args := m.Called(stmt, ts)
	return args.Error(0)
}
func (m *mockStmtLogger) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestCqlStore_doMutate(t *testing.T) {
	t.Parallel()
	//tests := []struct {
	//	name                    string
	//	useServerSideTimestamps bool
	//	queryExecError          error
	//	stmtLoggerError         error
	//	ignoredError            bool
	//	expectedError           string
	//	setupMocks              func(*mockSession, *mockQuery, *mockStmtLogger)
	//}{
	//	{
	//		name:                    "successful mutation with client side timestamps",
	//		useServerSideTimestamps: false,
	//		queryExecError:          nil,
	//		stmtLoggerError:         nil,
	//		expectedError:           "",
	//		setupMocks: func(session *mockSession, query *mockQuery, logger *mockStmtLogger) {
	//			query.On("WithTimestamp", mock.AnythingOfType("int64")).Return(query)
	//			query.On("Exec").Return(nil)
	//			query.On("Release").Return()
	//			logger.On("LogStmt", mock.Anything, mock.MatchedBy(func(ts mo.Option[time.Time]) bool {
	//				return ts.IsPresent()
	//			})).Return(nil)
	//		},
	//	},
	//	{
	//		name:                    "successful mutation with server side timestamps",
	//		useServerSideTimestamps: true,
	//		queryExecError:          nil,
	//		stmtLoggerError:         nil,
	//		expectedError:           "",
	//		setupMocks: func(session *mockSession, query *mockQuery, logger *mockStmtLogger) {
	//			query.On("Exec").Return(nil)
	//			query.On("Release").Return()
	//			logger.On("LogStmt", mock.Anything, mock.MatchedBy(func(ts mo.Option[time.Time]) bool {
	//				return ts.IsAbsent()
	//			})).Return(nil)
	//		},
	//	},
	//	{
	//		name:                    "query execution fails with non-ignored error",
	//		useServerSideTimestamps: false,
	//		queryExecError:          errors.New("connection failed"),
	//		stmtLoggerError:         nil,
	//		expectedError:           "[cluster = test-system, query = 'INSERT INTO users VALUES (?, ?)']",
	//		setupMocks: func(session *mockSession, query *mockQuery, logger *mockStmtLogger) {
	//			query.On("WithTimestamp", mock.AnythingOfType("int64")).Return(query)
	//			query.On("Exec").Return(errors.New("connection failed"))
	//			query.On("Release").Return()
	//		},
	//	},
	//	{
	//		name:                    "query execution fails with deadline exceeded (ignored)",
	//		useServerSideTimestamps: false,
	//		queryExecError:          context.DeadlineExceeded,
	//		stmtLoggerError:         nil,
	//		expectedError:           "",
	//		setupMocks: func(session *mockSession, query *mockQuery, logger *mockStmtLogger) {
	//			query.On("WithTimestamp", mock.AnythingOfType("int64")).Return(query)
	//			query.On("Exec").Return(context.DeadlineExceeded)
	//			query.On("Release").Return()
	//			logger.On("LogStmt", mock.Anything, mock.MatchedBy(func(ts mo.Option[time.Time]) bool {
	//				return ts.IsPresent()
	//			})).Return(nil)
	//		},
	//	},
	//	{
	//		name:                    "query execution fails with context canceled (ignored)",
	//		useServerSideTimestamps: false,
	//		queryExecError:          context.Canceled,
	//		stmtLoggerError:         nil,
	//		expectedError:           "",
	//		setupMocks: func(session *mockSession, query *mockQuery, logger *mockStmtLogger) {
	//			query.On("WithTimestamp", mock.AnythingOfType("int64")).Return(query)
	//			query.On("Exec").Return(context.Canceled)
	//			query.On("Release").Return()
	//			logger.On("LogStmt", mock.Anything, mock.MatchedBy(func(ts mo.Option[time.Time]) bool {
	//				return ts.IsPresent()
	//			})).Return(nil)
	//		},
	//	},
	//	{
	//		name:                    "statement logger fails",
	//		useServerSideTimestamps: false,
	//		queryExecError:          nil,
	//		stmtLoggerError:         errors.New("logging failed"),
	//		expectedError:           "logging failed",
	//		setupMocks: func(session *mockSession, query *mockQuery, logger *mockStmtLogger) {
	//			query.On("WithTimestamp", mock.AnythingOfType("int64")).Return(query)
	//			query.On("Exec").Return(nil)
	//			query.On("Release").Return()
	//			logger.On("LogStmt", mock.Anything, mock.MatchedBy(func(ts mo.Option[time.Time]) bool {
	//				return ts.IsPresent()
	//			})).Return(errors.New("logging failed"))
	//		},
	//	},
	//}
	//
	//for _, tt := range tests {
	//	t.Run(tt.name, func(t *testing.T) {
	//		// Setup mocks
	//		mockSession := &mockSession{}
	//		mockQuery := &mockQuery{}
	//		mockLogger := &mockStmtLogger{}
	//
	//		// Setup mock session to return mock query
	//		mockSession.On("Query", "INSERT INTO users VALUES (?, ?)", mock.Anything).Return(mockQuery)
	//
	//		// Setup specific test expectations
	//		tt.setupMocks(mockSession, mockQuery, mockLogger)
	//
	//		// Create test store
	//		store := &cqlStore{
	//			stmtLogger:              mockLogger,
	//			session:                 mockSession,
	//			logger:                  zap.NewNop(),
	//			system:                  "test-system",
	//			useServerSideTimestamps: tt.useServerSideTimestamps,
	//		}
	//
	//		// Create test statement
	//		stmt := &typedef.Stmt{
	//			Query:  &mockQueryBuilder{cql: "INSERT INTO users VALUES (?, ?)"},
	//			Values: []interface{}{123, "John"},
	//		}
	//
	//		// Execute the method
	//		err := store.doMutate(t.Context(), stmt)
	//
	//		// Verify results
	//		if tt.expectedError == "" {
	//			assert.NoError(t, err)
	//		} else {
	//			assert.Error(t, err)
	//			assert.Contains(t, err.Error(), tt.expectedError)
	//		}
	//
	//		// Verify all mock expectations were met
	//		mockSession.AssertExpectations(t)
	//		mockQuery.AssertExpectations(t)
	//		mockLogger.AssertExpectations(t)
	//	})
	//}
}

func TestCqlStore_doMutate_TimestampBehavior(t *testing.T) {
	t.Parallel()

	t.Cleanup(func() {

	})

	//t.Run("client side timestamps set correctly", func(t *testing.T) {
	//	t.Parallel()
	//	mockSession := &mockSession{}
	//	mockQuery := &mockQuery{}
	//	mockStmtLogger := &mockStmtLogger{}
	//
	//	// Capture the timestamp value
	//	var capturedTimestamp int64
	//	mockQuery.On("WithTimestamp", mock.AnythingOfType("int64")).Return(mockQuery).Run(func(args mock.Arguments) {
	//		capturedTimestamp = args.Get(0).(int64)
	//	})
	//	mockQuery.On("Exec").Return(nil)
	//	mockQuery.On("Release").Return()
	//	mockStmtLogger.On("LogStmt", mock.Anything, mock.Anything).Return(nil)
	//	mockSession.On("Query", mock.Anything, mock.Anything).Return(mockQuery)
	//
	//	store := &cqlStore{
	//		stmtLogger:              mockStmtLogger,
	//		session:                 mockSession,
	//		logger:                  zap.NewNop(),
	//		system:                  "test-system",
	//		useServerSideTimestamps: false,
	//	}
	//
	//	stmt := &typedef.Stmt{
	//		Query:  &mockQueryBuilder{cql: "INSERT INTO test VALUES (?)"},
	//		Values: []interface{}{1},
	//	}
	//
	//	beforeTime := time.Now().UnixMicro()
	//	err := store.doMutate(context.Background(), stmt)
	//	afterTime := time.Now().UnixMicro()
	//
	//	assert.NoError(t, err)
	//	assert.True(t, capturedTimestamp >= beforeTime && capturedTimestamp <= afterTime,
	//		"Timestamp should be set to current time in microseconds")
	//})
	//
	//t.Run("server side timestamps do not set WithTimestamp", func(t *testing.T) {
	//	mockSession := &mockSession{}
	//	mockQuery := &mockQuery{}
	//	mockStmtLogger := &mockStmtLogger{}
	//
	//	mockQuery.On("Exec").Return(nil)
	//	mockQuery.On("Release").Return()
	//	mockStmtLogger.On("LogStmt", mock.Anything, mock.Anything).Return(nil)
	//	mockSession.On("Query", mock.Anything, mock.Anything).Return(mockQuery)
	//
	//	store := &cqlStore{
	//		stmtLogger:              mockStmtLogger,
	//		session:                 mockSession,
	//		logger:                  zap.NewNop(),
	//		system:                  "test-system",
	//		useServerSideTimestamps: true,
	//	}
	//
	//	stmt := &typedef.Stmt{
	//		Query:  &mockQueryBuilder{cql: "INSERT INTO test VALUES (?)"},
	//		Values: []interface{}{1},
	//	}
	//
	//	err := store.doMutate(context.Background(), stmt)
	//
	//	assert.NoError(t, err)
	//	// Verify WithTimestamp was never called
	//	mockQuery.AssertNotCalled(t, "WithTimestamp", mock.Anything)
	//})
}
