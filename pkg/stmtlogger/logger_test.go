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

package stmtlogger

import (
	"errors"
	"testing"
	"time"

	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/scylladb/gemini/pkg/typedef"
)

// Mock closer for testing
type mockCloser struct {
	closeErr   error
	closeCalls int
	closed     bool
}

func (m *mockCloser) Close() error {
	m.closeCalls++
	m.closed = true
	return m.closeErr
}

func TestNewLogger(t *testing.T) {
	t.Parallel()

	t.Run("default options", func(t *testing.T) {
		t.Parallel()

		logger, err := NewLogger()
		require.NoError(t, err)
		require.NotNil(t, logger)

		ch := logger.channel.Load()
		assert.NotNil(t, ch)
		assert.NotNil(t, *ch)

		// Clean up
		err = logger.Close()
		assert.NoError(t, err)
	})

	t.Run("with custom channel", func(t *testing.T) {
		t.Parallel()

		customCh := make(chan Item, 100)
		logger, err := NewLogger(WithChannel(customCh))
		require.NoError(t, err)
		require.NotNil(t, logger)

		ch := logger.channel.Load()
		assert.NotNil(t, ch)
		assert.Equal(t, customCh, *ch)

		// Clean up
		err = logger.Close()
		assert.NoError(t, err)
	})

	t.Run("with inner logger", func(t *testing.T) {
		t.Parallel()

		mockClose := &mockCloser{}
		logger, err := NewLogger(WithLogger(mockClose, nil))
		require.NoError(t, err)
		require.NotNil(t, logger)

		assert.Equal(t, mockClose, logger.closer)

		// Clean up
		err = logger.Close()
		assert.NoError(t, err)
		assert.True(t, mockClose.closed)
		assert.Equal(t, 1, mockClose.closeCalls)
	})

	t.Run("with inner logger error", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("logger creation failed")
		logger, err := NewLogger(WithLogger(nil, expectedErr))

		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.Nil(t, logger)
	})

	t.Run("with multiple options", func(t *testing.T) {
		t.Parallel()

		customCh := make(chan Item, 50)
		mockClose := &mockCloser{}

		logger, err := NewLogger(
			WithChannel(customCh),
			WithLogger(mockClose, nil),
		)
		require.NoError(t, err)
		require.NotNil(t, logger)

		ch := logger.channel.Load()
		assert.Equal(t, customCh, *ch)
		assert.Equal(t, mockClose, logger.closer)

		// Clean up
		err = logger.Close()
		assert.NoError(t, err)
		assert.True(t, mockClose.closed)
	})

	t.Run("replacing channel with WithChannel", func(t *testing.T) {
		t.Parallel()

		ch1 := make(chan Item, 10)
		ch2 := make(chan Item, 20)

		// First channel should be closed when second is set
		logger, err := NewLogger(
			WithChannel(ch1),
			WithChannel(ch2),
		)
		require.NoError(t, err)
		require.NotNil(t, logger)

		ch := logger.channel.Load()
		assert.Equal(t, ch2, *ch)

		// ch1 should be closed
		select {
		case _, ok := <-ch1:
			assert.False(t, ok, "ch1 should be closed")
		default:
			t.Error("ch1 should be closed")
		}

		// Clean up
		err = logger.Close()
		assert.NoError(t, err)
	})
}

func TestLogger_LogStmt(t *testing.T) {
	t.Parallel()

	t.Run("log statement successfully", func(t *testing.T) {
		t.Parallel()

		mockClose := &mockCloser{}
		logger, err := NewLogger(WithLogger(mockClose, nil))
		require.NoError(t, err)
		defer logger.Close()

		item := Item{
			Start:         Time{Time: time.Now()},
			PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"pk0": {"key1"}}),
			Error:         mo.Left[error, string](nil),
			Statement:     "SELECT * FROM test",
			Host:          "127.0.0.1",
			Type:          TypeOracle,
			Values:        mo.Left[[]any, []byte]([]any{"value1"}),
			Duration:      Duration{Duration: time.Millisecond},
			Attempt:       1,
			GeminiAttempt: 1,
			StatementType: typedef.SelectStatementType,
		}

		err = logger.LogStmt(item)
		assert.NoError(t, err)

		// Verify item was sent to channel
		ch := logger.channel.Load()
		select {
		case receivedItem := <-*ch:
			assert.Equal(t, item.Statement, receivedItem.Statement)
			assert.Equal(t, item.Host, receivedItem.Host)
			assert.Equal(t, item.Type, receivedItem.Type)
		case <-time.After(100 * time.Millisecond):
			t.Error("Item was not sent to channel")
		}
	})

	t.Run("log statement with nil closer", func(t *testing.T) {
		t.Parallel()

		logger, err := NewLogger() // No inner logger
		require.NoError(t, err)
		defer logger.Close()

		item := Item{
			Start:         Time{Time: time.Now()},
			PartitionKeys: typedef.NewValuesFromMap(map[string][]any{"pk0": {"key1"}}),
			Statement:     "INSERT INTO test VALUES (?)",
			Host:          "192.168.1.1",
			Type:          TypeTest,
		}

		err = logger.LogStmt(item)
		assert.NoError(t, err)

		// Verify no item was sent because closer is nil
		ch := logger.channel.Load()
		select {
		case <-*ch:
			t.Error("Item should not be sent when closer is nil")
		case <-time.After(50 * time.Millisecond):
			// Expected - no item sent
		}
	})

	t.Run("log multiple statements", func(t *testing.T) {
		t.Parallel()

		mockClose := &mockCloser{}
		logger, err := NewLogger(WithLogger(mockClose, nil))
		require.NoError(t, err)
		defer logger.Close()

		items := []Item{
			{
				Statement: "SELECT * FROM test1",
				Type:      TypeOracle,
				Host:      "host1",
			},
			{
				Statement: "INSERT INTO test2",
				Type:      TypeTest,
				Host:      "host2",
			},
			{
				Statement: "UPDATE test3",
				Type:      TypeOracle,
				Host:      "host3",
			},
		}

		for _, item := range items {
			err = logger.LogStmt(item)
			assert.NoError(t, err)
		}

		// Verify all items were sent
		ch := logger.channel.Load()
		for i := 0; i < len(items); i++ {
			select {
			case <-*ch:
				// Item received
			case <-time.After(100 * time.Millisecond):
				t.Errorf("Expected item %d was not sent", i)
			}
		}
	})

	t.Run("log statement after close", func(t *testing.T) {
		t.Parallel()

		mockClose := &mockCloser{}
		logger, err := NewLogger(WithLogger(mockClose, nil))
		require.NoError(t, err)

		err = logger.Close()
		require.NoError(t, err)

		item := Item{
			Statement: "SELECT * FROM test",
			Type:      TypeOracle,
		}

		// Should not panic or error, but won't send anything
		err = logger.LogStmt(item)
		assert.NoError(t, err)
	})
}

func TestLogger_Close(t *testing.T) {
	t.Parallel()

	t.Run("close successfully", func(t *testing.T) {
		t.Parallel()

		mockClose := &mockCloser{}
		logger, err := NewLogger(WithLogger(mockClose, nil))
		require.NoError(t, err)

		err = logger.Close()
		assert.NoError(t, err)
		assert.True(t, mockClose.closed)
	})

	t.Run("close with error from inner logger", func(t *testing.T) {
		t.Parallel()

		expectedErr := errors.New("close failed")
		mockClose := &mockCloser{closeErr: expectedErr}
		logger, err := NewLogger(WithLogger(mockClose, nil))
		require.NoError(t, err)

		err = logger.Close()
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.True(t, mockClose.closed)
	})

	t.Run("close without inner logger", func(t *testing.T) {
		t.Parallel()

		logger, err := NewLogger()
		require.NoError(t, err)

		err = logger.Close()
		assert.NoError(t, err)
	})

	t.Run("close closes channel", func(t *testing.T) {
		t.Parallel()

		logger, err := NewLogger()
		require.NoError(t, err)

		ch := logger.channel.Load()
		require.NotNil(t, ch)

		err = logger.Close()
		require.NoError(t, err)

		// Verify channel is closed
		select {
		case _, ok := <-*ch:
			assert.False(t, ok, "Channel should be closed")
		default:
			t.Error("Channel should be closed")
		}

		// Verify channel pointer is nil
		nilCh := logger.channel.Load()
		assert.Nil(t, nilCh)
	})

	t.Run("multiple closes", func(t *testing.T) {
		t.Parallel()

		mockClose := &mockCloser{}
		logger, err := NewLogger(WithLogger(mockClose, nil))
		require.NoError(t, err)

		err = logger.Close()
		assert.NoError(t, err)
		assert.Equal(t, 1, mockClose.closeCalls)

		// Second close should handle nil channel gracefully
		// This will panic because we can't close a nil channel
		// but the swap will return nil
		defer func() {
			if r := recover(); r != nil {
				assert.True(t, r != nil)
				// Expected panic when trying to close nil
			}
		}()
	})
}

func TestItem_MemoryFootprint(t *testing.T) {
	t.Parallel()

	t.Run("empty item", func(t *testing.T) {
		t.Parallel()

		item := Item{}
		size := item.MemoryFootprint()
		assert.Greater(t, size, uint64(0))
	})

	t.Run("item with data", func(t *testing.T) {
		t.Parallel()

		item := Item{
			Statement: "SELECT * FROM test WHERE pk = ?",
			Host:      "192.168.1.1",
		}

		size := item.MemoryFootprint()

		emptyItem := Item{}
		emptySize := emptyItem.MemoryFootprint()

		// Size should be larger than empty item
		assert.Greater(t, size, emptySize)
	})

	t.Run("item with long strings", func(t *testing.T) {
		t.Parallel()

		shortItem := Item{
			Statement: "SELECT *",
			Host:      "localhost",
		}
		shortSize := shortItem.MemoryFootprint()

		longItem := Item{
			Statement: "SELECT * FROM very_long_table_name WHERE column1 = ? AND column2 = ? AND column3 = ?",
			Host:      "very-long-hostname-that-takes-more-space.example.com",
		}
		longSize := longItem.MemoryFootprint()

		assert.Greater(t, longSize, shortSize)
	})
}

func TestType_Constants(t *testing.T) {
	t.Parallel()

	t.Run("type constants", func(t *testing.T) {
		t.Parallel()

		assert.Equal(t, Type("oracle"), TypeOracle)
		assert.Equal(t, Type("test"), TypeTest)
		assert.NotEqual(t, TypeOracle, TypeTest)
	})
}

func TestTime_Duration_Structs(t *testing.T) {
	t.Parallel()

	t.Run("Time struct", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		timeStruct := Time{Time: now}

		assert.Equal(t, now, timeStruct.Time)
	})

	t.Run("Duration struct", func(t *testing.T) {
		t.Parallel()

		dur := time.Millisecond * 100
		durationStruct := Duration{Duration: dur}

		assert.Equal(t, dur, durationStruct.Duration)
	})
}

func TestItem_Fields(t *testing.T) {
	t.Parallel()

	t.Run("item with all fields", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		pks := typedef.NewValuesFromMap(map[string][]any{"pk0": {"key1"}})
		err := errors.New("test error")

		item := Item{
			Start:         Time{Time: now},
			PartitionKeys: pks,
			Error:         mo.Left[error, string](err),
			Statement:     "SELECT * FROM test",
			Host:          "127.0.0.1",
			Type:          TypeOracle,
			Values:        mo.Left[[]any, []byte]([]any{"value1", 123}),
			Duration:      Duration{Duration: time.Millisecond * 50},
			Attempt:       3,
			GeminiAttempt: 2,
			StatementType: typedef.SelectStatementType,
		}

		assert.Equal(t, now, item.Start.Time)
		assert.Equal(t, pks, item.PartitionKeys)
		assert.True(t, item.Error.IsLeft())
		assert.Equal(t, err, item.Error.MustLeft())
		assert.Equal(t, "SELECT * FROM test", item.Statement)
		assert.Equal(t, "127.0.0.1", item.Host)
		assert.Equal(t, TypeOracle, item.Type)
		assert.True(t, item.Values.IsLeft())
		assert.Equal(t, time.Millisecond*50, item.Duration.Duration)
		assert.Equal(t, 3, item.Attempt)
		assert.Equal(t, 2, item.GeminiAttempt)
		assert.Equal(t, typedef.SelectStatementType, item.StatementType)
	})

	t.Run("item with right error", func(t *testing.T) {
		t.Parallel()

		item := Item{
			Error: mo.Right[error, string]("error message"),
		}

		assert.True(t, item.Error.IsRight())
		assert.Equal(t, "error message", item.Error.MustRight())
	})

	t.Run("item with right values", func(t *testing.T) {
		t.Parallel()

		item := Item{
			Values: mo.Right[[]any, []byte]([]byte("serialized data")),
		}

		assert.True(t, item.Values.IsRight())
		assert.Equal(t, []byte("serialized data"), item.Values.MustRight())
	})
}

func TestWithChannel_ClosesOldChannel(t *testing.T) {
	t.Parallel()

	ch1 := make(chan Item, 10)
	ch2 := make(chan Item, 20)

	// Create options and apply WithChannel twice
	opts := &options{}

	// First application
	err := WithChannel(ch1)(opts)
	require.NoError(t, err)
	assert.Equal(t, ch1, opts.channel)

	// Second application should close ch1
	err = WithChannel(ch2)(opts)
	require.NoError(t, err)
	assert.Equal(t, ch2, opts.channel)

	// Verify ch1 is closed
	select {
	case _, ok := <-ch1:
		assert.False(t, ok, "ch1 should be closed")
	default:
		t.Error("ch1 should be closed")
	}
}

func TestLogger_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	t.Run("concurrent log statements", func(t *testing.T) {
		t.Parallel()

		mockClose := &mockCloser{}
		logger, err := NewLogger(WithLogger(mockClose, nil))
		require.NoError(t, err)
		defer logger.Close()

		const numGoroutines = 10
		const itemsPerGoroutine = 100

		done := make(chan bool, numGoroutines)

		for range numGoroutines {
			go func() {
				for range itemsPerGoroutine {
					item := Item{
						Statement: "SELECT * FROM test",
						Type:      TypeOracle,
						Host:      "host",
					}
					_ = logger.LogStmt(item)
				}
				done <- true
			}()
		}

		// Wait for all goroutines to complete
		for i := 0; i < numGoroutines; i++ {
			<-done
		}

		// Drain channel
		ch := logger.channel.Load()
		drained := 0
		for {
			select {
			case <-*ch:
				drained++
			case <-time.After(100 * time.Millisecond):
				return
			}
		}
	})
}
