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

package workpool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/samber/mo"
	"github.com/stretchr/testify/assert"
)

func TestNewWorkers(t *testing.T) {
	t.Parallel()

	t.Run("creates workers with specified count", func(t *testing.T) {
		t.Parallel()
		// Test creating workers with different counts
		counts := []int{1, 5, 10}

		for _, count := range counts {
			w := New(count)
			t.Cleanup(func() {
				_ = w.Close()
			})

			// Verify channel buffer size
			assert.Equal(t, count*ChannelSizeMultiplier, cap(*w.ch.Load()))

			// Verify workers can handle count number of concurrent tasks
			// by sending count tasks and ensuring they all complete
			var wg sync.WaitGroup
			wg.Add(count)

			for i := range count {
				go func(i int) {
					defer wg.Done()
					ch := w.Send(t.Context(), func(_ context.Context) (any, error) {
						return i, nil
					})
					res := <-ch
					assert.True(t, res.IsOk())
					data, _ := res.Get()
					assert.Equal(t, i, data.(int))
					w.Release(ch)
				}(i)
			}

			// Use a timeout to prevent hanging if there's a bug
			doneCh := make(chan struct{})
			go func() {
				wg.Wait()
				close(doneCh)
			}()

			select {
			case <-doneCh:
				// All workers completed successfully
			case <-time.After(2 * time.Second):
				t.Fatalf("Timeout waiting for %d workers to complete", count)
			}
		}
	})

	t.Run("zero workers", func(t *testing.T) {
		t.Parallel()
		assert.Panics(t, func() {
			_ = New(0)
		})
	})
}

func TestWorkersSend(t *testing.T) {
	t.Parallel()

	t.Run("successful task execution", func(t *testing.T) {
		t.Parallel()

		w := New(1)
		t.Cleanup(func() {
			_ = w.Close()
		})

		expectedRows := 1

		ch := w.Send(t.Context(), func(_ context.Context) (any, error) {
			return expectedRows, nil
		})

		// Get the result
		res := <-ch
		assert.True(t, res.IsOk())

		actualRows, err := res.Get()
		assert.NoError(t, err)
		assert.Equal(t, expectedRows, actualRows)

		w.Release(ch)
	})

	t.Run("task execution with error", func(t *testing.T) {
		t.Parallel()
		w := New(1)
		t.Cleanup(func() {
			_ = w.Close()
		})

		expectedErr := errors.New("test error")

		ch := w.Send(t.Context(), func(_ context.Context) (any, error) {
			return nil, expectedErr
		})

		// Get the result
		res := <-ch
		assert.True(t, res.IsError())

		_, err := res.Get()
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)

		w.Release(ch)
	})

	t.Run("context cancellation", func(t *testing.T) {
		t.Parallel()
		w := New(1)
		t.Cleanup(func() {
			_ = w.Close()
		})

		ctx, cancel := context.WithCancel(t.Context())
		cancel() // Cancel immediately

		ch := w.Send(ctx, func(ctx context.Context) (any, error) {
			// This should see the canceled context
			return nil, ctx.Err()
		})

		// Get the result
		res := <-ch
		assert.True(t, res.IsError())

		_, err := res.Get()
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)

		w.Release(ch)
	})

	t.Run("concurrent tasks", func(t *testing.T) {
		t.Parallel()
		workerCount := 5
		taskCount := 20
		w := New(workerCount)
		t.Cleanup(func() {
			_ = w.Close()
		})

		// Create a WaitGroup to wait for all tasks to complete
		var wg sync.WaitGroup
		wg.Add(taskCount)

		// Track the number of completed tasks
		var completed atomic.Int32

		// Launch multiple tasks
		for i := range taskCount {
			go func(taskID int) {
				defer wg.Done()

				ch := w.Send(t.Context(), func(_ context.Context) (any, error) {
					// Sleep to simulate work
					time.Sleep(50 * time.Millisecond)
					return taskID, nil
				})

				// Get the result
				res := <-ch
				assert.True(t, res.IsOk())
				rows, _ := res.Get()
				assert.Equal(t, taskID, rows.(int))

				completed.Add(1)
				w.Release(ch)
			}(i)
		}

		// Wait for all tasks to complete with a timeout
		doneCh := make(chan struct{})
		go func() {
			wg.Wait()
			close(doneCh)
		}()

		select {
		case <-doneCh:
			// All tasks completed
			assert.Equal(t, int32(taskCount), completed.Load())
		case <-time.After(1 * time.Second):
			t.Fatalf("Timeout waiting for concurrent tasks to complete. Only %d/%d completed", completed.Load(), taskCount)
		}
	})

	t.Run("channel reuse from pool", func(t *testing.T) {
		t.Parallel()
		w := New(1)
		t.Cleanup(func() {
			_ = w.Close()
		})

		// Get a channel from the pool
		ch1 := w.Send(t.Context(), func(_ context.Context) (any, error) {
			return 1, nil
		})

		// Receive the result
		<-ch1

		// Release the channel back to the pool
		w.Release(ch1)

		// Get another channel - should be the same one from the pool
		ch2 := w.Send(t.Context(), func(_ context.Context) (any, error) {
			return 2, nil
		})

		// Implementation detail: since sync.Pool is used, we can't guarantee
		// that ch2 is the same as ch1, but we can verify the functionality works
		res := <-ch2
		assert.True(t, res.IsOk())
		rows, _ := res.Get()
		assert.Equal(t, 2, rows.(int))

		w.Release(ch2)
	})
}

func TestWorkersRelease(t *testing.T) {
	t.Parallel()
	t.Run("release channel to pool", func(t *testing.T) {
		t.Parallel()
		w := New(1)
		t.Cleanup(func() {
			_ = w.Close()
		})

		// Create and use multiple channels in a sequence
		for i := range 100 {
			ch := w.Send(t.Context(), func(_ context.Context) (any, error) {
				return i, nil
			})

			// Get result
			res := <-ch
			assert.True(t, res.IsOk())

			// Release channel back to pool
			w.Release(ch)

			// The test passes if we don't run out of channels
		}
	})

	t.Run("attempt to release nil channel", func(t *testing.T) {
		t.Parallel()
		w := New(1)
		t.Cleanup(func() {
			_ = w.Close()
		})

		// This should not panic
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Releasing nil channel should not panic: %v", r)
			}
		}()

		w.Release(nil)
	})
}

func TestWorkersClose(t *testing.T) {
	t.Parallel()
	t.Run("close workers", func(t *testing.T) {
		t.Parallel()
		w := New(2)

		// Close should not panic
		err := w.Close()
		assert.NoError(t, err)
	})

	t.Run("double close", func(t *testing.T) {
		t.Parallel()
		w := New(2)

		// First, close should succeed
		err := w.Close()
		assert.NoError(t, err)

		// Second, close should panic, but we recover
		defer func() {
			r := recover()
			assert.NotNil(t, r, "Expected panic when closing already closed channel")
		}()

		_ = w.Close()
		t.Fatal("Should not reach this point")
	})
}

func TestWorkersEdgeCases(t *testing.T) {
	t.Parallel()
	t.Run("nil callback", func(t *testing.T) {
		t.Parallel()

		w := New(1)
		t.Cleanup(func() {
			_ = w.Close()
		})

		assert.Panics(t, func() {
			_ = w.Send(t.Context(), nil)
		})
	})

	t.Run("nil context", func(t *testing.T) {
		t.Parallel()

		w := New(1)
		t.Cleanup(func() {
			_ = w.Close()
		})

		// Using nil context is technically valid but not recommended
		ch := w.Send(nil, func(ctx context.Context) (any, error) { // nolint:staticcheck
			// Context should be nil
			if ctx != nil {
				return nil, errors.New("expected nil context")
			}
			return true, nil
		})

		res := <-ch
		assert.True(t, res.IsOk())

		rows, err := res.Get()
		assert.NoError(t, err)
		assert.Equal(t, true, rows.(bool))

		w.Release(ch)
	})

	t.Run("large task queue", func(t *testing.T) {
		t.Parallel()

		// Test with many tasks (more than buffer size)
		w := New(1)
		t.Cleanup(func() {
			_ = w.Close()
		})

		// Create tasks that fill the buffer and then some
		taskCount := 1100 // More than the 1024 buffer

		// We'll use a channel to track results
		resultCh := make(chan int, taskCount)

		// Launch tasks in a separate goroutine to avoid blocking
		go func() {
			for i := range taskCount {
				ch := w.Send(t.Context(), func(_ context.Context) (any, error) {
					return i, nil
				})

				go func(ch chan mo.Result[any], _ int) {
					res := <-ch
					if res.IsOk() {
						rows, _ := res.Get()
						resultCh <- rows.(int)
					}
					w.Release(ch)
				}(ch, i)
			}
		}()

		// Collect and check results
		receivedCount := 0
		results := make(map[int]bool)

		// Wait for all results with timeout
		timeout := time.After(5 * time.Second)
		for receivedCount < taskCount {
			select {
			case id := <-resultCh:
				results[id] = true
				receivedCount++
			case <-timeout:
				t.Fatalf("Timeout waiting for all tasks to complete. Got %d of %d", receivedCount, taskCount)
			}
		}

		// Verify we got all expected IDs
		assert.Equal(t, taskCount, len(results), "Should have received all task IDs")
		for i := 0; i < taskCount; i++ {
			assert.True(t, results[i], "Missing result for task %d", i)
		}
	})
}

func BenchmarkWorkers(b *testing.B) {
	b.Run("single worker", func(b *testing.B) {
		w := New(1)
		b.Cleanup(func() {
			_ = w.Close()
		})

		b.ResetTimer()
		for b.Loop() {
			ch := w.Send(b.Context(), func(_ context.Context) (any, error) {
				return 1, nil
			})
			<-ch
			w.Release(ch)
		}
	})

	b.Run("multiple workers", func(b *testing.B) {
		w := New(10)
		b.Cleanup(func() {
			_ = w.Close()
		})

		b.ResetTimer()
		for b.Loop() {
			ch := w.Send(b.Context(), func(_ context.Context) (any, error) {
				return 1, nil
			})
			<-ch
			w.Release(ch)
		}
	})

	b.Run("concurrent load", func(b *testing.B) {
		w := New(256)
		b.Cleanup(func() {
			_ = w.Close()
		})

		b.ResetTimer()
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				ch := w.Send(b.Context(), func(_ context.Context) (any, error) {
					return 1, nil
				})
				<-ch
				w.Release(ch)
			}
		})
	})
}
