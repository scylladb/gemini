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

package joberror

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/scylladb/gemini/pkg/typedef"
)

func TestJobError_Error(t *testing.T) {
	t.Parallel()

	timestamp := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)
	err := errors.New("test error")

	jobErr := JobError{
		Timestamp: timestamp,
		Err:       err,
		Message:   "test message",
		Query:     "SELECT * FROM test",
		StmtType:  typedef.SelectStatementType,
	}

	expected := "JobError(err=test error): test message (stmt-type=SelectStatement, query=SELECT * FROM test) time=2023-01-01T12:00:00Z"
	actual := jobErr.Error()

	if actual != expected {
		t.Errorf("Expected %q, got %q", expected, actual)
	}
}

func TestJobError_ErrorWithEmptyFields(t *testing.T) {
	t.Parallel()

	timestamp := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)

	jobErr := JobError{
		Timestamp: timestamp,
		Err:       nil,
		Message:   "",
		Query:     "",
		StmtType:  typedef.SelectStatementType,
	}

	expected := "JobError(err=<nil>):  (stmt-type=SelectStatement, query=) time=2023-01-01T12:00:00Z"
	actual := jobErr.Error()

	if actual != expected {
		t.Errorf("Expected %q, got %q", expected, actual)
	}
}

func TestNewErrorList(t *testing.T) {
	t.Parallel()

	limit := 5
	el := NewErrorList(limit)

	if el == nil {
		t.Fatal("NewErrorList returned nil")
	}

	if el.limit != limit {
		t.Errorf("Expected limit %d, got %d", limit, el.limit)
	}

	if len(el.errors) != 0 {
		t.Errorf("Expected empty errors slice, got length %d", len(el.errors))
	}

	if cap(el.errors) != limit {
		t.Errorf("Expected capacity %d, got %d", limit, cap(el.errors))
	}
}

func TestErrorList_AddError(t *testing.T) {
	t.Parallel()

	el := NewErrorList(3)
	timestamp := time.Now()

	err1 := JobError{
		Timestamp: timestamp,
		Err:       errors.New("error 1"),
		Message:   "message 1",
		Query:     "query 1",
		StmtType:  typedef.SelectStatementType,
	}

	err2 := JobError{
		Timestamp: timestamp,
		Err:       errors.New("error 2"),
		Message:   "message 2",
		Query:     "query 2",
		StmtType:  typedef.InsertStatementType,
	}

	el.AddError(err1)
	el.AddError(err2)

	errs := el.Errors()
	if len(errs) != 2 {
		t.Errorf("Expected 2 errors, got %d", len(errs))
	}

	if errs[0].Message != "message 1" {
		t.Errorf("Expected first error message 'message 1', got %q", errs[0].Message)
	}

	if errs[1].Message != "message 2" {
		t.Errorf("Expected second error message 'message 2', got %q", errs[1].Message)
	}
}

func TestErrorList_AddErrorExceedsLimit(t *testing.T) {
	t.Parallel()

	el := NewErrorList(2)
	timestamp := time.Now()

	// Add exactly limit number of errors
	for range 2 {
		err := JobError{
			Timestamp: timestamp,
			Err:       errors.New("error"),
			Message:   "message",
			Query:     "query",
			StmtType:  typedef.SelectStatementType,
		}
		el.AddError(err)
	}

	// Add one more error (should be added because condition is <=)
	err := JobError{
		Timestamp: timestamp,
		Err:       errors.New("error 3"),
		Message:   "message 3",
		Query:     "query 3",
		StmtType:  typedef.SelectStatementType,
	}
	el.AddError(err)

	errs := el.Errors()
	if len(errs) != 2 {
		t.Errorf("Expected 2 errors, got %d", len(errs))
	}

	// Add another error (should not be added because len > limit)
	err4 := JobError{
		Timestamp: timestamp,
		Err:       errors.New("error 4"),
		Message:   "message 4",
		Query:     "query 4",
		StmtType:  typedef.SelectStatementType,
	}
	el.AddError(err4)

	errs = el.Errors()
	if len(errs) != 2 {
		t.Errorf("Expected 2 errors after exceeding limit, got %d", len(errs))
	}
}

func TestErrorList_Errors(t *testing.T) {
	t.Parallel()

	el := NewErrorList(5)
	timestamp := time.Now()

	// Test empty list
	errs := el.Errors()
	if len(errs) != 0 {
		t.Errorf("Expected empty errors slice, got length %d", len(errs))
	}

	// Add some errors
	for i := 0; i < 3; i++ {
		err := JobError{
			Timestamp: timestamp,
			Err:       errors.New("error"),
			Message:   "message",
			Query:     "query",
			StmtType:  typedef.SelectStatementType,
		}
		el.AddError(err)
	}

	errs = el.Errors()
	if len(errs) != 3 {
		t.Errorf("Expected 3 errors, got %d", len(errs))
	}
}

func TestErrorList_Error(t *testing.T) {
	t.Parallel()

	el := NewErrorList(3)
	timestamp := time.Date(2023, 1, 1, 12, 0, 0, 0, time.UTC)

	// Test the empty error list
	errorStr := el.Error()
	if errorStr != "" {
		t.Errorf("Expected empty string for empty error list, got %q", errorStr)
	}

	// Add some errors
	err1 := JobError{
		Timestamp: timestamp,
		Err:       errors.New("error 1"),
		Message:   "message 1",
		Query:     "query 1",
		StmtType:  typedef.SelectStatementType,
	}

	err2 := JobError{
		Timestamp: timestamp,
		Err:       errors.New("error 2"),
		Message:   "message 2",
		Query:     "query 2",
		StmtType:  typedef.InsertStatementType,
	}

	el.AddError(err1)
	el.AddError(err2)

	errorStr = el.Error()

	val := `0JobError(err=error 1): message 1 (stmt-type=SelectStatement, query=query 1) time=2023-01-01T12:00:00Z
1JobError(err=error 2): message 2 (stmt-type=InsertStatement, query=query 2) time=2023-01-01T12:00:00Z
`
	// Check that the string contains the expected parts
	if errorStr != val {
		t.Errorf("Expected error string %q, got %q", val, errorStr)
	}
}

func TestErrorList_ConcurrentAccess(t *testing.T) {
	el := NewErrorList(100)
	timestamp := time.Now()

	var wg sync.WaitGroup
	numGoroutines := 10
	errorsPerGoroutine := 5

	// Add errors concurrently
	for range numGoroutines {
		wg.Add(2)
		go func() {
			defer wg.Done()
			for range errorsPerGoroutine {
				_ = el.Errors()
				_ = el.Error()
			}
		}()

		go func() {
			defer wg.Done()
			for range errorsPerGoroutine {
				err := JobError{
					Timestamp: timestamp,
					Err:       errors.New("concurrent error"),
					Message:   "concurrent message",
					Query:     "concurrent query",
					StmtType:  typedef.SelectStatementType,
				}
				el.AddError(err)
			}
		}()
	}

	wg.Wait()

	// Verify that some errors were added (exact number may vary due to concurrency and limit logic)
	errs := el.Errors()
	if len(errs) == 0 {
		t.Error("Expected some errors to be added concurrently")
	}
}
