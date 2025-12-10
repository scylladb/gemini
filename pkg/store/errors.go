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
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/scylladb/gemini/pkg/typedef"
)

// Type represents which store (test or oracle) an error occurred on
type Type string

const (
	TypeTest   Type = "test"
	TypeOracle Type = "oracle"
)

// AttemptError represents an error that occurred during a specific attempt
type AttemptError struct {
	Timestamp time.Time     `json:"timestamp"`
	Error     error         `json:"error"`
	Store     Type          `json:"store"`
	Attempt   int           `json:"attempt"`
	Duration  time.Duration `json:"duration,omitempty"`
}

// String returns a formatted string representation of the attempt error
func (ae AttemptError) String() string {
	durationStr := ""
	if ae.Duration > 0 {
		durationStr = fmt.Sprintf(" (took %v)", ae.Duration)
	}
	return fmt.Sprintf("attempt %d [%s%s]: %v", ae.Attempt, ae.Store, durationStr, ae.Error)
}

// ValidationError represents a comprehensive validation error with all attempts
type ValidationError struct {
	StartTime     time.Time      `json:"start_time"`
	EndTime       time.Time      `json:"end_time"`
	FinalError    error          `json:"final_error"`
	Statement     *typedef.Stmt  `json:"statement"`
	Table         *typedef.Table `json:"table"`
	Operation     string         `json:"operation"`
	Attempts      []AttemptError `json:"attempts"`
	TotalAttempts int            `json:"total_attempts"`
}

// Error implements the error interface
func (ve ValidationError) Error() string {
	var sb strings.Builder

	duration := ve.EndTime.Sub(ve.StartTime)
	sb.WriteString(fmt.Sprintf("%s failed after %d attempts (took %v)",
		ve.Operation, ve.TotalAttempts, duration))

	if ve.Table != nil {
		sb.WriteString(fmt.Sprintf(" on table %s", ve.Table.Name))
	}

	if ve.Statement != nil {
		sb.WriteString(fmt.Sprintf(" with query: %s", ve.Statement.Query))
		if ve.Statement.PartitionKeys.Values != nil && ve.Statement.PartitionKeys.Values.Len() > 0 {
			sb.WriteString(fmt.Sprintf(" (has %d partition keys)", ve.Statement.PartitionKeys.Values.Len()))
		}
	}

	// Only include the last attempt to keep logs concise and focused on the
	// final discrepancy the system cares about.
	if len(ve.Attempts) > 0 {
		last := ve.Attempts[len(ve.Attempts)-1]
		sb.WriteString("\n\nLast attempt:")
		sb.WriteString(fmt.Sprintf("\n  %s", last.String()))
	}

	if ve.FinalError != nil {
		sb.WriteString(fmt.Sprintf("\n\nFinal error: %v", ve.FinalError))
	}

	return sb.String()
}

// Unwrap implements error unwrapping for proper error chain support
func (ve ValidationError) Unwrap() error {
	return ve.FinalError
}

// Is implements error comparison for proper errors.Is() support
func (ve ValidationError) Is(target error) bool {
	// Check if any of the attempt errors matches the target
	for _, attempt := range ve.Attempts {
		if errors.Is(attempt.Error, target) {
			return true
		}
	}
	return false
}

// AddAttempt adds an attempt error to the validation error
func (ve *ValidationError) AddAttempt(attempt int, store Type, err error, duration time.Duration) {
	ve.Attempts = append(ve.Attempts, AttemptError{
		Attempt:   attempt,
		Timestamp: time.Now().UTC(),
		Store:     store,
		Error:     err,
		Duration:  duration,
	})
}

// GetAttemptsByStore returns all attempts for a specific store
func (ve ValidationError) GetAttemptsByStore(store Type) []AttemptError {
	var attempts []AttemptError
	for _, attempt := range ve.Attempts {
		if attempt.Store == store {
			attempts = append(attempts, attempt)
		}
	}
	return attempts
}

// GetLastAttempt returns the last attempt error, or nil if no attempts
func (ve ValidationError) GetLastAttempt() *AttemptError {
	if len(ve.Attempts) == 0 {
		return nil
	}
	return &ve.Attempts[len(ve.Attempts)-1]
}

// GetLastAttemptByStore returns the last attempt error for a specific store
func (ve ValidationError) GetLastAttemptByStore(store Type) *AttemptError {
	for i := len(ve.Attempts) - 1; i >= 0; i-- {
		if ve.Attempts[i].Store == store {
			return &ve.Attempts[i]
		}
	}
	return nil
}

// HasStore returns true if any attempts were made on the specified store
func (ve ValidationError) HasStore(store Type) bool {
	for _, attempt := range ve.Attempts {
		if attempt.Store == store {
			return true
		}
	}
	return false
}

// NewValidationError creates a new ValidationError
func NewValidationError(operation string, stmt *typedef.Stmt, table *typedef.Table) *ValidationError {
	return &ValidationError{
		Operation: operation,
		Statement: stmt,
		Table:     table,
		StartTime: time.Now().UTC(),
		Attempts:  make([]AttemptError, 0),
	}
}

// Finalize marks the validation error as complete
func (ve *ValidationError) Finalize(finalError error) {
	ve.EndTime = time.Now().UTC()
	ve.TotalAttempts = len(ve.Attempts)
	ve.FinalError = finalError
}

// StoreMutationError represents a comprehensive mutation error with all attempts
type MutationError struct {
	ValidationError         // Embed ValidationError for common functionality
	TestStoreSuccess   bool `json:"test_store_success"`
	OracleStoreSuccess bool `json:"oracle_store_success"`
}

// NewStoreMutationError creates a new StoreMutationError
func NewStoreMutationError(stmt *typedef.Stmt, table *typedef.Table) *MutationError {
	return &MutationError{
		ValidationError: ValidationError{
			Operation: "mutation",
			Statement: stmt,
			Table:     table,
			StartTime: time.Now().UTC(),
			Attempts:  make([]AttemptError, 0),
		},
	}
}

// SetStoreSuccess marks a store as successful
func (me *MutationError) SetStoreSuccess(store Type, success bool) {
	switch store {
	case TypeTest:
		me.TestStoreSuccess = success
	case TypeOracle:
		me.OracleStoreSuccess = success
	}
}

// Error implements the error interface with mutation-specific details
func (me MutationError) Error() string {
	base := me.ValidationError.Error()

	var sb strings.Builder
	sb.WriteString(base)
	sb.WriteString(fmt.Sprintf("\n\nStore status: test=%t, oracle=%t",
		me.TestStoreSuccess, me.OracleStoreSuccess))

	return sb.String()
}

// Unwrap implements error unwrapping for proper error chain support
func (me MutationError) Unwrap() error {
	return me.FinalError
}

// Is implements error comparison for proper errors.Is() support
func (me MutationError) Is(target error) bool {
	// Check if any of the attempt errors matches the target
	for _, attempt := range me.Attempts {
		if errors.Is(attempt.Error, target) {
			return true
		}
	}
	return false
}
