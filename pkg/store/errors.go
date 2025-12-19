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
	"encoding/json"
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

// ValidationError represents a comprehensive validation error with all attempts
type ValidationError struct {
	StartTime     time.Time     `json:"start_time"`
	EndTime       time.Time     `json:"end_time"`
	FinalError    error         `json:"-"`
	Statement     *typedef.Stmt `json:"statement"`
	Operation     string        `json:"operation"`
	TotalAttempts int           `json:"total_attempts"`
}

// Error implements the error interface
func (ve ValidationError) Error() string {
	if ve.FinalError != nil {
		return ve.FinalError.Error()
	}

	if ve.Operation != "" {
		return fmt.Sprintf("%s failed", ve.Operation)
	}

	return "validation failed"
}

// MarshalJSON strips verbose attempt and table data while keeping core context.
func (ve ValidationError) MarshalJSON() ([]byte, error) {
	finalErr := ""
	if ve.FinalError != nil {
		finalErr = ve.FinalError.Error()
	}

	payload := struct {
		StartTime     time.Time     `json:"start_time"`
		EndTime       time.Time     `json:"end_time"`
		FinalError    string        `json:"final_error"`
		Statement     *typedef.Stmt `json:"statement,omitempty"`
		Operation     string        `json:"operation,omitempty"`
		TotalAttempts int           `json:"total_attempts,omitempty"`
	}{
		StartTime:     ve.StartTime,
		EndTime:       ve.EndTime,
		FinalError:    finalErr,
		Statement:     ve.Statement,
		Operation:     ve.Operation,
		TotalAttempts: ve.TotalAttempts,
	}

	return json.Marshal(payload)
}

// Unwrap implements error unwrapping for proper error chain support
func (ve ValidationError) Unwrap() error {
	return ve.FinalError
}

// Is implements error comparison for proper errors.Is() support
func (ve ValidationError) Is(target error) bool {
	return errors.Is(ve.FinalError, target)
}

// AddAttempt adds an attempt error to the validation error
func (ve *ValidationError) AddAttempt(attempt int, store Type, err error, duration time.Duration) {
	ve.TotalAttempts++
}

// NewValidationError creates a new ValidationError
func NewValidationError(operation string, stmt *typedef.Stmt) *ValidationError {
	return &ValidationError{
		Operation: operation,
		Statement: stmt,
		StartTime: time.Now().UTC(),
	}
}

// Finalize marks the validation error as complete
func (ve *ValidationError) Finalize(finalError error) {
	ve.EndTime = time.Now().UTC()
	if finalError != nil {
		ve.FinalError = finalError
	}
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
			StartTime: time.Now().UTC(),
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
	return errors.Is(me.FinalError, target)
}
