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
	"encoding/json/v2"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
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
	StartTime  time.Time     `json:"start_time"`
	EndTime    time.Time     `json:"end_time"`
	FinalError error         `json:"-"`
	Statement  *typedef.Stmt `json:"statement"`
	// TestErr and OracleErr hold the error of each cluster. A caller decides
	// from TestErr whether the system under test failed. An oracle failure
	// is a fault of the reference cluster, not of the system under test.
	TestErr           error              `json:"-"`
	OracleErr         error              `json:"-"`
	ComparisonResults *ComparisonResults `json:"comparison_results,omitempty"`
	Operation         string             `json:"operation"`
	TotalAttempts     atomic.Uint64      `json:"total_attempts"`
}

// ComparisonResults stores the comparison data from the last validation attempt
type ComparisonResults struct {
	TestRows       Rows            `json:"testRows,omitempty"`
	OracleRows     Rows            `json:"oracleRows,omitempty"`
	TestOnlyRows   Rows            `json:"testOnlyRows,omitempty"`
	OracleOnlyRows Rows            `json:"oracleOnlyRows,omitempty"`
	DifferentRows  []RowDifference `json:"differentRows,omitempty"`
}

// RowDiffData represents a difference between a test and oracle row
type RowDiffData struct {
	Diff      string `json:"diff,omitempty"`
	TestRow   Row    `json:"testRow,omitzero"`
	OracleRow Row    `json:"oracleRow,omitzero"`
}

// Error implements the error interface. It names the error of each cluster
// that FinalError does not already show.
func (ve *ValidationError) Error() string {
	var msg string
	switch {
	case ve.FinalError != nil:
		msg = ve.FinalError.Error()
	case ve.Operation != "":
		msg = ve.Operation + " failed"
	default:
		msg = "validation failed"
	}

	if ve.TestErr != nil && !errors.Is(ve.FinalError, ve.TestErr) {
		msg += "; test cluster: " + ve.TestErr.Error()
	}
	if ve.OracleErr != nil && !errors.Is(ve.FinalError, ve.OracleErr) {
		msg += "; oracle cluster: " + ve.OracleErr.Error()
	}

	return msg
}

// Diverged reports whether the clusters returned different rows.
func (ve *ValidationError) Diverged() bool {
	return ve.ComparisonResults != nil
}

// IsDivergence reports whether err holds a row comparison that found a
// difference between the clusters.
func IsDivergence(err error) bool {
	ve, ok := errors.AsType[*ValidationError](err)
	return ok && ve.Diverged()
}

// IsOracleLoadError reports whether err is a read failure of the oracle
// cluster alone. The test cluster returned its rows, so no comparison ran and
// the system under test did not fail.
func IsOracleLoadError(err error) bool {
	ve, ok := errors.AsType[*ValidationError](err)
	return ok && !ve.Diverged() && ve.OracleErr != nil && ve.TestErr == nil
}

// MarshalJSON strips verbose attempt and table data while keeping core context.
func (ve *ValidationError) MarshalJSON() ([]byte, error) {
	finalErr := ""
	if ve.FinalError != nil {
		finalErr = ve.FinalError.Error()
	}

	payload := struct {
		StartTime     time.Time     `json:"start_time"`
		EndTime       time.Time     `json:"end_time"`
		FinalError    string        `json:"final_error"`
		TestError     string        `json:"test_error,omitempty"`
		OracleError   string        `json:"oracle_error,omitempty"`
		Statement     *typedef.Stmt `json:"statement,omitempty"`
		Operation     string        `json:"operation,omitempty"`
		TotalAttempts uint64        `json:"total_attempts,omitzero"`
	}{
		StartTime:     ve.StartTime,
		EndTime:       ve.EndTime,
		FinalError:    finalErr,
		TestError:     errorString(ve.TestErr),
		OracleError:   errorString(ve.OracleErr),
		Statement:     ve.Statement,
		Operation:     ve.Operation,
		TotalAttempts: ve.TotalAttempts.Load(),
	}

	return json.Marshal(payload, json.Deterministic(true))
}

func errorString(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

// Unwrap implements error unwrapping for proper error chain support
func (ve *ValidationError) Unwrap() error {
	return ve.FinalError
}

// Is implements error comparison for proper errors.Is() support
func (ve *ValidationError) Is(target error) bool {
	return errors.Is(ve.FinalError, target)
}

// AddAttempt increments attempts; detailed per-attempt data is intentionally discarded to keep errors small.
func (ve *ValidationError) AddAttempt(_ error) {
	ve.TotalAttempts.Add(1)
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

// MutationError represents a comprehensive mutation error with all attempts
type MutationError struct {
	ValidationError

	TestStoreSuccess   bool `json:"test_store_success"`
	OracleStoreSuccess bool `json:"oracle_store_success"`

	// CompensationFailed reports that a compensating whole-partition DELETE was
	// attempted after a timeout and did NOT fully succeed on both clusters.
	//
	// This is a separate signal from the two success flags above, and the
	// difference is load-bearing. Those flags describe the ORIGINAL write; when
	// both clusters time out they are equal (both false) even though each server
	// may independently have committed or not. Compensation exists precisely to
	// collapse that ambiguity by deleting the partition on both sides. If it only
	// half-succeeds, the clusters can be left genuinely different — one erased,
	// one still holding a write that timed out but committed — while the original
	// flags still look symmetric.
	//
	// Callers must therefore treat this as an explicit requirement to invalidate
	// the affected partitions: without it, a partial compensation failure silently
	// manufactures a divergence that validation later reports as a product bug.
	CompensationFailed bool `json:"compensation_failed"`

	// Compensated reports that a compensating whole-partition DELETE made the
	// partition equal on both clusters. The error is still returned when the
	// test cluster failed the write with a definite error, so that failure is
	// charged to the system under test.
	Compensated bool `json:"compensated"`

	// OutcomeUnknown reports that a cluster may have applied a write gemini
	// could not confirm and could not compensate: a counter increment whose
	// reply did not arrive. The partition must be invalidated.
	OutcomeUnknown bool `json:"outcome_unknown"`
}

// MarshalJSON writes the fields of the embedded ValidationError and the flags
// that tell whether the write left the two clusters asymmetric. Without its own
// method the type would use the one of ValidationError and drop the flags.
func (me *MutationError) MarshalJSON() ([]byte, error) {
	finalErr := ""
	if me.FinalError != nil {
		finalErr = me.FinalError.Error()
	}

	payload := struct {
		StartTime          time.Time     `json:"start_time"`
		EndTime            time.Time     `json:"end_time"`
		FinalError         string        `json:"final_error"`
		TestError          string        `json:"test_error,omitempty"`
		OracleError        string        `json:"oracle_error,omitempty"`
		Statement          *typedef.Stmt `json:"statement,omitempty"`
		Operation          string        `json:"operation,omitempty"`
		TotalAttempts      uint64        `json:"total_attempts,omitzero"`
		TestStoreSuccess   bool          `json:"test_store_success"`
		OracleStoreSuccess bool          `json:"oracle_store_success"`
		CompensationFailed bool          `json:"compensation_failed"`
		Compensated        bool          `json:"compensated"`
		OutcomeUnknown     bool          `json:"outcome_unknown"`
	}{
		StartTime:          me.StartTime,
		EndTime:            me.EndTime,
		FinalError:         finalErr,
		TestError:          errorString(me.TestErr),
		OracleError:        errorString(me.OracleErr),
		Statement:          me.Statement,
		Operation:          me.Operation,
		TotalAttempts:      me.TotalAttempts.Load(),
		TestStoreSuccess:   me.TestStoreSuccess,
		OracleStoreSuccess: me.OracleStoreSuccess,
		CompensationFailed: me.CompensationFailed,
		Compensated:        me.Compensated,
		OutcomeUnknown:     me.OutcomeUnknown,
	}

	return json.Marshal(payload, json.Deterministic(true))
}

// NewStoreMutationError creates a new StoreMutationError
func NewStoreMutationError(stmt *typedef.Stmt) *MutationError {
	return &MutationError{
		Operation: "mutation",
		Statement: stmt,
		StartTime: time.Now().UTC(),
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
func (me *MutationError) Error() string {
	base := me.ValidationError.Error()

	var sb strings.Builder
	sb.WriteString(base)
	_, _ = fmt.Fprintf(&sb, "\n\nStore status: test=%t, oracle=%t", me.TestStoreSuccess, me.OracleStoreSuccess)

	return sb.String()
}

// Unwrap implements error unwrapping for proper error chain support
func (me *MutationError) Unwrap() error {
	return me.FinalError
}

// Is implements error comparison for proper errors.Is() support
func (me *MutationError) Is(target error) bool {
	return errors.Is(me.FinalError, target)
}
