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

package joberror

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

// RowDiff represents a difference between test and oracle rows
type RowDiff struct {
	Diff      string          `json:"diff,omitempty"`
	TestRow   json.RawMessage `json:"testRow,omitempty"`
	OracleRow json.RawMessage `json:"oracleRow,omitempty"`
}

// ComparisonResults stores the results from both test and oracle for debugging
type ComparisonResults struct {
	TestRows       []json.RawMessage `json:"testRows,omitempty"`
	OracleRows     []json.RawMessage `json:"oracleRows,omitempty"`
	TestOnlyRows   []json.RawMessage `json:"testOnlyRows,omitempty"`
	OracleOnlyRows []json.RawMessage `json:"oracleOnlyRows,omitempty"`
	DifferentRows  []RowDiff         `json:"differentRows,omitempty"`
}

// PartitionValidation stores validation timing data for a specific partition.
type PartitionValidation struct {
	Recent         []uint64 `json:"recent,omitempty"`
	FirstSuccessNS uint64   `json:"firstSuccessNS,omitempty"`
	LastSuccessNS  uint64   `json:"lastSuccessNS,omitempty"`
	LastFailureNS  uint64   `json:"lastFailureNS,omitempty"`
}

type JobError struct {
	Timestamp       time.Time                      `json:"timestamp"`
	Err             error                          `json:"err,omitempty"`
	PartitionKeys   *typedef.Values                `json:"partition-keys"`
	Results         *ComparisonResults             `json:"results,omitempty"`
	LastValidations map[string]PartitionValidation `json:"lastValidations,omitempty"`
	Message         string                         `json:"message"`
	Query           string                         `json:"query"`
	Values          []any                          `json:"values,omitempty"`
	hash            [32]byte
	StmtType        typedef.StatementType `json:"stmt-type"`
}

func (j *JobError) Error() string {
	data, _ := json.Marshal(j.PartitionKeys)

	return fmt.Sprintf(
		"JobError(err=%v): %s (stmt-type=%s, query=%s, time=%s) partition-keys=%s",
		j.Err,
		j.Message,
		j.StmtType,
		j.Query,
		j.Timestamp.Format(time.RFC3339Nano),
		utils.UnsafeString(data),
	)
}

func (j *JobError) Hash() [32]byte {
	if j.hash != [32]byte{} {
		return j.hash
	}

	hasher := sha256.New()
	var bytes [32]byte

	hasher.Write(utils.UnsafeBytes(j.Query))
	hasher.Write([]byte{byte(j.StmtType)})

	if j.PartitionKeys != nil {
		keys := j.PartitionKeys.Keys()

		for _, key := range keys {
			hasher.Write(utils.UnsafeBytes(key))
			data, _ := json.Marshal(j.PartitionKeys.Get(key))
			hasher.Write(data)
		}
	}

	hasher.Sum(bytes[:0])

	j.hash = bytes
	return bytes
}

// HashHex returns the hex-encoded string form of the 32-byte hash.
// Useful for using the hash as a JSON map key or log identifier.
func (j *JobError) HashHex() string {
	sum := j.Hash()
	return hex.EncodeToString(sum[:])
}

type ErrorList struct {
	ch            chan *JobError
	errors        []JobError
	limit         int
	mu            sync.Mutex
	channelClosed atomic.Bool
}

func (el *ErrorList) AddError(err JobError) {
	el.mu.Lock()
	defer el.mu.Unlock()

	if len(el.errors) < el.limit {
		el.errors = append(el.errors, err)
		if el.ch != nil {
			el.ch <- &el.errors[len(el.errors)-1]
		}
	}
}

func (el *ErrorList) Errors() []JobError {
	el.mu.Lock()
	out := make([]JobError, len(el.errors))
	copy(out, el.errors)
	el.mu.Unlock()

	return out
}

func (el *ErrorList) MarshalJSON() ([]byte, error) {
	return json.Marshal(el.Errors())
}

func (el *ErrorList) Cap() int {
	return el.limit
}

func (el *ErrorList) Len() int {
	el.mu.Lock()
	defer el.mu.Unlock()

	return len(el.errors)
}

func (el *ErrorList) Error() string {
	var builder strings.Builder
	builder.Grow(1024)

	errors := el.Errors()

	for i, err := range errors {
		builder.WriteString(strconv.FormatInt(int64(i), 10))
		builder.WriteString(": ")
		builder.WriteString(err.Error())
		builder.WriteString("\n")
	}

	return strings.TrimRight(builder.String(), "\n")
}

func (el *ErrorList) GetChannel() <-chan *JobError {
	return el.ch
}

func (el *ErrorList) Close() error {
	el.mu.Lock()
	el.channelClosed.Store(true)
	ch := el.ch
	el.ch = nil
	el.mu.Unlock()

	if ch != nil {
		close(ch)
	}

	return nil
}

func NewErrorList(limit int) *ErrorList {
	return &ErrorList{
		limit:  limit,
		errors: make([]JobError, 0, limit),
		ch:     make(chan *JobError, limit+1),
	}
}
