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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

type JobError struct {
	Timestamp     time.Time             `json:"timestamp"`
	Err           error                 `json:"err,omitempty"`
	PartitionKeys *typedef.Values       `json:"partition-keys"`
	Message       string                `json:"message"`
	Query         string                `json:"query"`
	StmtType      typedef.StatementType `json:"stmt-type"`
}

func (j JobError) Error() string {
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

type ErrorList struct {
	errors []JobError
	limit  int
	mu     sync.Mutex
}

func (el *ErrorList) AddError(err JobError) {
	el.mu.Lock()
	defer el.mu.Unlock()

	if len(el.errors) <= el.limit {
		el.errors = append(el.errors, err)
	}
}

func (el *ErrorList) Errors() []JobError {
	out := make([]JobError, el.limit)

	el.mu.Lock()
	n := copy(out, el.errors[:len(el.errors)])
	el.mu.Unlock()

	return out[:n]
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
		builder.WriteString(err.Error())
		builder.WriteString("\n")
	}

	return strings.TrimRight(builder.String(), "\n")
}

func NewErrorList(limit int) *ErrorList {
	return &ErrorList{
		limit:  limit,
		errors: make([]JobError, 0, limit),
	}
}
