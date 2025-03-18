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
)

type JobError struct {
	Timestamp time.Time `json:"timestamp"`
	Err       error     `json:"err"`
	Message   string    `json:"message"`
	Query     string    `json:"query"`
	StmtType  string    `json:"stmt-type"`
}

func (j *JobError) Error() string {
	return fmt.Sprintf(
		"JobError(err=%v): %s (stmt-type=%s, query=%s) time=%s",
		j.Message,
		j.Err,
		j.StmtType,
		j.Query,
		j.Timestamp.Format(time.RFC3339Nano),
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
	el.mu.Lock()
	l := len(el.errors)
	el.mu.Unlock()

	out := make([]JobError, l)

	el.mu.Lock()
	copy(out, el.errors[:l])
	el.mu.Unlock()

	return out
}

func (el *ErrorList) MarshalJSON() ([]byte, error) {
	return json.Marshal(el.Errors())
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

	return builder.String()
}

func NewErrorList(limit int) *ErrorList {
	return &ErrorList{
		limit:  limit,
		errors: make([]JobError, 0, limit),
	}
}
