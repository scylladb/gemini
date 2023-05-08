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
	"sync/atomic"
	"time"
	"unsafe"
)

type JobError struct {
	Timestamp time.Time `json:"timestamp"`
	Message   string    `json:"message"`
	Query     string    `json:"query"`
}

type ErrorList struct {
	errors []*JobError
	idx    atomic.Int32
	limit  int32
}

func (el *ErrorList) AddError(err *JobError) {
	idx := el.idx.Add(1)
	if idx <= el.limit {
		atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&el.errors[idx-1])), unsafe.Pointer(err))
	}
}

func (el *ErrorList) Errors() []*JobError {
	out := make([]*JobError, 0)
	for id := range el.errors {
		err := atomic.LoadPointer((*unsafe.Pointer)(unsafe.Pointer(&el.errors[id])))
		if err != nil {
			out = append(out, (*JobError)(err))
		}
	}
	return out
}

func (el *ErrorList) MarshalJSON() ([]byte, error) {
	return json.Marshal(el.Errors())
}

func NewErrorList(limit int32) *ErrorList {
	return &ErrorList{
		limit:  limit,
		errors: make([]*JobError, limit),
	}
}
