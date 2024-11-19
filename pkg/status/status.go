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

package status

import (
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/typedef"
)

type Uint64 struct {
	atomic.Uint64
}

func (u *Uint64) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.Load())
}

type GlobalStatus struct {
	Errors      *joberror.ErrorList `json:"errors,omitempty"`
	WriteOps    Uint64              `json:"write_ops"`
	WriteErrors Uint64              `json:"write_errors"`
	ReadOps     Uint64              `json:"read_ops"`
	ReadErrors  Uint64              `json:"read_errors"`
}

func (gs *GlobalStatus) AddWriteError(err *joberror.JobError) {
	gs.Errors.AddError(err)
	gs.WriteErrors.Add(1)
}

func (gs *GlobalStatus) AddReadError(err *joberror.JobError) {
	gs.Errors.AddError(err)
	gs.ReadErrors.Add(1)
}

func (gs *GlobalStatus) PrintResultAsJSON(w io.Writer, schema *typedef.Schema, version string, start time.Time) error {
	result := map[string]any{
		"result":         gs,
		"gemini_version": version,
		"schemaHash":     schema.GetHash(),
		"Time":           time.Since(start).String(),
	}
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent(" ", "    ")
	if err := encoder.Encode(result); err != nil {
		return errors.Wrap(err, "unable to create json from result")
	}
	return nil
}

func (gs *GlobalStatus) String() string {
	return fmt.Sprintf("write ops: %v | read ops: %v | write errors: %v | read errors: %v",
		gs.WriteOps.Load(), gs.ReadOps.Load(), gs.WriteErrors.Load(), gs.ReadErrors.Load())
}

func (gs *GlobalStatus) HasErrors() bool {
	return gs.WriteErrors.Load() > 0 || gs.ReadErrors.Load() > 0
}

func (gs *GlobalStatus) PrintResult(w io.Writer, schema *typedef.Schema, version string, start time.Time) {
	if err := gs.PrintResultAsJSON(w, schema, version, start); err != nil {
		// In case there has been it has been a long run we want to display it anyway...
		fmt.Printf("Unable to print result as json, using plain text to stdout, error=%s\n", err)
		fmt.Printf("Gemini version: %s\n", version)
		fmt.Printf("Results:\n")
		fmt.Printf("\ttime:    %v\n", time.Since(start).String())
		fmt.Printf("\twrite ops:    %v\n", gs.WriteOps.Load())
		fmt.Printf("\tread ops:     %v\n", gs.ReadOps.Load())
		fmt.Printf("\twrite errors: %v\n", gs.WriteErrors.Load())
		fmt.Printf("\tread errors:  %v\n", gs.ReadErrors.Load())
		for i, err := range gs.Errors.Errors() {
			fmt.Printf("Error %d: %s\n", i, err)
		}
	}
}

func NewGlobalStatus(limit int32) *GlobalStatus {
	return &GlobalStatus{
		Errors: joberror.NewErrorList(limit),
	}
}
