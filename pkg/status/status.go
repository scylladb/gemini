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

	"github.com/pkg/errors"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/typedef"
)

type Uint64 struct {
	atomic.Uint64
}

func (u *Uint64) MarshalJSON() ([]byte, error) {
	return json.Marshal(u.Load())
}

type GlobalStatus struct {
	Errors         *joberror.ErrorList `json:"errors,omitempty"`
	testStmtFile   string
	oracleStmtFile string
	WriteOps       Uint64 `json:"write_ops"`
	WriteErrors    Uint64 `json:"write_errors"`
	ReadOps        Uint64 `json:"read_ops"`
	ValidatedRows  Uint64 `json:"validated_rows,omitempty"`
	ReadErrors     Uint64 `json:"read_errors"`
}

func (gs *GlobalStatus) SetStatementFiles(testFile, oracleFile string) {
	gs.testStmtFile = testFile
	gs.oracleStmtFile = oracleFile
}

func (gs *GlobalStatus) AddWriteError(err joberror.JobError) {
	gs.WriteErrors.Add(1)
	metrics.ExecutionErrors.WithLabelValues("write").Add(1)
	gs.Errors.AddError(err)
}

func (gs *GlobalStatus) AddReadError(err joberror.JobError) {
	gs.ReadErrors.Add(1)
	metrics.ExecutionErrors.WithLabelValues("read").Inc()
	gs.Errors.AddError(err)
}

func (gs *GlobalStatus) PrintResultAsJSON(w io.Writer, schema *typedef.Schema, version string, info map[string]any) error {
	return gs.PrintResultAsJSONWithSummary(w, schema, version, info, nil)
}

func (gs *GlobalStatus) PrintResultAsJSONWithSummary(w io.Writer, schema *typedef.Schema, version string, info map[string]any, summary []joberror.CorruptionEntry) error {
	result := map[string]any{
		"result":           gs,
		"gemini_version":   version,
		"schemaHash":       schema.GetHash(),
		"schema":           schema,
		"statement_ratios": info,
	}
	if len(summary) > 0 {
		result["corruption_summary"] = summary
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

func (gs *GlobalStatus) HasReachedErrorCount() bool {
	return (gs.ReadErrors.Load() + gs.WriteErrors.Load()) >= uint64(gs.Errors.Cap())
}

func (gs *GlobalStatus) HasErrors() bool {
	return gs.WriteErrors.Load() > 0 || gs.ReadErrors.Load() > 0
}

//nolint:forbidigo
func (gs *GlobalStatus) PrintResult(
	w io.Writer,
	summaryWriter io.Writer,
	schema *typedef.Schema,
	version string,
	statementInfo map[string]any,
) {
	// Build the summary once — both JSON and text will use the same data.
	var summary []joberror.CorruptionEntry
	if gs.Errors.Len() > 0 {
		summary = joberror.ComputeSummary(gs.Errors.Errors(), gs.testStmtFile, gs.oracleStmtFile)
	}

	if err := gs.PrintResultAsJSONWithSummary(w, schema, version, statementInfo, summary); err != nil {
		fmt.Printf("Unable to print result as json, using plain text to stdout, error=%s\n", err)
		fmt.Printf("Gemini version: %s\n", version)
		fmt.Printf("Results:\n")
		fmt.Printf("\twrite ops:    %v\n", gs.WriteOps.Load())
		fmt.Printf("\tread ops:     %v\n", gs.ReadOps.Load())
		fmt.Printf("\tvalidated rows:     %v\n", gs.ValidatedRows.Load())
		fmt.Printf("\twrite errors: %v\n", gs.WriteErrors.Load())
		fmt.Printf("\tread errors:  %v\n", gs.ReadErrors.Load())
		for i, err := range gs.Errors.Errors() {
			fmt.Printf("Error %d: %v\n", i, err)
		}
		jsonSchema, _ := json.MarshalIndent(schema, "", "    ")
		fmt.Printf("Schema: %v\n", string(jsonSchema))
	}

	joberror.PrintCorruptionSummary(summaryWriter, summary)
}

func (gs *GlobalStatus) WriteOp() {
	gs.WriteOps.Add(1)
}

func (gs *GlobalStatus) ReadOp() {
	gs.ReadOps.Add(1)
}

func (gs *GlobalStatus) AddValidatedRows(rows int) {
	gs.ValidatedRows.Add(uint64(rows))
}

func NewGlobalStatus(limit int) *GlobalStatus {
	return &GlobalStatus{
		Errors: joberror.NewErrorList(limit),
	}
}
