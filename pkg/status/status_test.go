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

package status_test

import (
	"encoding/json"
	"strconv"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/scylladb/gemini/pkg/joberror"
	"github.com/scylladb/gemini/pkg/status"
)

func TestSerialization(t *testing.T) {
	t.Parallel()
	//nolint:lll
	expected := []byte(`{"errors":[{"timestamp":"2020-02-01T00:00:00Z","message":"Some Message 0","query":"Some Query 0"},{"timestamp":"2020-02-02T00:00:00Z","message":"Some Message 1","query":"Some Query 1"},{"timestamp":"2020-02-03T00:00:00Z","message":"Some Message 2","query":"Some Query 2"},{"timestamp":"2020-02-04T00:00:00Z","message":"Some Message 3","query":"Some Query 3"},{"timestamp":"2020-02-05T00:00:00Z","message":"Some Message 4","query":"Some Query 4"},{"timestamp":"2020-03-01T00:00:00Z","message":"Some Message 0","query":"Some Query 0"},{"timestamp":"2020-03-02T00:00:00Z","message":"Some Message 1","query":"Some Query 1"},{"timestamp":"2020-03-03T00:00:00Z","message":"Some Message 2","query":"Some Query 2"},{"timestamp":"2020-03-04T00:00:00Z","message":"Some Message 3","query":"Some Query 3"},{"timestamp":"2020-03-05T00:00:00Z","message":"Some Message 4","query":"Some Query 4"}],"write_ops":10,"write_errors":5,"read_ops":5,"read_errors":5}`)
	st := status.NewGlobalStatus(10)
	st.WriteOps.Store(10)
	st.ReadOps.Store(5)

	baseDate := time.Date(2020, 02, 01, 0, 0, 0, 0, time.UTC)
	for y := 0; y < 5; y++ {
		st.AddReadError(&joberror.JobError{
			Timestamp: baseDate.AddDate(0, 0, y),
			Message:   "Some Message " + strconv.Itoa(y),
			Query:     "Some Query " + strconv.Itoa(y),
		})
	}

	baseDate = time.Date(2020, 03, 01, 0, 0, 0, 0, time.UTC)
	for y := 0; y < 5; y++ {
		st.AddWriteError(&joberror.JobError{
			Timestamp: baseDate.AddDate(0, 0, y),
			Message:   "Some Message " + strconv.Itoa(y),
			Query:     "Some Query " + strconv.Itoa(y),
		})
	}

	result, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	if diff := cmp.Diff(expected, result); diff != "" {
		t.Error(diff)
	}
}
