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

package joberror_test

import (
	"encoding/json"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/scylladb/gemini/pkg/joberror"
)

func TestParallel(t *testing.T) {
	wg := sync.WaitGroup{}
	resultList := joberror.NewErrorList(1000)
	expectedList := joberror.NewErrorList(1000)
	baseDate := time.Date(2020, 02, 01, 0, 0, 0, 0, time.UTC)
	idx := atomic.Int32{}
	for y := 0; y < 1000; y++ {
		expectedList.AddError(&joberror.JobError{
			Timestamp: baseDate.AddDate(0, 0, y),
			Message:   "Some Message " + strconv.Itoa(y),
			Query:     "Some Query " + strconv.Itoa(y),
		})
	}
	expectedRawList := expectedList.Errors()

	for x := 0; x < 10; x++ {
		wg.Add(1)
		go func() {
			for y := 0; y < 100; y++ {
				resultList.AddError(expectedRawList[int(idx.Add(1)-1)])
			}
			wg.Done()
		}()
	}
	wg.Wait()

	sort.Slice(expectedRawList, func(i, j int) bool {
		return expectedRawList[i].Timestamp.After(expectedRawList[j].Timestamp)
	})

	resultRawList := resultList.Errors()
	sort.Slice(resultRawList, func(i, j int) bool {
		return resultRawList[i].Timestamp.After(resultRawList[j].Timestamp)
	})

	if diff := cmp.Diff(expectedRawList, resultRawList); diff != "" {
		t.Error(diff)
	}
}

func TestErrorSerialization(t *testing.T) {
	expected := []byte(`{"timestamp":"2020-02-01T00:00:00Z","message":"Some Message","query":"Some Query","stmt-type":"Some Type"}`)
	result, err := json.Marshal(joberror.JobError{
		Timestamp: time.Date(2020, 02, 01, 0, 0, 0, 0, time.UTC),
		Message:   "Some Message",
		Query:     "Some Query",
		StmtType:  "Some Type",
	})
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(expected, result); diff != "" {
		t.Error(diff)
	}
}

func TestErrorListSerialization(t *testing.T) {
	//nolint:lll
	expected := []byte(`[{"timestamp":"2020-02-01T00:00:00Z","message":"Some Message 0","query":"Some Query 0","stmt-type":"Some Stmt Type 0"},{"timestamp":"2020-02-02T00:00:00Z","message":"Some Message 1","query":"Some Query 1","stmt-type":"Some Stmt Type 1"},{"timestamp":"2020-02-03T00:00:00Z","message":"Some Message 2","query":"Some Query 2","stmt-type":"Some Stmt Type 2"},{"timestamp":"2020-02-04T00:00:00Z","message":"Some Message 3","query":"Some Query 3","stmt-type":"Some Stmt Type 3"},{"timestamp":"2020-02-05T00:00:00Z","message":"Some Message 4","query":"Some Query 4","stmt-type":"Some Stmt Type 4"},{"timestamp":"2020-02-06T00:00:00Z","message":"Some Message 5","query":"Some Query 5","stmt-type":"Some Stmt Type 5"},{"timestamp":"2020-02-07T00:00:00Z","message":"Some Message 6","query":"Some Query 6","stmt-type":"Some Stmt Type 6"},{"timestamp":"2020-02-08T00:00:00Z","message":"Some Message 7","query":"Some Query 7","stmt-type":"Some Stmt Type 7"},{"timestamp":"2020-02-09T00:00:00Z","message":"Some Message 8","query":"Some Query 8","stmt-type":"Some Stmt Type 8"},{"timestamp":"2020-02-10T00:00:00Z","message":"Some Message 9","query":"Some Query 9","stmt-type":"Some Stmt Type 9"}]`)
	lst := joberror.NewErrorList(1000)
	baseDate := time.Date(2020, 02, 01, 0, 0, 0, 0, time.UTC)
	for y := 0; y < 10; y++ {
		lst.AddError(&joberror.JobError{
			Timestamp: baseDate.AddDate(0, 0, y),
			StmtType:  "Some Stmt Type " + strconv.Itoa(y),
			Message:   "Some Message " + strconv.Itoa(y),
			Query:     "Some Query " + strconv.Itoa(y),
		})
	}

	result, err := json.Marshal(lst)
	if err != nil {
		t.Fatal(err)
	}

	if diff := cmp.Diff(expected, result); diff != "" {
		t.Error(diff)
	}
}
