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

package testutils

import (
	"encoding/json"
	"os"
	"sync"
	"testing"

	"github.com/pkg/errors"
)

type ExpectedEntry[T any] interface {
	Equal(T) bool
	Diff(T) string
}

type expectedList[T ExpectedEntry[T]] map[string]T

type ExpectedStore[T ExpectedEntry[T]] struct {
	list     expectedList[T]
	filePath string
	update   bool
	listLock sync.RWMutex
}

func (e *expectedList[T]) checkCasesExisting(cases []string) error {
	empty := new(T)
	for _, caseName := range cases {
		exp, ok := (*e)[caseName]
		if !ok || exp.Equal(*empty) {
			return errors.Errorf("expected for case %s not found", caseName)
		}
	}
	return nil
}

func (e *expectedList[T]) addCases(cases ...string) {
	for _, caseName := range cases {
		(*e)[caseName] = *new(T)
	}
}

func (e *expectedList[T]) loadExpectedFromFile(filePath string) error {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return errors.Wrapf(err, "failed to open file %s", filePath)
	}
	err = json.Unmarshal(data, e)
	if err != nil {
		return errors.Wrapf(err, "failed to unmarshal expected from file %s", filePath)
	}
	return nil
}

func (f *ExpectedStore[T]) CompareOrStore(t *testing.T, caseName string, received T) {
	t.Helper()

	if f.update {
		f.listLock.Lock()
		f.list[caseName] = received
		f.listLock.Unlock()
		return
	}
	f.listLock.RLock()
	expected := f.list[caseName]

	if diff := expected.Diff(received); diff != "" {
		t.Error(diff)
	}
	f.listLock.RUnlock()
}

func (f *ExpectedStore[T]) UpdateExpected(t *testing.T) {
	t.Helper()
	if f.update {
		f.listLock.RLock()
		data, err := json.MarshalIndent(f.list, "", "  ")
		f.listLock.RUnlock()
		if err != nil {
			t.Fatalf("Marshal funcStmtTests error:%v", err)
		}
		err = os.WriteFile(f.filePath, data, 0o644)
		if err != nil {
			t.Fatalf("write to file %s error:%v", f.filePath, err)
		}
	}
}

func LoadExpectedFromFile[T ExpectedEntry[T]](
	t *testing.T,
	filePath string,
	cases []string,
	updateExpected bool,
) *ExpectedStore[T] {
	t.Helper()
	expected := make(expectedList[T])
	if updateExpected {
		expected.addCases(cases...)
	} else {
		err := expected.loadExpectedFromFile(filePath)
		if err != nil {
			t.Fatal(err.Error())
		}
		err = expected.checkCasesExisting(cases)
		if err != nil {
			t.Fatal(err.Error())
		}
	}
	return &ExpectedStore[T]{filePath: filePath, list: expected, update: updateExpected}
}
