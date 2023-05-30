// Copyright 2019 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package generators

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"strings"
	"testing"

	"github.com/pkg/errors"

	"github.com/scylladb/gemini/pkg/typedef"
)

type expectedStore struct {
	list     ExpectedList
	filePath string
	update   bool
}

// Result description:
type Result struct {
	Token       string
	TokenValues string
	Query       string
	Names       string
	Values      string
	Types       string
	QueryType   string
}

type Results []*Result

func initExpected(t *testing.T, fileName string, cases []string, updateExpected bool) *expectedStore {
	filePath := path.Join(testDirPath, fileName)
	expected := make(ExpectedList)
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
	return &expectedStore{filePath: filePath, list: expected, update: updateExpected}
}

func (f *expectedStore) CompareOrStore(t *testing.T, caseName string, stmt interface{}) {
	received := convertStmtsToResults(stmt)

	if f.update {
		f.list[caseName] = received
		return
	}
	expected := f.list[caseName]
	if len(expected) != len(received) {
		t.Fatalf("error: len received = %d , len expected = %d are different", len(received), len(expected))
	}
	for idx, res := range expected {
		res.Diff(t, received[idx])
	}
}

func (f *expectedStore) updateExpected(t *testing.T) {
	if f.update {
		data, err := json.MarshalIndent(f.list, "", "  ")
		if err != nil {
			t.Fatalf("Marshal funcStmtTests error:%v", err)
		}
		err = os.WriteFile(f.filePath, data, 0644)
		if err != nil {
			t.Fatalf("write to file %s error:%v", f.filePath, err)
		}
	}
}

func validateStmt(t *testing.T, stmt interface{}, err error) {
	if err != nil {
		t.Fatalf("error: get an error on create test inputs:%v", err)
	}
	if stmt == nil {
		t.Fatalf("error: stmt is nil")
	}
	switch stmts := stmt.(type) {
	case *typedef.Stmts:
		if stmts == nil || stmts.List == nil || len(stmts.List) == 0 {
			t.Fatalf("error: stmts is empty")
		}
		for i := range stmts.List {
			if stmts.List[i] == nil || stmts.List[i].Query == nil {
				t.Fatalf("error: stmts has nil stmt #%d", i)
			}
		}
	case *typedef.Stmt:
		if stmts == nil || stmts.Query == nil {
			t.Fatalf("error: stmt is empty")
		}
	default:
		t.Fatalf("error: unkwon type of stmt")
	}
}

func getErrorMsgIfDifferent(t *testing.T, expected, received, errMsg string) {
	if expected == received {
		return
	}
	errMsgList := make([]string, 0)
	switch len(expected) == len(received) {
	case true:
		// Inject nice row that highlights differences if length is not changed
		errMsgList = []string{
			errMsg,
			fmt.Sprintf("Expected⇶⇶⇶%s", expected),
			"           " + diffHighlightString(expected, received),
			fmt.Sprintf("Received⇶⇶⇶%s", received),
			"-------------------------------------------",
		}
	case false:
		errMsgList = []string{
			errMsg,
			fmt.Sprintf("Expected⇶⇶⇶%s", expected),
			fmt.Sprintf("Received⇶⇶⇶%s", received),
			"-------------------------------------------",
		}
	}
	t.Error(strings.Join(errMsgList, "\n"))
}

func diffHighlightString(expected, received string) string {
	out := ""
	for idx := range expected {
		if expected[idx] == received[idx] {
			out = out + " "
		} else {
			out = out + "↕"
		}
	}
	return out
}

func convertStmtsToResults(stmt interface{}) Results {
	var out Results
	switch stmts := stmt.(type) {
	case *typedef.Stmts:
		for idx := range stmts.List {
			out = append(out, convertStmtToResults(stmts.List[idx]))
		}
	case *typedef.Stmt:
		out = append(out, convertStmtToResults(stmts))

	}
	return out
}

func convertStmtToResults(stmt *typedef.Stmt) *Result {
	types := ""
	for idx := range stmt.Types {
		types = fmt.Sprintf("%s %s", types, stmt.Types[idx].Name())
	}
	query, names := stmt.Query.ToCql()
	return &Result{
		Token:       fmt.Sprintf("%v", (*stmt).ValuesWithToken.Token),
		TokenValues: strings.TrimSpace(fmt.Sprintf("%v", (*stmt).ValuesWithToken.Value)),
		Query:       strings.TrimSpace(query),
		Names:       strings.TrimSpace(fmt.Sprintf("%s", names)),
		Values:      strings.TrimSpace(fmt.Sprintf("%v", stmt.Values)),
		Types:       types,
		QueryType:   fmt.Sprintf("%v", stmt.QueryType),
	}
}

func (r *Result) Diff(t *testing.T, received *Result) {
	getErrorMsgIfDifferent(t, r.Token, received.Token, " error: value stmt.ValuesWithToken.Token expected and received are different:")
	getErrorMsgIfDifferent(t, r.TokenValues, received.TokenValues, " error: value stmt.ValuesWithToken.Value expected and received are different:")
	getErrorMsgIfDifferent(t, r.Query, received.Query, " error: value stmt.Query.ToCql().stmt expected and received are different:")
	getErrorMsgIfDifferent(t, r.Names, received.Names, " error: value stmt.Query.ToCql().Names expected and received are different:")
	getErrorMsgIfDifferent(t, r.Values, received.Values, " error: value stmt.Values expected and received are different:")
	getErrorMsgIfDifferent(t, r.Types, received.Types, " error: value stmt.Types expected and received are different:")
	getErrorMsgIfDifferent(t, r.Values, received.Values, " error: value stmt.Values expected and received are different:")
	getErrorMsgIfDifferent(t, r.QueryType, received.QueryType, " error: value stmt.QueryType expected and received are different:")
}

type ExpectedList map[string]Results

func (e *ExpectedList) checkCasesExisting(cases []string) error {
	for idx := range cases {
		exp, ok := (*e)[cases[idx]]
		if !ok || (&exp) == (&Results{}) {
			return errors.Errorf("expected for case %s not found", cases[idx])
		}
	}
	return nil
}

func (e *ExpectedList) addCases(cases ...string) {
	for idx := range cases {
		(*e)[cases[idx]] = Results{}
	}
}

func (e *ExpectedList) loadExpectedFromFile(filePath string) error {
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
