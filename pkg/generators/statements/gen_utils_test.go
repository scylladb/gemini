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

package statements

import (
	"fmt"
	"strings"
	"testing"

	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
	"github.com/scylladb/gemini/pkg/utils"
)

type resultToken struct {
	Token       string
	TokenValues string
}

func (r resultToken) Equal(received resultToken) bool {
	return r.Token == received.Token && r.TokenValues == received.TokenValues
}

type resultTokens []resultToken

func (r resultTokens) Equal(received resultTokens) bool {
	if len(r) != len(received) {
		return false
	}
	for id, expectedToken := range r {
		if !expectedToken.Equal(received[id]) {
			return false
		}
	}
	return true
}

func (r resultTokens) Diff(received resultTokens) string {
	var out []string
	maxIdx := len(r)
	if maxIdx < len(received) {
		maxIdx = len(received)
	}
	var expected, found *resultToken
	for idx := 0; idx < maxIdx; idx++ {
		if idx < len(r) {
			expected = &r[idx]
		} else {
			expected = &resultToken{}
		}

		if idx < len(received) {
			found = &received[idx]
		} else {
			found = &resultToken{}
		}

		out = testutils.AppendIfNotEmpty(out, testutils.GetErrorMsgIfDifferent(
			expected.TokenValues, found.TokenValues, " error: value stmt.ValuesWithToken.Token expected and received are different:"))
		out = testutils.AppendIfNotEmpty(out, testutils.GetErrorMsgIfDifferent(
			expected.TokenValues, found.TokenValues, " error: value stmt.ValuesWithToken.Value expected and received are different:"))
	}
	return strings.Join(out, "\n")
}

type result struct {
	Query       string
	Names       string
	Values      string
	Types       string
	QueryType   string
	TokenValues resultTokens
}

func (r *result) Equal(t *result) bool {
	var expected result
	if r != nil {
		expected = *r
	}

	var provided result
	if t != nil {
		provided = *t
	}
	return expected.Query == provided.Query &&
		expected.Names == provided.Names &&
		expected.Values == provided.Values &&
		expected.Types == provided.Types &&
		expected.QueryType == provided.QueryType &&
		expected.TokenValues.Equal(provided.TokenValues)
}

func (r *result) Diff(received *result) string {
	var out []string
	out = testutils.AppendIfNotEmpty(out, r.TokenValues.Diff(received.TokenValues))
	out = testutils.AppendIfNotEmpty(out, testutils.GetErrorMsgIfDifferent(
		r.Query, received.Query, " error: value stmt.Query.ToCql().stmt expected and received are different:"))
	out = testutils.AppendIfNotEmpty(out, testutils.GetErrorMsgIfDifferent(
		r.Names, received.Names, " error: value stmt.Query.ToCql().Names expected and received are different:"))
	out = testutils.AppendIfNotEmpty(out, testutils.GetErrorMsgIfDifferent(
		r.Values, received.Values, " error: value stmt.Values expected and received are different:"))
	out = testutils.AppendIfNotEmpty(out, testutils.GetErrorMsgIfDifferent(
		r.Types, received.Types, " error: value stmt.Types expected and received are different:"))
	out = testutils.AppendIfNotEmpty(out, testutils.GetErrorMsgIfDifferent(
		r.Values, received.Values, " error: value stmt.Values expected and received are different:"))
	out = testutils.AppendIfNotEmpty(out, testutils.GetErrorMsgIfDifferent(
		r.QueryType, received.QueryType, " error: value stmt.QueryType expected and received are different:"))
	return strings.Join(out, "\n")
}

type results []*result

func (r results) Equal(t results) bool {
	return r.Diff(t) == ""
}

func (r results) Diff(t results) string {
	var out []string
	maxIdx := len(r)
	if maxIdx < len(t) {
		maxIdx = len(t)
	}
	var expected, found *result
	for idx := 0; idx < maxIdx; idx++ {
		if idx < len(r) {
			expected = r[idx]
		} else {
			expected = &result{}
		}

		if idx < len(t) {
			found = t[idx]
		} else {
			found = &result{}
		}

		out = testutils.AppendIfNotEmpty(out, expected.Diff(found))
	}
	return strings.Join(out, "\n")
}

func convertStmtsToResults(stmt any) results {
	var out results
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

func convertStmtToResults(stmt *typedef.Stmt) *result {
	types := ""
	for idx := range stmt.Types {
		types = fmt.Sprintf("%s %s", types, stmt.Types[idx].Name())
	}
	query, names := stmt.Query.ToCql()
	var tokens []resultToken
	for _, valueToken := range stmt.ValuesWithToken {
		tokens = append(tokens, resultToken{
			Token:       fmt.Sprintf("%v", valueToken.Token),
			TokenValues: strings.TrimSpace(fmt.Sprintf("%v", valueToken.Value)),
		})
	}

	return &result{
		TokenValues: tokens,
		Query:       strings.TrimSpace(query),
		Names:       strings.TrimSpace(fmt.Sprintf("%s", names)),
		Values:      strings.TrimSpace(fmt.Sprintf("%v", stmt.Values)),
		Types:       types,
		QueryType:   fmt.Sprintf("%v", stmt.QueryType),
	}
}

func RunStmtTest[T testutils.ExpectedEntry[T]](
	t *testing.T,
	filePath string,
	cases []string,
	testBody func(subT *testing.T, caseName string, expected *testutils.ExpectedStore[T]),
) {
	t.Helper()
	utils.SetUnderTest()
	t.Parallel()
	expected := testutils.LoadExpectedFromFile[T](t, filePath, cases, *testutils.UpdateExpectedFlag)
	if *testutils.UpdateExpectedFlag {
		t.Cleanup(func() {
			expected.UpdateExpected(t)
		})
	}
	for idx := range cases {
		caseName := cases[idx]
		t.Run(caseName,
			func(subT *testing.T) {
				subT.Parallel()
				testBody(subT, caseName, expected)
			})
	}
}

func GetPkCountFromOptions(options testutils.TestCaseOptions, allValue int) int {
	pkCount := 0
	options.HandleOption("cpk", func(option string) {
		switch option {
		case "cpkAll":
			pkCount = allValue
		case "cpk1":
			pkCount = 1
		}
	})
	return pkCount
}

func GetCkCountFromOptions(options testutils.TestCaseOptions, allValue int) int {
	ckCount := -1
	options.HandleOption("cck", func(option string) {
		switch option {
		case "cckAll":
			ckCount = allValue
		case "cck1":
			ckCount = 0
		}
	})
	return ckCount
}

func validateStmt(t *testing.T, stmt any, err error) {
	t.Helper()
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
