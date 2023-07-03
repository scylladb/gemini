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

package testutils

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestSplitCaseName(t *testing.T) {
	t.Parallel()
	tcases := []struct {
		in          string
		caseName    string
		optionsList TestCaseOptions
	}{
		{
			in:          "pk1_ck0_col0_cpk1",
			caseName:    "pk1_ck0_col0_cpk1",
			optionsList: nil,
		},
		{
			in:          "pk1_ck0_col0_cpk1.opt1",
			caseName:    "pk1_ck0_col0_cpk1",
			optionsList: []string{"opt1"},
		},
		{
			in:          "pk1_ck0_col0_cpk1.opt1.opt2",
			caseName:    "pk1_ck0_col0_cpk1",
			optionsList: []string{"opt1", "opt2"},
		},
	}
	for idx := range tcases {
		testCase := tcases[idx]
		t.Run(testCase.in, func(t *testing.T) {
			t.Parallel()
			caseName, optionsList := SplitCaseName(testCase.in)
			if testCase.caseName != caseName {
				t.Errorf("unexpectec result %s", caseName)
			}

			if !cmp.Equal(testCase.optionsList[:], optionsList[:]) {
				t.Errorf("unexpectec result %s", optionsList)
			}
		})

	}
}
