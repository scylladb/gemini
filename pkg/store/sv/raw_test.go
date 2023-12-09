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

//nolint:thelper

package sv

import (
	"reflect"
	"testing"

	"github.com/scylladb/gemini/pkg/utils"
)

func TestColumnRaw_UnmarshalCQL(t *testing.T) {
	errorMsg := "wrong ColumnRaw.UnmarshalCQL work:"
	testColumn := make(ColumnRaw, 0)

	testsCount := 1000
	for i := 0; i < testsCount; i++ {
		expected := utils.RandBytes(rnd, rnd.Intn(1000))
		if i == 0 {
			expected = ColumnRaw{}
		}
		_ = testColumn.UnmarshalCQL(nil, expected)
		if !reflect.DeepEqual(expected, ([]byte)(testColumn)) {
			t.Fatalf("%s\nreceived:%+v \nexpected:%+v", errorMsg, testColumn, expected)
		}
		testColumn = make(ColumnRaw, 0)
	}
}
