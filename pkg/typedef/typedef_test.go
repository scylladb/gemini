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

package typedef

import (
	"testing"

	"github.com/google/go-cmp/cmp"
)

func TestValues(t *testing.T) {
	tmp := make(Values, 0, 10)
	expected := Values{1, 2, 3, 4, 5, 6, 7}
	expected2 := Values{1, 2, 3, 4, 5, 6, 7, 8, 9}
	expected3 := Values{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	tmp = append(tmp, 1, 2, 3, 4, 5)
	tmp = tmp.CopyFrom(Values{6, 7})
	if !cmp.Equal(tmp, expected) {
		t.Error("%i != %i", tmp, expected)
	}
	tmp = tmp.CopyFrom(Values{8, 9})
	if !cmp.Equal(tmp, expected2) {
		t.Error("%i != %i", tmp, expected)
	}
	tmp = append(tmp, 10)
	if !cmp.Equal(tmp, expected3) {
		t.Error("%i != %i", tmp, expected)
	}
}
