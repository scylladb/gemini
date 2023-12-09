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

package ver

import (
	"sync"
	"testing"
)

func TestCheck(t *testing.T) {
	t.Parallel()
	parallels := 10
	// Test without version difference.
	runParallel(parallels, -1, false)
	if !Check.Done() {
		t.Fatalf("wrong ver.Check work, Check.Done() shoul return 'true'")
	}
	if !Check.ModeSV() {
		t.Fatalf("wrong ver.Check work, Check.ModeSV() shoul return 'true'")
	}

	// Test with version difference in the first response.
	Check.reInit()
	runParallel(parallels, 0, true)
	if !Check.Done() {
		t.Fatalf("wrong ver.Check work, Check.Done() shoul return 'true'")
	}
	if Check.ModeSV() {
		t.Fatalf("wrong ver.Check work, Check.ModeSV() shoul return 'false'")
	}

	// Test with version difference not in the first response.
	Check.reInit()
	runParallel(parallels, parallels/2, true)
	if !Check.Done() {
		t.Fatalf("wrong ver.Check work, Check.Done() shoul return 'true'")
	}
	if Check.ModeSV() {
		t.Fatalf("wrong ver.Check work, Check.ModeSV() shoul return 'false'")
	}
}

func runParallel(parallel, addTo int, addOld bool) {
	wg := sync.WaitGroup{}
	if addOld && addTo < 1 {
		Check.Add(true)
	}
	for i := 0; i < parallel; i++ {
		wg.Add(1)
		addOld = false
		if addTo == i {
			addOld = true
		}
		go func(add bool) {
			l := 0
			if add {
				Check.Add(true)
			}
			defer wg.Done()
			for {
				Check.Add(false)
				l++
				if Check.Done() {
					return
				}
			}
		}(addOld)
	}
	wg.Wait()
}
