// Copyright 2026 ScyllaDB
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

package services

import (
	"runtime"
	"testing"
)

func TestWorkerCounts(t *testing.T) {
	t.Parallel()

	cpus := runtime.GOMAXPROCS(0)

	tests := map[string]struct {
		mutation, read, shards              int
		wantMutation, wantRead, wantInitial int
	}{
		"sized from shards":          {shards: 14, wantMutation: 224, wantRead: 224, wantInitial: 112},
		"configured maximum wins":    {mutation: 50, read: 50, shards: 14, wantMutation: 50, wantRead: 50, wantInitial: 50},
		"configured above the start": {mutation: 500, read: 500, shards: 14, wantMutation: 500, wantRead: 500, wantInitial: 112},
		"one kind configured":        {read: 10, shards: 4, wantMutation: 64, wantRead: 10, wantInitial: 32},
		"no shards uses the CPUs":    {wantMutation: 4 * cpus, wantRead: 4 * cpus, wantInitial: 4 * cpus},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			mutation, read, initial := workerCounts(tc.mutation, tc.read, tc.shards)
			if mutation != tc.wantMutation || read != tc.wantRead || initial != tc.wantInitial {
				t.Fatalf("workerCounts(%d, %d, %d) = (%d, %d, %d), want (%d, %d, %d)",
					tc.mutation, tc.read, tc.shards, mutation, read, initial,
					tc.wantMutation, tc.wantRead, tc.wantInitial)
			}
		})
	}
}
