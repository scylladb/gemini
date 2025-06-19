// Copyright 2025 ScyllaDB
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

package utils

import "sync"

var (
	finalizers   []func()
	finalizersMu sync.Mutex
)

func AddFinalizer(f func()) {
	finalizersMu.Lock()
	defer finalizersMu.Unlock()

	finalizers = append(finalizers, sync.OnceFunc(f))
}

func ExecuteFinalizers() {
	finalizersMu.Lock()
	defer finalizersMu.Unlock()

	for _, f := range finalizers {
		f()
	}
}
