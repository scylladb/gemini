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

//go:build testing

package services

import "github.com/scylladb/gemini/pkg/testutils"

// Tear down the shared, reusable ScyllaDB node pool after all tests run. The
// hook is invoked by TestMain (in service_test.go), which compiles in every
// build, whereas testutils.CloseScyllaPool only exists under the testing tag.
func init() {
	afterTests = testutils.CloseScyllaPool
}
