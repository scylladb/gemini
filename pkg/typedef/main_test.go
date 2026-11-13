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

package typedef_test

import (
	"math/rand/v2"
	"os"
	"testing"

	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/utils"
)

func TestMain(m *testing.M) {
	// Fill the random string buffer before any test runs. RandString reads it
	// without synchronization, so filling it from a parallel test races.
	utils.PreallocateRandomString(rand.New(rand.NewPCG(7, 7)), 1<<16)

	code := m.Run()
	// Tear down the shared, reusable ScyllaDB node pool so its containers do not
	// keep running after the test binary exits.
	testutils.CloseScyllaPool()
	os.Exit(code)
}
