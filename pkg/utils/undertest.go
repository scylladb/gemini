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

//go:build !testing

package utils

import (
	"testing"

	"github.com/gocql/gocql"
)

func IsUnderTest() bool {
	return false
}

func TestContainers(tb testing.TB) (*gocql.Session, *gocql.Session) {
	tb.Helper()
	tb.Fatal("TestContainers is not supported outside of testing build tag")
	return nil, nil
}
