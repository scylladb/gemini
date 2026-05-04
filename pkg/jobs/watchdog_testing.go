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

//go:build testing

package jobs

// watchdogExit is a no-op under the "testing" build tag. os.Exit(2)
// kills the entire Go test binary, swallowing all test output and
// producing only "exit status 2". Instead we let the goroutine dump
// and log message (already emitted before this call) provide the
// diagnostic info, and rely on Go's -timeout flag to terminate the
// test binary with a proper timeout message.
func watchdogExit() {}
