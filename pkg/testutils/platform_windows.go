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

//go:build testing && windows

package testutils

import "os"

// Windows lacks flock and Kill(0), so the machine-wide ScyllaDB instance cap
// degrades to per-process here: flock is a no-op (parallel `go test ./...`
// processes do not coordinate through the registry file) and processAlive
// conservatively reports true (reapDeadOwners therefore never removes another
// process's entries). This keeps the testing build compiling and runnable on
// Windows; cross-process container capping stays a Unix-only optimization. A
// single test process still bounds its own pool via the per-process limit.
func flockExclusive(*os.File) error { return nil }
func flockUnlock(*os.File) error    { return nil }
func processAlive(int) bool         { return true }

// reRaiseSignal is a no-op on Windows; the caller's os.Exit(1) provides exit
// semantics. Windows has no Kill to re-raise a trapped signal with.
func reRaiseSignal(os.Signal) {}
