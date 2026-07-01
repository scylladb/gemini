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

//go:build testing && !windows

package testutils

import (
	"os"
	"os/signal"
	"syscall"
)

// flockExclusive / flockUnlock take and release an exclusive advisory lock on
// the given file. They back the machine-wide ScyllaDB instance cap.
func flockExclusive(f *os.File) error { return syscall.Flock(int(f.Fd()), syscall.LOCK_EX) }
func flockUnlock(f *os.File) error    { return syscall.Flock(int(f.Fd()), syscall.LOCK_UN) }

// processAlive reports whether a process with the given PID currently exists.
// Signal 0 performs error checking without sending a signal: nil means the
// process exists; EPERM means it exists but is owned by someone else.
func processAlive(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}

// reRaiseSignal restores default handling for sig and re-raises it to this
// process, so an interrupted test run exits with normal signal semantics.
func reRaiseSignal(sig os.Signal) {
	if s, ok := sig.(syscall.Signal); ok {
		signal.Reset(s)
		_ = syscall.Kill(os.Getpid(), s)
	}
}
