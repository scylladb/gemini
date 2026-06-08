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

package testutils

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"syscall"
)

// The ScyllaDB node pool is per-process, but `go test ./...` runs each package's
// test binary as a SEPARATE process, in parallel. To stop those independent
// pools from collectively spawning far more ScyllaDB containers than the host
// can handle, the total number of live instances is bounded MACHINE-WIDE via a
// small registry file guarded by an advisory file lock (flock):
//
//   - The registry ($TMPDIR/gemini-scylla-pool/instances.json) records one
//     entry per reserved instance: a unique token, the owning test process PID,
//     and the container ID once known.
//   - All reads/writes happen under an exclusive flock on a sibling lock file.
//     flock is released automatically if the holder dies, so a crashed test
//     process never wedges the registry.
//   - Reserving an instance is atomic and capped: a process may only add entries
//     while the live count stays within the cap, so the sum across all processes
//     is bounded.
//   - On every locked access, entries owned by a no-longer-running PID are reaped
//     (their containers force-removed), reclaiming slots leaked by killed runs.
//
// Each test process releases its own entries (and terminates its containers) via
// CloseScyllaPool in TestMain; orphan reaping is the backstop for crashes.

const scyllaRegistryDir = "gemini-scylla-pool"

type registryEntry struct {
	Token       string `json:"token"`
	ContainerID string `json:"container_id"`
	PID         int    `json:"pid"`
}

var registryTokenSeq atomic.Uint64

func registryRoot() string      { return filepath.Join(os.TempDir(), scyllaRegistryDir) }
func registryLockPath() string  { return filepath.Join(registryRoot(), "lock") }
func registryStatePath() string { return filepath.Join(registryRoot(), "instances.json") }

func newRegistryToken() string {
	return strconv.Itoa(os.Getpid()) + "-" + strconv.FormatUint(registryTokenSeq.Add(1), 10)
}

// pidAlive reports whether a process with the given PID currently exists.
func pidAlive(pid int) bool {
	if pid <= 0 {
		return false
	}
	// Signal 0 performs error checking without sending a signal: nil means the
	// process exists; EPERM means it exists but is owned by someone else.
	err := syscall.Kill(pid, 0)
	return err == nil || err == syscall.EPERM
}

// withRegistry runs fn while holding an exclusive flock on the registry, passing
// it the current entries (after reaping those owned by dead processes) and
// persisting whatever fn returns. It is best-effort: if the lock or files cannot
// be accessed, fn is called with the in-memory view and persistence is skipped,
// so a broken registry never blocks tests outright.
func withRegistry(fn func([]registryEntry) []registryEntry) {
	_ = os.MkdirAll(registryRoot(), 0o755)

	lock, err := os.OpenFile(registryLockPath(), os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		fn(nil)
		return
	}
	defer func() { _ = lock.Close() }()

	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX); err != nil {
		fn(nil)
		return
	}
	defer func() { _ = syscall.Flock(int(lock.Fd()), syscall.LOCK_UN) }()

	entries := reapDeadOwners(readRegistry())
	writeRegistry(fn(entries))
}

func readRegistry() []registryEntry {
	data, err := os.ReadFile(registryStatePath())
	if err != nil || len(data) == 0 {
		return nil
	}
	var entries []registryEntry
	if json.Unmarshal(data, &entries) != nil {
		return nil
	}
	return entries
}

func writeRegistry(entries []registryEntry) {
	data, err := json.Marshal(entries)
	if err != nil {
		return
	}
	_ = os.WriteFile(registryStatePath(), data, 0o644)
}

// reapDeadOwners drops entries whose owning process is gone, force-removing any
// container they left behind so its machine-wide slot is reclaimed. MUST be
// called under the registry lock.
func reapDeadOwners(entries []registryEntry) []registryEntry {
	me := os.Getpid()
	kept := entries[:0]
	for _, e := range entries {
		if e.PID == me || pidAlive(e.PID) {
			kept = append(kept, e)
			continue
		}
		if e.ContainerID != "" {
			_ = exec.Command("docker", "rm", "-f", e.ContainerID).Run()
		}
	}
	return kept
}

// registryTryReserve atomically reserves n machine-wide instance slots if the
// live count allows it, returning n tokens, or nil if the limit would be
// exceeded.
func registryTryReserve(n, limit int) []string {
	if n <= 0 {
		return nil
	}
	var tokens []string
	withRegistry(func(entries []registryEntry) []registryEntry {
		if len(entries)+n > limit {
			return entries
		}
		me := os.Getpid()
		for range n {
			tok := newRegistryToken()
			entries = append(entries, registryEntry{Token: tok, PID: me})
			tokens = append(tokens, tok)
		}
		return entries
	})
	if len(tokens) == n {
		return tokens
	}
	return nil
}

// registryBindContainer records the container ID for a previously reserved token
// so orphan reaping can find and remove it.
func registryBindContainer(token, containerID string) {
	withRegistry(func(entries []registryEntry) []registryEntry {
		for i := range entries {
			if entries[i].Token == token {
				entries[i].ContainerID = containerID
			}
		}
		return entries
	})
}

// registryReleaseMine drops every registry entry owned by this process,
// force-removing any container still recorded for them. It is the catch-all run
// at process teardown: it cleans up not just fully-created nodes but also
// reservations whose container creation was interrupted (e.g. by a signal),
// which the per-node teardown would otherwise miss.
func registryReleaseMine() {
	me := os.Getpid()
	withRegistry(func(entries []registryEntry) []registryEntry {
		kept := entries[:0]
		for _, e := range entries {
			if e.PID != me {
				kept = append(kept, e)
				continue
			}
			if e.ContainerID != "" {
				_ = exec.Command("docker", "rm", "-f", e.ContainerID).Run()
			}
		}
		return kept
	})
}

// registryRelease drops the given tokens, freeing their machine-wide slots.
func registryRelease(tokens ...string) {
	if len(tokens) == 0 {
		return
	}
	drop := make(map[string]bool, len(tokens))
	for _, t := range tokens {
		drop[t] = true
	}
	withRegistry(func(entries []registryEntry) []registryEntry {
		kept := entries[:0]
		for _, e := range entries {
			if !drop[e.Token] {
				kept = append(kept, e)
			}
		}
		return kept
	})
}
