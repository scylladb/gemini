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

import (
	"sync"
	"time"
)

// TimerPool provides a pool of reusable timers to avoid allocations
var TimerPool = sync.Pool{
	New: func() any {
		t := time.NewTimer(time.Hour) // Create with a long duration
		t.Stop()                      // Stop it immediately
		return t
	},
}

// GetTimer gets a timer from the pool and sets it to the specified duration
func GetTimer(d time.Duration) *time.Timer {
	timer := TimerPool.Get().(*time.Timer)
	timer.Reset(d)
	return timer
}

// PutTimer returns a timer to the pool after stopping it
func PutTimer(timer *time.Timer) {
	if !timer.Stop() {
		// Drain the channel if the timer has already fired
		select {
		case <-timer.C:
		default:
		}
	}
	TimerPool.Put(timer)
}
