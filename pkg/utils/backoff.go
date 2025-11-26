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
//
//nolint:revive
package utils

import "time"

// ExponentialBackoffCapped returns an exponential backoff delay for the given attempt,
// starting at minDelay and doubling each retry, capped at maxDelay.
//
// attempt should start at 0 for the first retry delay.
// If minDelay <= 0, it defaults to 10ms. If maxDelay <= 0, it returns 0.
// If minDelay > maxDelay, maxDelay is returned.
func ExponentialBackoffCapped(attempt int, maxDelay, minDelay time.Duration) time.Duration {
	if maxDelay <= 0 {
		return 0
	}
	if minDelay <= 0 {
		minDelay = 50 * time.Millisecond
	}

	if attempt <= 0 {
		if minDelay > maxDelay {
			return maxDelay
		}
		return minDelay
	}

	delay := minDelay << uint(attempt) // minDelay * 2^attempt
	if delay > maxDelay {
		delay = maxDelay
	}
	return delay
}
