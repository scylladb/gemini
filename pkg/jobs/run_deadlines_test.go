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

package jobs

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestRunDeadlinesLeaveTimeForRetries: when the soft stop and the end of the
// run context fall at the same instant, finishPendingRetries runs on a context
// that already ended, and every retry the run queued in its last seconds is
// dropped without a report.
func TestRunDeadlinesLeaveTimeForRetries(t *testing.T) {
	t.Parallel()

	softStop, contextEnd := runDeadlines(10 * time.Minute)

	assert.Equal(t, 10*time.Minute+time.Second, softStop)
	assert.Equal(t, softStop+retryDrainGrace, contextEnd, "the context must outlive the soft stop")
	assert.Less(t, retryDrainGrace, watchdogGracePeriod, "the drain must end before the watchdog fires")
}
