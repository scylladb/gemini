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

package burst

import (
	"context"
	"math/rand/v2"
	"time"
)

const ChannelSize = 10000

func work(ctx context.Context, pump chan<- time.Duration, chance int, sleepDuration time.Duration) {
	defer close(pump)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			sleep := time.Duration(0)

			if rand.Int()%chance == 0 {
				sleep = sleepDuration
			}

			pump <- sleep
		}
	}
}

func New(ctx context.Context, chance int, sleepDuration time.Duration) chan time.Duration {
	pump := make(chan time.Duration, ChannelSize)
	go work(ctx, pump, chance, sleepDuration)
	return pump
}
