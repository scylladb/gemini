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

package jobs

import (
	"time"

	"github.com/scylladb/gemini/pkg/stop"

	"go.uber.org/zap"
	"golang.org/x/exp/rand"
)

func NewPump(stopFlag *stop.Flag, logger *zap.Logger) chan time.Duration {
	pump := make(chan time.Duration, 10000)
	logger = logger.Named("Pump")
	go func() {
		logger.Debug("pump channel opened")
		defer func() {
			close(pump)
			logger.Debug("pump channel closed")
		}()
		for !stopFlag.IsHardOrSoft() {
			pump <- newHeartBeat()
		}
	}()

	return pump
}

func newHeartBeat() time.Duration {
	r := rand.Intn(10)
	switch r {
	case 0:
		return 10 * time.Millisecond
	default:
		return 0
	}
}
