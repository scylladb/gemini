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

package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.uber.org/zap"
)

type Pump struct {
	ctx      context.Context
	ch       chan heartBeat
	graceful chan os.Signal
	logger   *zap.Logger
}

type heartBeat struct {
	sleep time.Duration
}

func (hb heartBeat) await() {
	if hb.sleep > 0 {
		time.Sleep(hb.sleep)
	}
}

func (p *Pump) Start(ctx context.Context, done context.CancelFunc) error {
	defer p.cleanup()
	for {
		select {
		case <-ctx.Done():
			p.logger.Info("Test run stopped. Exiting.")
			return ctx.Err()
		case <-p.graceful:
			p.logger.Info("Test run aborted. Exiting.")
			done()
			return ctx.Err()
		case p.ch <- newHeartBeat():
		}
	}
}

func (p *Pump) cleanup() {
	close(p.ch)
	p.logger.Debug("pump channel closed")
}

func createPump(sz int, logger *zap.Logger) *Pump {
	logger = logger.Named("pump")
	var graceful = make(chan os.Signal, 1)
	signal.Notify(graceful, syscall.SIGTERM, syscall.SIGINT)
	pump := &Pump{
		ch:       make(chan heartBeat, sz),
		graceful: graceful,
		logger:   logger,
	}
	return pump
}
