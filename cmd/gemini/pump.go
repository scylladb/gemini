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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/briandowns/spinner"
	"github.com/scylladb/gemini"
	"go.uber.org/zap"
	"gopkg.in/tomb.v2"
)

type Pump struct {
	ch       chan heartBeat
	t        *tomb.Tomb
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

func (p *Pump) Start(d time.Duration, postFunc func()) {
	p.t.Go(func() error {
		defer p.cleanup(postFunc)
		timer := time.NewTimer(d)
		for {
			select {
			case <-p.t.Dying():
				p.logger.Info("Test run stopped. Exiting.")
				return nil
			case <-p.graceful:
				p.logger.Info("Test run aborted. Exiting.")
				p.t.Kill(nil)
				return nil
			case <-timer.C:
				p.logger.Info("Test run completed. Exiting.")
				p.t.Kill(nil)
				return nil
			case p.ch <- newHeartBeat():
			}
		}
	})
}

func (p *Pump) cleanup(postFunc func()) {
	close(p.ch)
	for range p.ch {
	}
	p.logger.Debug("pump channel drained")
	postFunc()
}

func createPump(t *tomb.Tomb, sz int, logger *zap.Logger) *Pump {
	logger = logger.Named("pump")
	var graceful = make(chan os.Signal, 1)
	signal.Notify(graceful, syscall.SIGTERM, syscall.SIGINT)
	pump := &Pump{
		ch:       make(chan heartBeat, sz),
		t:        t,
		graceful: graceful,
		logger:   logger,
	}
	return pump
}

func createPumpCallback(generators []*gemini.Generator, result chan Status, sp *spinner.Spinner) func() {
	return func() {
		if sp != nil {
			sp.Stop()
		}
		for _, g := range generators {
			g.Stop()
		}
	}
}
