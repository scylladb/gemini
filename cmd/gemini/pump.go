package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/briandowns/spinner"
	"go.uber.org/zap"
)

type Pump struct {
	ch       chan heartBeat
	done     chan struct{}
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
	go func() {
		defer p.cleanup(postFunc)
		timer := time.NewTimer(d)
		for {
			select {
			case <-p.done:
				p.logger.Info("Test run stopped. Exiting.")
				return
			case <-p.graceful:
				p.logger.Info("Test run aborted. Exiting.")
				return
			case <-timer.C:
				p.logger.Info("Test run completed. Exiting.")
				return
			case p.ch <- newHeartBeat():
			}
		}
	}()
}

func (p *Pump) Stop() {
	p.logger.Debug("pump asked to stop")
	p.done <- struct{}{}
}

func (p *Pump) cleanup(postFunc func()) {
	close(p.ch)
	for range p.ch {
	}
	p.logger.Debug("pump channel drained")
	postFunc()
}

func createPump(sz int, logger *zap.Logger) *Pump {
	logger = logger.Named("pump")
	var graceful = make(chan os.Signal, 1)
	signal.Notify(graceful, syscall.SIGTERM, syscall.SIGINT)
	pump := &Pump{
		ch:       make(chan heartBeat, sz),
		done:     make(chan struct{}, 1),
		graceful: graceful,
		logger:   logger,
	}
	return pump
}

func createPumpCallback(result chan Status, sp *spinner.Spinner) func() {
	return func() {
		if sp != nil {
			sp.Stop()
		}
	}
}
