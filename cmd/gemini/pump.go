package main

import (
	"context"
	"os"
	"os/signal"
	"sync"
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
			default:
				p.ch <- newHeartBeat()
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

func createPumpCallback(cancel context.CancelFunc, c chan Status, wg *sync.WaitGroup, sp *spinner.Spinner) func() {
	return func() {
		if sp != nil {
			sp.Stop()
		}
		cancel()
		wg.Wait()
		close(c)
	}
}
