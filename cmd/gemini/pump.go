package main

import (
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/briandowns/spinner"
	"go.uber.org/zap"
	"golang.org/x/net/context"
)

type Pump struct {
	ch     chan heartBeat
	ctx    context.Context
	cancel context.CancelFunc
}

func (p *Pump) Start(postFunc func()) {
	go func() {
		for {
			select {
			case <-p.ctx.Done():
				close(p.ch)
				for range p.ch {
				}
				postFunc()
				return
			default:
				p.ch <- newHeartBeat()
			}
		}
	}()
}

func (p *Pump) Stop() {
	p.cancel()
}

func createPump(sz int, d time.Duration, logger *zap.Logger) *Pump {
	logger = logger.Named("pump")
	// Gracefully terminate
	var gracefulStop = make(chan os.Signal)
	signal.Notify(gracefulStop, syscall.SIGTERM, syscall.SIGINT)

	// Create the actual pump
	pumpCh := make(chan heartBeat, sz)
	pumpCtx, pumpCancel := context.WithCancel(context.Background())
	pump := &Pump{
		ch:     pumpCh,
		ctx:    pumpCtx,
		cancel: pumpCancel,
	}
	go func(d time.Duration) {
		timer := time.NewTimer(d)
		for {
			select {
			case <-gracefulStop:
				pump.Stop()
				logger.Info("Test run aborted. Exiting.")
				return
			case <-timer.C:
				pump.Stop()
				logger.Info("Test run completed. Exiting.")
				return
			}
		}
	}(duration + warmup)
	return pump
}

func createPumpCallback(c chan Status, wg *sync.WaitGroup, sp *spinner.Spinner) func() {
	return func() {
		if sp != nil {
			sp.Stop()
		}
		wg.Wait()
		close(c)
	}
}
