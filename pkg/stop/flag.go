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

package stop

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"

	"go.uber.org/zap"
)

const (
	SignalNoop uint32 = iota
	SignalSoftStop
	SignalHardStop
)

type SignalChannel chan uint32

var closedChan = createClosedChan()

func createClosedChan() SignalChannel {
	ch := make(SignalChannel)
	close(ch)
	return ch
}

type SyncList[T any] struct {
	children     []T
	childrenLock sync.RWMutex
}

func (f *SyncList[T]) Append(el T) {
	f.childrenLock.Lock()
	defer f.childrenLock.Unlock()
	f.children = append(f.children, el)
}

func (f *SyncList[T]) Get() []T {
	f.childrenLock.RLock()
	defer f.childrenLock.RUnlock()
	return f.children
}

type logger interface {
	Debug(msg string, fields ...zap.Field)
}

type Flag struct {
	name         string
	log          logger
	ch           atomic.Pointer[SignalChannel]
	parent       *Flag
	children     SyncList[*Flag]
	stopHandlers SyncList[func(signal uint32)]
	val          atomic.Uint32
}

func (s *Flag) Name() string {
	return s.name
}

func (s *Flag) closeChannel() {
	ch := s.ch.Swap(&closedChan)
	if ch != &closedChan {
		close(*ch)
	}
}

func (s *Flag) sendSignal(signal uint32, sendToParent bool) bool {
	s.log.Debug(fmt.Sprintf("flag %s received signal %s", s.name, GetStateName(signal)))
	s.closeChannel()
	out := s.val.CompareAndSwap(SignalNoop, signal)
	if !out {
		return false
	}

	for _, handler := range s.stopHandlers.Get() {
		handler(signal)
	}

	for _, child := range s.children.Get() {
		child.sendSignal(signal, sendToParent)
	}
	if sendToParent && s.parent != nil {
		s.parent.sendSignal(signal, sendToParent)
	}
	return out
}

func (s *Flag) SetHard(sendToParent bool) bool {
	return s.sendSignal(SignalHardStop, sendToParent)
}

func (s *Flag) SetSoft(sendToParent bool) bool {
	return s.sendSignal(SignalSoftStop, sendToParent)
}

func (s *Flag) CreateChild(name string) *Flag {
	child := newFlag(name, s)
	s.children.Append(child)
	val := s.val.Load()
	switch val {
	case SignalSoftStop, SignalHardStop:
		child.sendSignal(val, false)
	}
	return child
}

func (s *Flag) SignalChannel() SignalChannel {
	return *s.ch.Load()
}

func (s *Flag) IsSoft() bool {
	return s.val.Load() == SignalSoftStop
}

func (s *Flag) IsHard() bool {
	return s.val.Load() == SignalHardStop
}

func (s *Flag) IsHardOrSoft() bool {
	return s.val.Load() != SignalNoop
}

func (s *Flag) Load() int32 {
	return int32(s.val.Load())
}

func (s *Flag) AddHandler(handler func(signal uint32)) {
	s.stopHandlers.Append(handler)
	if s.IsHardOrSoft() {
		handler(s.val.Load())
	}
}

func (s *Flag) AddHandler2(handler func(), expectedSignal uint32) {
	s.AddHandler(func(signal uint32) {
		switch expectedSignal {
		case SignalNoop:
			handler()
		default:
			if signal == expectedSignal {
				handler()
			}
		}
	})
}

func (s *Flag) CancelContextOnSignal(ctx context.Context, expectedSignal uint32) context.Context {
	ctx, cancel := context.WithCancel(ctx)
	s.AddHandler2(cancel, expectedSignal)
	return ctx
}

func (s *Flag) SetLogger(log logger) {
	s.log = log
}

func NewFlag(name string) *Flag {
	return newFlag(name, nil)
}

func newFlag(name string, parent *Flag) *Flag {
	out := Flag{
		name:   name,
		parent: parent,
		log:    zap.NewNop(),
	}
	ch := make(SignalChannel)
	out.ch.Store(&ch)
	return &out
}

func StartOsSignalsTransmitter(logger *zap.Logger, flags ...*Flag) {
	graceful := make(chan os.Signal, 1)
	signal.Notify(graceful, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-graceful
		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			for i := range flags {
				flags[i].SetSoft(true)
			}
			logger.Info("Get SIGINT signal, begin soft stop.")
		default:
			for i := range flags {
				flags[i].SetHard(true)
			}
			logger.Info("Get SIGTERM signal, begin hard stop.")
		}
	}()
}

func GetStateName(state uint32) string {
	switch state {
	case SignalSoftStop:
		return "soft"
	case SignalHardStop:
		return "hard"
	case SignalNoop:
		return "no-signal"
	default:
		panic(fmt.Sprintf("unexpected signal %d", state))
	}
}
