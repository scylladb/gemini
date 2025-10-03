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
	Info(msg string, fields ...zap.Field)
	Warn(msg string, fields ...zap.Field)
	Error(msg string, fields ...zap.Field)
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
	s.log.Debug("closing signal channel", zap.String("flag", s.name))
	ch := s.ch.Swap(&closedChan)
	if ch != &closedChan {
		close(*ch)
		s.log.Debug("signal channel closed successfully", zap.String("flag", s.name))
	} else {
		s.log.Debug("signal channel was already closed", zap.String("flag", s.name))
	}
}

func (s *Flag) sendSignal(signal uint32, sendToParent bool) bool {
	s.log.Info("sending signal to flag",
		zap.String("flag", s.name),
		zap.String("signal", GetStateName(signal)),
		zap.Bool("send_to_parent", sendToParent),
	)

	s.closeChannel()
	out := s.val.CompareAndSwap(SignalNoop, signal)
	if !out {
		s.log.Debug("signal already set, ignoring new signal",
			zap.String("flag", s.name),
			zap.String("current_signal", GetStateName(s.val.Load())),
			zap.String("attempted_signal", GetStateName(signal)),
		)
		return false
	}

	s.log.Info("signal set successfully",
		zap.String("flag", s.name),
		zap.String("signal", GetStateName(signal)),
	)

	handlers := s.stopHandlers.Get()
	s.log.Debug("executing stop handlers",
		zap.String("flag", s.name),
		zap.Int("handler_count", len(handlers)),
	)
	for i, handler := range handlers {
		s.log.Debug("executing stop handler",
			zap.String("flag", s.name),
			zap.Int("handler_index", i),
		)
		handler(signal)
	}

	children := s.children.Get()
	if len(children) > 0 {
		s.log.Debug("propagating signal to children",
			zap.String("flag", s.name),
			zap.Int("child_count", len(children)),
		)
		for i, child := range children {
			s.log.Debug("sending signal to child",
				zap.String("parent_flag", s.name),
				zap.String("child_flag", child.name),
				zap.Int("child_index", i),
			)
			child.sendSignal(signal, sendToParent)
		}
	}

	if sendToParent && s.parent != nil {
		s.log.Debug("propagating signal to parent",
			zap.String("child_flag", s.name),
			zap.String("parent_flag", s.parent.name),
		)
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
	s.log.Info("creating child flag",
		zap.String("parent_flag", s.name),
		zap.String("child_name", name),
	)

	child := newFlag(name, s)
	s.children.Append(child)

	val := s.val.Load()
	s.log.Debug("child flag created, checking parent state",
		zap.String("parent_flag", s.name),
		zap.String("child_flag", child.name),
		zap.String("parent_state", GetStateName(val)),
	)

	switch val {
	case SignalSoftStop, SignalHardStop:
		s.log.Debug("propagating parent signal to new child",
			zap.String("parent_flag", s.name),
			zap.String("child_flag", child.name),
			zap.String("signal", GetStateName(val)),
		)
		child.sendSignal(val, false)
	default:
	}

	s.log.Info("child flag created successfully",
		zap.String("parent_flag", s.name),
		zap.String("child_flag", child.name),
	)

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
	s.log.Debug("adding stop handler",
		zap.String("flag", s.name),
	)

	s.stopHandlers.Append(handler)

	if s.IsHardOrSoft() {
		currentSignal := s.val.Load()
		s.log.Debug("flag already stopped, executing handler immediately",
			zap.String("flag", s.name),
			zap.String("current_signal", GetStateName(currentSignal)),
		)
		handler(currentSignal)
	}

	s.log.Info("stop handler added successfully",
		zap.String("flag", s.name),
	)
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

	out.log.Debug("flag created",
		zap.String("flag", name),
		zap.Bool("has_parent", parent != nil),
	)

	if parent != nil {
		out.log.Debug("flag created with parent",
			zap.String("flag", name),
			zap.String("parent", parent.name),
		)
	}

	return &out
}

func StartOsSignalsTransmitter(logger *zap.Logger, flags ...*Flag) {
	logger.Info("starting OS signals transmitter",
		zap.Int("flag_count", len(flags)),
	)

	graceful := make(chan os.Signal, 1)
	signal.Notify(graceful, syscall.SIGTERM, syscall.SIGINT)

	logger.Debug("OS signal handler registered for SIGTERM and SIGINT")

	go func() {
		logger.Debug("OS signal handler goroutine started, waiting for signals")
		sig := <-graceful

		logger.Info("received OS signal",
			zap.String("signal", sig.String()),
		)

		switch sig {
		case syscall.SIGINT, syscall.SIGTERM:
			logger.Info("processing graceful shutdown signal", zap.String("signal", sig.String()))
			for i := range flags {
				logger.Debug("setting soft stop for flag",
					zap.String("flag", flags[i].name),
					zap.Int("flag_index", i),
				)
				flags[i].SetSoft(true)
			}
			logger.Info("soft stop initiated for all flags")
		default:
			logger.Warn("processing unexpected signal as hard stop", zap.String("signal", sig.String()))
			for i := range flags {
				logger.Debug("setting hard stop for flag",
					zap.String("flag", flags[i].name),
					zap.Int("flag_index", i),
				)
				flags[i].SetHard(false)
			}
			logger.Info("hard stop initiated for all flags")
		}
	}()

	logger.Debug("OS signals transmitter started successfully")
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
