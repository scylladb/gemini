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
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"go.uber.org/zap"
)

const (
	signalNoSignal uint32 = iota
	signalSoftStop
	signalHardStop
)

type Flag struct {
	hardStopHandler func()
	val             atomic.Uint32 // 1 - "soft stop";2 - "hard stop"
}

func (s *Flag) SetSoft() bool {
	return s.val.CompareAndSwap(signalNoSignal, signalSoftStop)
}

func (s *Flag) SetHard() bool {
	out := s.val.CompareAndSwap(signalNoSignal, signalHardStop)
	if out && s.hardStopHandler != nil {
		s.hardStopHandler()
	}
	return out
}

func (s *Flag) IsSoft() bool {
	return s.val.Load() == signalSoftStop
}

func (s *Flag) IsHard() bool {
	return s.val.Load() == signalHardStop
}

func (s *Flag) IsHardOrSoft() bool {
	return s.val.Load() != signalNoSignal
}

func (s *Flag) SetOnHardStopHandler(function func()) {
	s.hardStopHandler = function
}

func NewFlag() Flag {
	return Flag{}
}

func StartOsSignalsTransmitter(logger *zap.Logger, flags ...*Flag) {
	graceful := make(chan os.Signal, 1)
	signal.Notify(graceful, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-graceful
		switch sig {
		case syscall.SIGINT:
			for i := range flags {
				flags[i].SetSoft()
			}
			logger.Info("Get SIGINT signal, begin soft stop.")
		default:
			for i := range flags {
				flags[i].SetHard()
			}
			logger.Info("Get SIGTERM signal, begin hard stop.")
		}
	}()
}
