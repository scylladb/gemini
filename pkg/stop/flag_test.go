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

package stop_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gemini/pkg/stop"
)

func TestHardStop(t *testing.T) {
	t.Parallel()
	testFlag, ctx, workersDone := initVars()
	workers := 30

	testSignals(t, workersDone, workers, testFlag.IsHard, testFlag.SetHard)
	if ctx.Err() == nil {
		t.Error("Error:SetHard function does not apply hardStopHandler")
	}
}

func TestSoftStop(t *testing.T) {
	t.Parallel()
	testFlag, ctx, workersDone := initVars()
	workers := 30

	testSignals(t, workersDone, workers, testFlag.IsSoft, testFlag.SetSoft)
	if ctx.Err() != nil {
		t.Error("Error:SetSoft function apply hardStopHandler")
	}
}

func TestSoftOrHardStop(t *testing.T) {
	t.Parallel()
	testFlag, ctx, workersDone := initVars()
	workers := 30

	testSignals(t, workersDone, workers, testFlag.IsHardOrSoft, testFlag.SetSoft)
	if ctx.Err() != nil {
		t.Error("Error:SetSoft function apply hardStopHandler")
	}

	workersDone.Store(uint32(0))
	testSignals(t, workersDone, workers, testFlag.IsHardOrSoft, testFlag.SetHard)
	if ctx.Err() != nil {
		t.Error("Error:SetHard function apply hardStopHandler after SetSoft")
	}

	testFlag, ctx, workersDone = initVars()
	workersDone.Store(uint32(0))

	testSignals(t, workersDone, workers, testFlag.IsHardOrSoft, testFlag.SetHard)
	if ctx.Err() == nil {
		t.Error("Error:SetHard function does not apply hardStopHandler")
	}
}

func initVars() (testFlag *stop.Flag, ctx context.Context, workersDone *atomic.Uint32) {
	testFlagOut := stop.NewFlag("main_test")
	ctx = testFlagOut.CancelContextOnSignal(context.Background(), stop.SignalHardStop)
	workersDone = &atomic.Uint32{}
	return testFlagOut, ctx, workersDone
}

func testSignals(
	t *testing.T,
	workersDone *atomic.Uint32,
	workers int,
	checkFunc func() bool,
	setFunc func(propagation bool) bool,
) {
	t.Helper()
	for i := 0; i != workers; i++ {
		go func() {
			for {
				if checkFunc() {
					workersDone.Add(1)
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
		}()
	}
	time.Sleep(200 * time.Millisecond)
	setFunc(false)

	for i := 0; i != 10; i++ {
		time.Sleep(100 * time.Millisecond)
		if workersDone.Load() == uint32(workers) {
			break
		}
	}

	setFuncName := runtime.FuncForPC(reflect.ValueOf(setFunc).Pointer()).Name()
	setFuncName, _ = strings.CutSuffix(setFuncName, "-fm")
	_, setFuncName, _ = strings.Cut(setFuncName, ".(")
	setFuncName = strings.ReplaceAll(setFuncName, ").", ".")
	checkFuncName := runtime.FuncForPC(reflect.ValueOf(checkFunc).Pointer()).Name()
	checkFuncName, _ = strings.CutSuffix(checkFuncName, "-fm")
	_, checkFuncName, _ = strings.Cut(checkFuncName, ".(")
	checkFuncName = strings.ReplaceAll(checkFuncName, ").", ".")

	if workersDone.Load() != uint32(workers) {
		t.Errorf("Error:%s or %s functions works not correctly %[2]s=%v", setFuncName, checkFuncName, checkFunc())
	}
}

func TestSendToParent(t *testing.T) {
	t.Parallel()
	tcases := []tCase{
		{
			testName:      "parent-hard-true",
			parentSignal:  stop.SignalHardStop,
			child1Signal:  stop.SignalHardStop,
			child11Signal: stop.SignalHardStop,
			child12Signal: stop.SignalHardStop,
			child2Signal:  stop.SignalHardStop,
		},
		{
			testName:      "parent-hard-false",
			parentSignal:  stop.SignalHardStop,
			child1Signal:  stop.SignalHardStop,
			child11Signal: stop.SignalHardStop,
			child12Signal: stop.SignalHardStop,
			child2Signal:  stop.SignalHardStop,
		},
		{
			testName:      "parent-soft-true",
			parentSignal:  stop.SignalSoftStop,
			child1Signal:  stop.SignalSoftStop,
			child11Signal: stop.SignalSoftStop,
			child12Signal: stop.SignalSoftStop,
			child2Signal:  stop.SignalSoftStop,
		},
		{
			testName:      "parent-soft-false",
			parentSignal:  stop.SignalSoftStop,
			child1Signal:  stop.SignalSoftStop,
			child11Signal: stop.SignalSoftStop,
			child12Signal: stop.SignalSoftStop,
			child2Signal:  stop.SignalSoftStop,
		},
		{
			testName:      "child1-soft-true",
			parentSignal:  stop.SignalSoftStop,
			child1Signal:  stop.SignalSoftStop,
			child11Signal: stop.SignalSoftStop,
			child12Signal: stop.SignalSoftStop,
			child2Signal:  stop.SignalSoftStop,
		},
		{
			testName:      "child1-soft-false",
			parentSignal:  stop.SignalNoop,
			child1Signal:  stop.SignalSoftStop,
			child11Signal: stop.SignalSoftStop,
			child12Signal: stop.SignalSoftStop,
			child2Signal:  stop.SignalNoop,
		},
		{
			testName:      "child11-soft-true",
			parentSignal:  stop.SignalSoftStop,
			child1Signal:  stop.SignalSoftStop,
			child11Signal: stop.SignalSoftStop,
			child12Signal: stop.SignalSoftStop,
			child2Signal:  stop.SignalSoftStop,
		},
		{
			testName:      "child11-soft-false",
			parentSignal:  stop.SignalNoop,
			child1Signal:  stop.SignalNoop,
			child11Signal: stop.SignalSoftStop,
			child12Signal: stop.SignalNoop,
			child2Signal:  stop.SignalNoop,
		},
	}
	for id := range tcases {
		tcase := tcases[id]
		t.Run(tcase.testName, func(t *testing.T) {
			t.Parallel()
			if err := tcase.runTest(); err != nil {
				t.Error(err)
			}
		})
	}
}

// nolint: govet
type parentChildInfo struct {
	parent        *stop.Flag
	parentSignal  uint32
	child1        *stop.Flag
	child1Signal  uint32
	child11       *stop.Flag
	child11Signal uint32
	child12       *stop.Flag
	child12Signal uint32
	child2        *stop.Flag
	child2Signal  uint32
}

func (t *parentChildInfo) getFlag(flagName string) *stop.Flag {
	switch flagName {
	case "parent":
		return t.parent
	case "child1":
		return t.child1
	case "child2":
		return t.child2
	case "child11":
		return t.child11
	case "child12":
		return t.child12
	default:
		panic(fmt.Sprintf("no such flag %s", flagName))
	}
}

func (t *parentChildInfo) getFlagHandlerState(flagName string) uint32 {
	switch flagName {
	case "parent":
		return t.parentSignal
	case "child1":
		return t.child1Signal
	case "child2":
		return t.child2Signal
	case "child11":
		return t.child11Signal
	case "child12":
		return t.child12Signal
	default:
		panic(fmt.Sprintf("no such flag %s", flagName))
	}
}

func (t *parentChildInfo) checkFlagState(flag *stop.Flag, expectedState uint32) error {
	var err error
	flagName := flag.Name()
	state := t.getFlagHandlerState(flagName)
	if state != expectedState {
		err = errors.Join(err, fmt.Errorf("flag %s handler has state %s while it is expected to be %s", flagName, stop.GetStateName(state), stop.GetStateName(expectedState)))
	}
	flagState := getFlagState(flag)
	if stop.GetStateName(expectedState) != flagState {
		err = errors.Join(err, fmt.Errorf("flag %s has state %s while it is expected to be %s", flagName, flagState, stop.GetStateName(expectedState)))
	}
	return err
}

type tCase struct {
	testName      string
	parentSignal  uint32
	child1Signal  uint32
	child11Signal uint32
	child12Signal uint32
	child2Signal  uint32
}

func (t *tCase) runTest() error {
	chunk := strings.Split(t.testName, "-")
	if len(chunk) != 3 {
		panic(fmt.Sprintf("wrong test name %s", t.testName))
	}
	flagName := chunk[0]
	signalTypeName := chunk[1]
	sendToParentName := chunk[2]

	var sendToParent bool
	switch sendToParentName {
	case "true":
		sendToParent = true
	case "false":
		sendToParent = false
	default:
		panic(fmt.Sprintf("wrong test name %s", t.testName))
	}
	runt := newParentChildInfo()
	flag := runt.getFlag(flagName)
	switch signalTypeName {
	case "soft":
		flag.SetSoft(sendToParent)
	case "hard":
		flag.SetHard(sendToParent)
	default:
		panic(fmt.Sprintf("wrong test name %s", t.testName))
	}
	var err error
	err = errors.Join(err, runt.checkFlagState(runt.parent, t.parentSignal))
	err = errors.Join(err, runt.checkFlagState(runt.child1, t.child1Signal))
	err = errors.Join(err, runt.checkFlagState(runt.child2, t.child2Signal))
	err = errors.Join(err, runt.checkFlagState(runt.child11, t.child11Signal))
	err = errors.Join(err, runt.checkFlagState(runt.child12, t.child12Signal))
	return err
}

func newParentChildInfo() *parentChildInfo {
	parent := stop.NewFlag("parent")
	child1 := parent.CreateChild("child1")
	out := parentChildInfo{
		parent:  parent,
		child1:  child1,
		child11: child1.CreateChild("child11"),
		child12: child1.CreateChild("child12"),
		child2:  parent.CreateChild("child2"),
	}

	out.parent.AddHandler(func(signal uint32) {
		out.parentSignal = signal
	})
	out.child1.AddHandler(func(signal uint32) {
		out.child1Signal = signal
	})
	out.child11.AddHandler(func(signal uint32) {
		out.child11Signal = signal
	})
	out.child12.AddHandler(func(signal uint32) {
		out.child12Signal = signal
	})
	out.child2.AddHandler(func(signal uint32) {
		out.child2Signal = signal
	})
	return &out
}

func getFlagState(flag *stop.Flag) string {
	switch {
	case flag.IsSoft():
		return "soft"
	case flag.IsHard():
		return "hard"
	default:
		return "no-signal"
	}
}

func TestSignalChannel(t *testing.T) {
	t.Parallel()
	t.Run("single-no-signal", func(t *testing.T) {
		t.Parallel()
		flag := stop.NewFlag("parent")
		select {
		case <-flag.SignalChannel():
			t.Error("should not get the signal")
		case <-time.Tick(200 * time.Millisecond):
		}
	})

	t.Run("single-beforehand", func(t *testing.T) {
		t.Parallel()
		flag := stop.NewFlag("parent")
		flag.SetSoft(true)
		<-flag.SignalChannel()
	})

	t.Run("single-normal", func(t *testing.T) {
		t.Parallel()
		flag := stop.NewFlag("parent")
		go func() {
			time.Sleep(200 * time.Millisecond)
			flag.SetSoft(true)
		}()
		<-flag.SignalChannel()
	})

	t.Run("parent-beforehand", func(t *testing.T) {
		t.Parallel()
		parent := stop.NewFlag("parent")
		child := parent.CreateChild("child")
		parent.SetSoft(true)
		<-child.SignalChannel()
	})

	t.Run("parent-beforehand", func(t *testing.T) {
		t.Parallel()
		parent := stop.NewFlag("parent")
		parent.SetSoft(true)
		child := parent.CreateChild("child")
		<-child.SignalChannel()
	})

	t.Run("parent-normal", func(t *testing.T) {
		t.Parallel()
		parent := stop.NewFlag("parent")
		child := parent.CreateChild("child")
		go func() {
			time.Sleep(200 * time.Millisecond)
			parent.SetSoft(true)
		}()
		<-child.SignalChannel()
	})

	t.Run("child-beforehand", func(t *testing.T) {
		t.Parallel()
		parent := stop.NewFlag("parent")
		child := parent.CreateChild("child")
		child.SetSoft(true)
		<-parent.SignalChannel()
	})

	t.Run("child-normal", func(t *testing.T) {
		t.Parallel()
		parent := stop.NewFlag("parent")
		child := parent.CreateChild("child")
		go func() {
			time.Sleep(200 * time.Millisecond)
			child.SetSoft(true)
		}()
		<-parent.SignalChannel()
	})
}
