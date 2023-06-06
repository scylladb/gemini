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
	"reflect"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scylladb/gemini/pkg/stop"
)

func TestHardStop(t *testing.T) {
	testFlag, ctx, workersDone := initVars()
	workers := 30

	testSignals(t, workersDone, workers, testFlag.IsHard, testFlag.SetHard)
	if ctx.Err() == nil {
		t.Error("Error:SetHard function does not apply hardStopHandler")
	}
}

func TestSoftStop(t *testing.T) {
	testFlag, ctx, workersDone := initVars()
	workers := 30

	testSignals(t, workersDone, workers, testFlag.IsSoft, testFlag.SetSoft)
	if ctx.Err() != nil {
		t.Error("Error:SetSoft function apply hardStopHandler")
	}
}

func TestSoftOrHardStop(t *testing.T) {
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
	testFlagOut := stop.NewFlag()
	ctx, cancelFunc := context.WithCancel(context.Background())
	testFlagOut.SetOnHardStopHandler(cancelFunc)
	workersDone = &atomic.Uint32{}
	return &testFlagOut, ctx, workersDone
}

func testSignals(
	t *testing.T,
	workersDone *atomic.Uint32,
	workers int,
	checkFunc func() bool,
	setFunc func() bool,
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
	setFunc()

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
