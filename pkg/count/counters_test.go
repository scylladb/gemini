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

package count_test

import (
	"sync"
	"testing"
	"time"

	"golang.org/x/exp/rand"

	"github.com/scylladb/gemini/pkg/count"
)

func TestSimpleCounters(t *testing.T) {
	t.Parallel()
	countersInfo := []count.Info{{
		Name:                  "test simple counter 1",
		Unit:                  "ms",
		PrometheusIntegration: false,
		Description:           "test counts",
	}, {
		Name:                  "test simple counter 123123",
		Unit:                  "ssm",
		PrometheusIntegration: true,
		Description:           "test counts",
	}, {
		Name:                  "test simple counter 5656565",
		Unit:                  "s",
		PrometheusIntegration: true,
		Description:           "test counts",
	}}
	group := count.InitGroup("test group", "testing", true)
	sCounters := group.AddSimpleCounters(countersInfo)
	workers := 10
	adds := 100000
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			scOperations(sCounters, adds)
			wg.Done()
		}()
	}
	wg.Wait()
	sum := getSimpleCounterSum(sCounters)
	if sum != workers*adds {
		t.Errorf("wrong simple counters work. expected sum:%d, received sum:%d", workers*adds, sum)
	}
	count.PrintAllGroups()
}

func TestTotalCounters(t *testing.T) {
	t.Parallel()
	countersNames := []string{
		"sub counter1",
		"sub counter200",
		"sub counter3",
		"sub counter4",
		"sub counter5000",
		"sub counter6",
		"sub counter7",
		"sub counter800000",
		"sub counter9",
		"sub counter10",
	}
	group := count.InitGroup("test group", "testing", true)
	group2 := group.AddGroup("test group222", "testing222", true)
	tCounter1 := group.AddTotalCounter(count.Info{Name: "total counter 1", Unit: "qty", PrometheusIntegration: false, Description: "count qty"}, "count", countersNames)
	tCounter2 := group2.AddTotalCounter(count.Info{Name: "total counter 2", Unit: "ps", PrometheusIntegration: true, Description: "count ps"}, "count", countersNames)
	tCounters := count.TotalCounters{tCounter1, tCounter2}
	workers := 10
	adds := 100000
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			totalCounterOperations(tCounters, adds)
			wg.Done()
		}()
	}
	wg.Wait()
	tSum, sum := getTotalCounterSum(tCounters)
	if sum != workers*adds {
		t.Errorf("wrong simple counters work. expected sum:%d, received sum:%d", workers*adds, sum)
	}
	if tSum != workers*adds {
		t.Errorf("wrong simple counters work. expected sum:%d, received sum:%d", workers*adds, sum)
	}
	count.PrintAllGroups()
}

func scOperations(counters count.SimpleCounters, adds int) {
	cl := len(counters)
	rnd := rand.New(rand.NewSource(uint64(time.Now().Unix())))
	for c := 0; c < adds; c++ {
		counters[rnd.Intn(cl)].Inc()
		counters[rnd.Intn(cl)].Get()
	}
}

func getSimpleCounterSum(counters count.SimpleCounters) int {
	sum := 0
	for idx := range counters {
		sum += int(counters[idx].Get())
	}
	return sum
}

func totalCounterOperations(counters count.TotalCounters, adds int) {
	cl := len(counters)
	rnd := rand.New(rand.NewSource(uint64(time.Now().Unix())))
	for c := 0; c < adds; c++ {
		n := rnd.Intn(10)
		counters[rnd.Intn(cl)].Inc(n)
		counters[rnd.Intn(cl)].Get(n)
	}
}

func getTotalCounterSum(counters count.TotalCounters) (int, int) {
	sumTotal := 0
	sum := 0
	for idx := range counters {
		sumTotal += int(counters[idx].GetTotal())
		subCounters := counters[idx].GetSubCounters()
		for _, sub := range subCounters {
			sum += int(sub.Get())
		}
	}
	return sumTotal, sum
}
