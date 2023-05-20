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

//go:build slow
// +build slow

package utils_test

import (
	"github.com/scylladb/gemini/pkg/utils"
	"golang.org/x/exp/rand"
	"testing"
	"testing/quick"
	"time"
)

var rnd = rand.New(rand.NewSource(uint64(time.Now().UnixNano())))

func TestNonEmptyRandString(t *testing.T) {
	// TODO: Figure out why this is so horribly slow...
	tt := time.Now()
	f := func(len int32) bool {
		if len < 0 {
			len = -len
		}
		r := utils.RandStringWithTime(rnd, int(len), tt)
		return r != ""
	}
	cfg := &quick.Config{MaxCount: 10}
	if err := quick.Check(f, cfg); err != nil {
		t.Error(err)
	}
}
