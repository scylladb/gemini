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

package main

import (
	"fmt"
	"time"

	"github.com/briandowns/spinner"
)

type spinningFeedback struct {
	s *spinner.Spinner
}

func (sf *spinningFeedback) Set(format string, args ...any) {
	if sf.s != nil {
		sf.s.Suffix = fmt.Sprintf(format, args...)
	}
}

func (sf *spinningFeedback) Stop() {
	if sf.s != nil {
		sf.s.Stop()
	}
}

func createSpinner(active bool) *spinningFeedback {
	if !active {
		return &spinningFeedback{}
	}
	spinnerCharSet := []string{"|", "/", "-", "\\"}
	sp := spinner.New(spinnerCharSet, 1*time.Second)
	_ = sp.Color("black")
	sp.Start()
	return &spinningFeedback{
		s: sp,
	}
}
