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

package ver

import "sync/atomic"

// info represents information about check of responses on difference in protocol versions.
type info struct {
	// count of checked responses on difference in protocol versions.
	count atomic.Uint32
	// init indicating a status of check's initialization.
	init atomic.Bool
	// currentOld indicating a version of the first response.
	// True - version <=2, False - version >2.
	currentOld atomic.Bool
	// svMode indicating a mode (mv - multi protocol version or sv - single protocol version).
	svMode atomic.Bool
	// pass in a value 'true' indicates a check finished.
	pass atomic.Bool
}

// maxResponsesCount represent max count of responses to check on different versions of protocol.
const maxResponsesCount = uint32(200)

var Check = info{
	count:      atomic.Uint32{},
	init:       atomic.Bool{},
	currentOld: atomic.Bool{},
	svMode:     atomic.Bool{},
	pass:       atomic.Bool{},
}

func (p *info) reInit() {
	p.count = atomic.Uint32{}
	p.init = atomic.Bool{}
	p.currentOld = atomic.Bool{}
	p.svMode = atomic.Bool{}
	p.pass = atomic.Bool{}
}

func (p *info) ModeSV() bool {
	return p.svMode.Load()
}

func (p *info) Done() bool {
	return p.pass.Load()
}

// Add adds one response check.
// oldVersion 'true' means version <=2, 'false' means version >2.
func (p *info) Add(oldVersion bool) {
	if !p.init.Load() {
		// Check initialization.
		p.currentOld.Store(oldVersion)
		p.init.Store(true)
		return
	}
	if oldVersion == p.currentOld.Load() {
		diff := Check.count.Add(1)
		if diff >= maxResponsesCount || !p.pass.Load() {
			// Check done
			p.svMode.Store(true)
			p.pass.Store(true)
		}
	} else {
		// Different versions detected, check done, single version mode off.
		p.svMode.Store(false)
		p.pass.Store(true)
	}
}
