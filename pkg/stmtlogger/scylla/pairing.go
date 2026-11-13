// Copyright 2026 ScyllaDB
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

package scylla

import (
	"time"

	"github.com/scylladb/gemini/pkg/stmtlogger"
	"github.com/scylladb/gemini/pkg/typedef"
)

// pairWindow is how long the pairing stage keeps one cluster's item while it
// waits for the other cluster's item of the same execution. A cluster that
// gemini did not retry, or a statement only one cluster ran, leaves its partner
// missing. After the window the row is written with that cluster's columns
// null. Failure fetches wait statementErrorFetchDelay, which is far longer, so
// a row is in _logs before a fetch reads it.
const pairWindow = 2 * time.Second

// logPair holds the test and the oracle item of one execution of a statement
// on one partition. Either side is missing when gemini observed only one
// cluster for that execution.
type logPair struct {
	test      stmtlogger.Item
	oracle    stmtlogger.Item
	hasTest   bool
	hasOracle bool
}

// pairKey identifies one execution on one partition. Both clusters run the same
// *typedef.Stmt, so the pointer with the partition index and the attempts names
// the execution on either cluster. A pending pair holds the pointer, so it
// cannot be reused for another statement while the pair is open.
type pairKey struct {
	origin         *typedef.Stmt
	partitionIndex int
	geminiAttempt  int
	attempt        int
}

func keyOf(item stmtlogger.Item) pairKey {
	return pairKey{
		origin:         item.Origin,
		partitionIndex: item.PartitionIndex,
		geminiAttempt:  item.GeminiAttempt,
		attempt:        item.Attempt,
	}
}

func pairOf(item stmtlogger.Item) logPair {
	var p logPair
	p.set(item)

	return p
}

// set stores item as its cluster's side. It reports false when that side is
// already set, so the caller writes the held pair and starts a new one.
func (p *logPair) set(item stmtlogger.Item) bool {
	switch item.Type {
	case stmtlogger.TypeTest:
		if p.hasTest {
			return false
		}
		p.test, p.hasTest = item, true
	case stmtlogger.TypeOracle:
		if p.hasOracle {
			return false
		}
		p.oracle, p.hasOracle = item, true
	default:
		panic("statement log item without a cluster type: " + string(item.Type))
	}

	return true
}

func (p *logPair) complete() bool {
	return p.hasTest && p.hasOracle
}

// shared returns the item whose statement, values, partition keys and attempts
// the row stores. Both items of a pair hold the same ones.
func (p *logPair) shared() *stmtlogger.Item {
	if p.hasTest {
		return &p.test
	}

	return &p.oracle
}

// start returns the earlier start of the two clusters. It is the row's ts.
func (p *logPair) start() time.Time {
	switch {
	case p.hasTest && p.hasOracle:
		if p.oracle.Start.Time.Before(p.test.Start.Time) {
			return p.oracle.Start.Time
		}

		return p.test.Start.Time
	case p.hasTest:
		return p.test.Start.Time
	default:
		return p.oracle.Start.Time
	}
}
