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

package comp

// Results interface for comparison Test and Oracle rows
type Results interface {
	// EqualSingeRow equals fist rows and deletes if equal, returns count of equal rows.
	// Equals only the first Oracle and Test rows without `for` cycle.
	// Most responses have only one row.
	EqualSingeRow() int

	// EasyEqualRowsTest returns count of equal rows into stores simultaneously deletes equal rows.
	// Most cases have no difference between Oracle and Test rows, therefore the fastest compare way to compare
	// Test and Oracle responses row by row.
	// Travels through Test rows.
	EasyEqualRowsTest() int
	// EasyEqualRowsOracle same as EasyEqualRowsTest, but travels through Oracle rows.
	EasyEqualRowsOracle() int

	// EqualRowsTest equals all rows and deletes if equal, returns count of equal rows.
	// For cases then EasyEqualRowsTest did not bring full success.
	// Travels through Test rows.
	EqualRowsTest() int
	// EqualRowsOracle same as EqualRowsTest, but travels through Oracle rows.
	EqualRowsOracle() int

	LenRowsOracle() int
	LenRowsTest() int

	StringAllRows(prefix string) []string
	HaveRows() bool
}
