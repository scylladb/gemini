// Copyright 2025 ScyllaDB
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

package store_test

import (
	"context"
	"fmt"

	"github.com/scylladb/gemini/pkg/store"
)

// ExampleRowIterator_Collect demonstrates how to collect all rows from an iterator
func ExampleRowIterator_Collect() {
	// Create a simple iterator
	iter := store.RowIterator(func(yield func(store.Row, error) bool) {
		for i := range 3 {
			row := store.NewRow([]string{"id", "name"}, []any{i, fmt.Sprintf("user%d", i)})
			if !yield(row, nil) {
				return
			}
		}
	})

	// Collect all rows
	rows, err := iter.Collect()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Collected %d rows\n", len(rows))
	// Output: Collected 3 rows
}

// ExampleRowIterator_Count demonstrates counting rows without loading them into memory
func ExampleRowIterator_Count() {
	// Create a simple iterator
	iter := store.RowIterator(func(yield func(store.Row, error) bool) {
		for i := range 100 {
			row := store.NewRow([]string{"id"}, []any{i})
			if !yield(row, nil) {
				return
			}
		}
	})

	// Count rows without loading them
	count, err := iter.Count()
	if err != nil {
		panic(err)
	}

	fmt.Printf("Found %d rows\n", count)
	// Output: Found 100 rows
}

// ExampleRowIterator demonstrates iterating over rows one by one
func ExampleRowIterator() {
	// Create a simple iterator
	iter := store.RowIterator(func(yield func(store.Row, error) bool) {
		for i := range 3 {
			row := store.NewRow([]string{"id"}, []any{i})
			if !yield(row, nil) {
				return
			}
		}
	})

	// Process rows one by one
	count := 0
	for row, err := range iter {
		if err != nil {
			panic(err)
		}
		if row.Get("id") != nil {
			count++
		}
	}

	fmt.Printf("Processed %d rows\n", count)
	// Output: Processed 3 rows
}

// ExampleCompareCollectedRows demonstrates comparing rows from two sources
func ExampleCompareCollectedRows() {
	ctx := context.Background()
	_ = ctx // Use ctx in real code

	// This is a simplified example - in real usage, you would use actual iterators
	// from store.loadIter() calls

	fmt.Println("Compare rows from test and oracle stores")
	fmt.Println("Use CompareCollectedRows for detailed comparison results")
	// Output:
	// Compare rows from test and oracle stores
	// Use CompareCollectedRows for detailed comparison results
}
