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

package scylla

import (
	"sort"

	"github.com/scylladb/gemini/pkg/joberror"
)

// reorganizePartitionKeys transposes a flat map of partition key arrays into
// per-partition PartitionInfo entries, and attaches any lastValidations from the
// job error.
//
// Input layout (flat map):
//
//	pk0: [1, 2]
//	pk1: [3, 4]
//
// Means two partitions were hit:
//
//	Partition 0: pk0=1, pk1=3
//	Partition 1: pk0=2, pk1=4
//
// Output: []PartitionInfo with one entry per partition, each containing a
// map[string]any of that partition's key values and its lastValidations (if any).
func reorganizePartitionKeys(flatKeys map[string][]any, jobErr *joberror.JobError) []PartitionInfo {
	if len(flatKeys) == 0 {
		return nil
	}

	numPartitions := 0
	for _, vals := range flatKeys {
		if len(vals) > numPartitions {
			numPartitions = len(vals)
		}
	}

	if numPartitions == 0 {
		return nil
	}

	// Sort key names for deterministic output.
	keyNames := make([]string, 0, len(flatKeys))
	for k := range flatKeys {
		keyNames = append(keyNames, k)
	}
	sort.Strings(keyNames)

	result := make([]PartitionInfo, numPartitions)
	for i := range numPartitions {
		pkMap := make(map[string]any, len(keyNames))
		for _, name := range keyNames {
			vals := flatKeys[name]
			if i < len(vals) {
				pkMap[name] = vals[i]
			}
		}
		result[i] = PartitionInfo{
			PartitionKeys: pkMap,
		}
	}

	if jobErr != nil && len(jobErr.LastValidations) > 0 {
		if numPartitions == 1 {
			result[0].LastValidations = jobErr.LastValidations
		} else {
			for i := range result {
				result[i].LastValidations = jobErr.LastValidations
			}
		}
	}

	return result
}
