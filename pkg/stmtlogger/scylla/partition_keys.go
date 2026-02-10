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

	// Determine the number of partitions by finding the max length of any key's values.
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

	// Attach lastValidations from the job error if available.
	if jobErr != nil && len(jobErr.LastValidations) > 0 {
		// For single-partition errors the whole map applies to the one partition.
		// For multi-partition errors the validation data is keyed by partition UUID
		// which we attach to all partitions (the consumer can correlate by keys).
		if numPartitions == 1 {
			validations := make(map[string]LastValidationData, len(jobErr.LastValidations))
			for id, v := range jobErr.LastValidations {
				validations[id] = LastValidationData{
					FirstSuccessNS: v.FirstSuccessNS,
					LastSuccessNS:  v.LastSuccessNS,
					LastFailureNS:  v.LastFailureNS,
					Recent:         v.Recent,
				}
			}
			result[0].LastValidations = validations
		} else {
			// Multiple partitions: distribute validation data across all partitions.
			// Each partition gets a copy of the full validation map since we can't
			// correlate partition UUID to position without additional metadata.
			validations := make(map[string]LastValidationData, len(jobErr.LastValidations))
			for id, v := range jobErr.LastValidations {
				validations[id] = LastValidationData{
					FirstSuccessNS: v.FirstSuccessNS,
					LastSuccessNS:  v.LastSuccessNS,
					LastFailureNS:  v.LastFailureNS,
					Recent:         v.Recent,
				}
			}
			for i := range result {
				result[i].LastValidations = validations
			}
		}
	}

	return result
}
