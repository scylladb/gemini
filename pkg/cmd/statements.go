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

package main

import (
	"encoding/json"
	"os"

	"github.com/pkg/errors"

	"github.com/scylladb/gemini/pkg/generators/statements"
	"github.com/scylladb/gemini/pkg/utils"
)

func parseStatementRatiosJSON(jsonStr string) (statements.Ratios, error) {
	ratios := statements.DefaultStatementRatios()

	if jsonStr == "" {
		return ratios, nil
	}

	var data []byte

	if utils.IsFile(jsonStr) {
		bytes, err := os.ReadFile(jsonStr)
		if err != nil {
			return statements.Ratios{}, errors.Wrapf(err, "failed to read statement ratios JSON file %q", jsonStr)
		}

		data = bytes
	} else {
		data = utils.UnsafeBytes(utils.SingleToDoubleQuoteReplacer.Replace(jsonStr))
	}

	var jsonMap map[string]any
	if err := json.Unmarshal(data, &jsonMap); err != nil {
		return statements.Ratios{}, errors.Wrap(err, "failed to parse statement ratios JSON")
	}

	if err := json.Unmarshal(data, &ratios); err != nil {
		return statements.Ratios{}, errors.Wrap(err, "failed to parse statement ratios JSON")
	}

	if err := fixPartialSubtypes(&ratios, jsonMap); err != nil {
		return statements.Ratios{}, errors.Wrap(err, "failed to fix partial subtype configurations")
	}

	return ratios, nil
}

func fixPartialSubtypes(ratios *statements.Ratios, jsonMap map[string]any) error {
	if err := fixMutationSubtypes(ratios, jsonMap); err != nil {
		return err
	}

	if err := fixValidationSubtypes(ratios, jsonMap); err != nil {
		return err
	}

	return nil
}

// fixMutationSubtypes handles partial mutation subtype configurations
func fixMutationSubtypes(ratios *statements.Ratios, jsonMap map[string]any) error {
	defaults := statements.DefaultStatementRatios()

	mutationInterface, exists := jsonMap["mutation"]
	if !exists {
		return nil
	}

	mutation, ok := mutationInterface.(map[string]any)
	if !ok {
		return nil
	}

	if err := fixInsertSubtypes(ratios, mutation, defaults); err != nil {
		return err
	}

	if err := fixDeleteSubtypes(ratios, mutation, defaults); err != nil {
		return err
	}

	return nil
}

// fixValidationSubtypes handles partial validation subtype configurations
func fixValidationSubtypes(ratios *statements.Ratios, jsonMap map[string]any) error {
	defaults := statements.DefaultStatementRatios()

	validationInterface, exists := jsonMap["validation"]
	if !exists {
		return nil
	}

	validation, ok := validationInterface.(map[string]any)
	if !ok {
		return nil
	}

	return fixSelectSubtypes(ratios, validation, defaults)
}

// fixInsertSubtypes handles partial insert subtype configurations
func fixInsertSubtypes(ratios *statements.Ratios, mutation map[string]any, defaults statements.Ratios) error {
	insertSubtypesInterface, exists := mutation["insert_subtypes"]
	if !exists {
		return nil
	}

	insertSubtypes, ok := insertSubtypesInterface.(map[string]any)
	if !ok {
		return nil
	}

	var hasRegular, hasJSON bool

	if _, found := insertSubtypes["regular_insert"]; found {
		hasRegular = true
	}
	if _, found := insertSubtypes["json_insert"]; found {
		hasJSON = true
	}

	// Handle different cases
	switch {
	case hasRegular && !hasJSON:
		ratios.MutationRatios.InsertSubtypeRatios.JSONInsertRatio = 1.0 - ratios.MutationRatios.InsertSubtypeRatios.RegularInsertRatio
	case hasJSON && !hasRegular:
		ratios.MutationRatios.InsertSubtypeRatios.RegularInsertRatio = 1.0 - ratios.MutationRatios.InsertSubtypeRatios.JSONInsertRatio
	case !hasRegular && !hasJSON:
		// Empty object - use defaults
		ratios.MutationRatios.InsertSubtypeRatios = defaults.MutationRatios.InsertSubtypeRatios
	}

	return nil
}

// fixDeleteSubtypes handles partial delete subtype configurations
func fixDeleteSubtypes(ratios *statements.Ratios, mutation map[string]any, defaults statements.Ratios) error {
	deleteSubtypesInterface, exists := mutation["delete_subtypes"]
	if !exists {
		return nil
	}

	deleteSubtypes, ok := deleteSubtypesInterface.(map[string]any)
	if !ok {
		return nil
	}

	providedFields := make(map[string]bool)
	totalProvided := 0.0

	if _, found := deleteSubtypes["whole_partition"]; found {
		providedFields["whole_partition"] = true
		totalProvided += ratios.MutationRatios.DeleteSubtypeRatios.WholePartitionRatio
	}
	if _, found := deleteSubtypes["single_row"]; found {
		providedFields["single_row"] = true
		totalProvided += ratios.MutationRatios.DeleteSubtypeRatios.SingleRowRatio
	}
	if _, found := deleteSubtypes["single_column"]; found {
		providedFields["single_column"] = true
		totalProvided += ratios.MutationRatios.DeleteSubtypeRatios.SingleColumnRatio
	}
	if _, found := deleteSubtypes["multiple_partitions"]; found {
		providedFields["multiple_partitions"] = true
		totalProvided += ratios.MutationRatios.DeleteSubtypeRatios.MultiplePartitionsRatio
	}

	providedCount := len(providedFields)
	if providedCount > 0 && providedCount < 4 {
		// Distribute remaining ratio among unprovided fields
		remaining := 1.0 - totalProvided
		unprovided := 4 - providedCount
		perUnprovided := remaining / float64(unprovided)

		if !providedFields["whole_partition"] {
			ratios.MutationRatios.DeleteSubtypeRatios.WholePartitionRatio = perUnprovided
		}
		if !providedFields["single_row"] {
			ratios.MutationRatios.DeleteSubtypeRatios.SingleRowRatio = perUnprovided
		}
		if !providedFields["single_column"] {
			ratios.MutationRatios.DeleteSubtypeRatios.SingleColumnRatio = perUnprovided
		}
		if !providedFields["multiple_partitions"] {
			ratios.MutationRatios.DeleteSubtypeRatios.MultiplePartitionsRatio = perUnprovided
		}
	} else if providedCount == 0 {
		// Empty object - use defaults
		ratios.MutationRatios.DeleteSubtypeRatios = defaults.MutationRatios.DeleteSubtypeRatios
	}

	return nil
}

// fixSelectSubtypes handles partial select subtype configurations
func fixSelectSubtypes(ratios *statements.Ratios, validation map[string]any, defaults statements.Ratios) error {
	selectSubtypesInterface, exists := validation["select_subtypes"]
	if !exists {
		return nil
	}

	selectSubtypes, ok := selectSubtypesInterface.(map[string]any)
	if !ok {
		return nil
	}

	providedFields := make(map[string]bool)
	totalProvided := 0.0

	if _, found := selectSubtypes["single_partition"]; found {
		providedFields["single_partition"] = true
		totalProvided += ratios.ValidationRatios.SelectSubtypeRatios.SinglePartitionRatio
	}
	if _, found := selectSubtypes["multiple_partition"]; found {
		providedFields["multiple_partition"] = true
		totalProvided += ratios.ValidationRatios.SelectSubtypeRatios.MultiplePartitionRatio
	}
	if _, found := selectSubtypes["clustering_range"]; found {
		providedFields["clustering_range"] = true
		totalProvided += ratios.ValidationRatios.SelectSubtypeRatios.ClusteringRangeRatio
	}
	if _, found := selectSubtypes["multiple_partition_clustering_range"]; found {
		providedFields["multiple_partition_clustering_range"] = true
		totalProvided += ratios.ValidationRatios.SelectSubtypeRatios.MultiplePartitionClusteringRangeRatio
	}
	if _, found := selectSubtypes["single_index"]; found {
		providedFields["single_index"] = true
		totalProvided += ratios.ValidationRatios.SelectSubtypeRatios.SingleIndexRatio
	}

	providedCount := len(providedFields)
	if providedCount > 0 && providedCount < 5 {
		// Distribute remaining ratio among unprovided fields
		remaining := 1.0 - totalProvided
		unprovided := 5 - providedCount
		perUnprovided := remaining / float64(unprovided)

		if !providedFields["single_partition"] {
			ratios.ValidationRatios.SelectSubtypeRatios.SinglePartitionRatio = perUnprovided
		}
		if !providedFields["multiple_partition"] {
			ratios.ValidationRatios.SelectSubtypeRatios.MultiplePartitionRatio = perUnprovided
		}
		if !providedFields["clustering_range"] {
			ratios.ValidationRatios.SelectSubtypeRatios.ClusteringRangeRatio = perUnprovided
		}
		if !providedFields["multiple_partition_clustering_range"] {
			ratios.ValidationRatios.SelectSubtypeRatios.MultiplePartitionClusteringRangeRatio = perUnprovided
		}
		if !providedFields["single_index"] {
			ratios.ValidationRatios.SelectSubtypeRatios.SingleIndexRatio = perUnprovided
		}
	} else if providedCount == 0 {
		// Empty object - use defaults
		ratios.ValidationRatios.SelectSubtypeRatios = defaults.ValidationRatios.SelectSubtypeRatios
	}

	return nil
}
