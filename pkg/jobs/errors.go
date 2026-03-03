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

package jobs

import (
	"errors"

	"github.com/google/uuid"

	"github.com/scylladb/gemini/pkg/typedef"
)

var (
	ErrMutationJobStopped   = errors.New("mutation job stopped due to errors")
	ErrValidationJobStopped = errors.New("validation job stopped due to errors")
)

func collectPartitionIDs(pks []typedef.PartitionKeys) []uuid.UUID {
	if len(pks) == 0 {
		return nil
	}

	ids := make([]uuid.UUID, 0, len(pks))

	for i := range pks {
		if pks[i].ID != uuid.Nil {
			ids = append(ids, pks[i].ID)
		}
	}

	return ids
}
