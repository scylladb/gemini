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

	if err := json.Unmarshal(data, &ratios); err != nil {
		return statements.Ratios{}, errors.Wrap(err, "failed to parse statement ratios JSON")
	}

	return ratios, nil
}
