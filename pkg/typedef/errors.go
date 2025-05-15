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

package typedef

import "github.com/pkg/errors"

var (
	ErrSchemaConfigInvalidRangePK = errors.New(
		"max number of partition keys must be bigger than min number of partition keys",
	)
	ErrSchemaConfigInvalidRangeCK = errors.New(
		"max number of clustering keys must be bigger than min number of clustering keys",
	)
	ErrSchemaConfigInvalidRangeCols = errors.New(
		"max number of columns must be bigger than min number of columns",
	)
)
