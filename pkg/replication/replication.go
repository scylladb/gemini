// Copyright 2019 ScyllaDB
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package replication

import (
	"encoding/json"
	"strings"
)

type Replication map[string]any

func (r Replication) ToCQL() string {
	b, _ := json.Marshal(r)
	return strings.ReplaceAll(string(b), "\"", "'")
}

func NewSimpleStrategy() Replication {
	return Replication{
		"class":              "SimpleStrategy",
		"replication_factor": 1,
	}
}

func NewNetworkTopologyStrategy() Replication {
	return Replication{
		"class":              "NetworkTopologyStrategy",
		"datacenter1":        1,
		"replication_factor": 1,
	}
}

func (r Replication) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any(r))
}

func (r *Replication) UnmarshalJSON(data []byte) error {
	dataMap := make(map[string]any)
	if err := json.Unmarshal(data, &dataMap); err != nil {
		return err
	}

	if dataMap == nil {
		return nil
	}

	out := Replication{}
	for idx := range dataMap {
		val := dataMap[idx]
		if fVal, ok := val.(float64); ok {
			dataMap[idx] = int(fVal)
		}
		out[idx] = dataMap[idx]
	}
	*r = out
	return nil
}
