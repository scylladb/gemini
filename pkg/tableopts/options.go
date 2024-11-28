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

package tableopts

import (
	"encoding/json"
	"strings"

	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Option interface {
	ToCQL() string
}
type SimpleOption struct {
	key string
	val string
}

func (o *SimpleOption) ToCQL() string {
	return o.key + " = " + o.val
}

type MapOption struct {
	val map[string]any
	key string
}

func (o *MapOption) ToCQL() string {
	b, _ := json.Marshal(o.val)
	return o.key + " = " + strings.ReplaceAll(string(b), "\"", "'")
}

func FromCQL(cql string) (Option, error) {
	parts := strings.Split(cql, "=")
	if len(parts) != 2 {
		return nil, errors.Errorf("invalid table option, exactly two parts separated by '=' is needed, input=%s", cql)
	}

	keyPart := strings.TrimSpace(parts[0])
	valPart := strings.TrimSpace(parts[1])
	if strings.HasPrefix(valPart, "{") {
		o := &MapOption{
			key: keyPart,
		}
		if err := json.Unmarshal([]byte(strings.ReplaceAll(valPart, "'", "\"")), &o.val); err != nil {
			return nil, errors.Wrapf(err, "unable to interpret table options %s", cql)
		}
		return o, nil
	}
	return &SimpleOption{
		key: keyPart,
		val: valPart,
	}, nil
}

func CreateTableOptions(tableOptionStrings []string, logger *zap.Logger) []Option {
	tableOptions := make([]Option, 0, len(tableOptionStrings))

	for _, optionString := range tableOptionStrings {
		o, err := FromCQL(optionString)
		if err != nil {
			logger.Warn("invalid table option", zap.String("option", optionString), zap.Error(err))
			continue
		}
		tableOptions = append(tableOptions, o)
	}
	return tableOptions
}
