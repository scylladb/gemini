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

package drivers

import (
	"context"

	"github.com/scylladb/gemini/pkg/typedef"
)

type Nop struct{}

func NewNop() Nop {
	return Nop{}
}

func (n Nop) Execute(context.Context, *typedef.Stmt) error {
	return nil
}

func (n Nop) Fetch(context.Context, *typedef.Stmt) ([]map[string]any, error) {
	return nil, nil
}
