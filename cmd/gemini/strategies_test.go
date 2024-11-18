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

package main

import (
	"encoding/json"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/scylladb/gemini/pkg/typedef"
)

// TestReadExampleSchema main task of this test to be sure that schema example (schema.json) is correct and have correct marshal, unmarshal
func TestReadExampleSchema(t *testing.T) {
	filePath := "schema.json"

	testSchema, err := readSchema(filePath, typedef.SchemaConfig{})
	if err != nil {
		t.Fatalf("failed to open schema example json file %s, error:%s", filePath, err)
	}

	opts := cmp.Options{
		cmp.AllowUnexported(typedef.Table{}, typedef.MaterializedView{}),
		cmpopts.IgnoreUnexported(typedef.Table{}, typedef.MaterializedView{}),
	}

	testSchemaMarshaled, err := json.MarshalIndent(testSchema, "  ", "  ")
	if err != nil {
		t.Fatalf("unable to marshal schema example json, error=%s\n", err)
	}
	testSchemaUnMarshaled := typedef.Schema{}
	if err = json.Unmarshal(testSchemaMarshaled, &testSchemaUnMarshaled); err != nil {
		t.Fatalf("unable to unmarshal json, error=%s\n", err)
	}

	if diff := cmp.Diff(*testSchema, testSchemaUnMarshaled, opts); diff != "" {
		t.Errorf("schema not the same after marshal/unmarshal, diff=%s", diff)
	}
}
