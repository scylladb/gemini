// Copyright 2026 ScyllaDB
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

package typedef_test

import (
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/scylladb/gemini/pkg/typedef"
)

// TestGenValue_SameSeedGivesSameValues pins the property the partition pool
// relies on: it stores a seed per partition and regenerates the key values on
// every borrow, so a partition key type must draw only from its RNG.
func TestGenValue_SameSeedGivesSameValues(t *testing.T) {
	t.Parallel()

	config := &typedef.ValueRangeConfig{
		MaxBlobLength:   32,
		MinBlobLength:   1,
		MaxStringLength: 32,
		MinStringLength: 1,
	}

	types := make([]typedef.Type, 0, len(typedef.PartitionKeyTypes)+4)
	types = append(types, &typedef.TupleType{
		ComplexType: typedef.TypeTuple,
		ValueTypes:  []typedef.SimpleType{typedef.TypeUUID, typedef.TypeTimeuuid, typedef.TypeText},
	})
	for _, simple := range typedef.PartitionKeyTypes {
		types = append(types, simple)
	}
	types = append(types, typedef.TypeUUID, typedef.TypeTimeuuid, typedef.TypeBlob)

	for _, typ := range types {
		t.Run(typ.Name(), func(t *testing.T) {
			t.Parallel()

			for seed := range uint64(20) {
				first := typ.GenValue(rand.New(rand.NewPCG(seed, 1)), config)
				second := typ.GenValue(rand.New(rand.NewPCG(seed, 1)), config)
				if !reflect.DeepEqual(first, second) {
					t.Fatalf("seed %d gave %#v, then %#v", seed, first, second)
				}
			}
		})
	}
}
