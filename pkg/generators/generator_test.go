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

package generators_test

import (
	"math/rand/v2"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/generators"
	"github.com/scylladb/gemini/pkg/typedef"
)

//go:norace
func TestGenerator(t *testing.T) {
	t.Parallel()
	assert := require.New(t)

	table := &typedef.Table{
		Name:          "tbl",
		PartitionKeys: generators.CreatePkColumns(1, "pk"),
	}
	var current atomic.Int32
	cfg := generators.Config{
		PartitionsRangeConfig: typedef.PartitionRangeConfig{
			MaxStringLength: 10,
			MinStringLength: 0,
			MaxBlobLength:   10,
			MinBlobLength:   0,
		},
		PkUsedBufferSize: 100,
		PartitionsCount:  10000,
		PartitionsDistributionFunc: func() uint32 {
			return uint32(current.Load())
		},
	}
	logger, _ := zap.NewDevelopment()
	generator := generators.NewGenerator(table, cfg, logger, rand.NewChaCha8([32]byte{}))
	for i := range cfg.PartitionsCount {
		current.Store(int32(i))
		v, err := generator.Get(t.Context())
		assert.NoError(err)
		n, err := generator.Get(t.Context())
		assert.NoError(err)
		assert.Equal(v.Token%uint32(generator.PartitionCount()), n.Token%uint32(generator.PartitionCount()))
	}
}
