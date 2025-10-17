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

package generators

import (
	"context"
	"math/rand/v2"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/scylladb/gemini/pkg/distributions"
	"github.com/scylladb/gemini/pkg/metrics"
	"github.com/scylladb/gemini/pkg/replication"
	"github.com/scylladb/gemini/pkg/routingkey"
	"github.com/scylladb/gemini/pkg/schema"
	"github.com/scylladb/gemini/pkg/tableopts"
	"github.com/scylladb/gemini/pkg/testutils"
	"github.com/scylladb/gemini/pkg/typedef"
)

var KnownSeedsForStalePartitions = []int64{32, 60, 72, 50}

func TestGenerator_StalePartitions(t *testing.T) {
	t.Parallel()

	for _, seed := range KnownSeedsForStalePartitions {
		t.Run("Seed "+strconv.FormatInt(seed, 10), func(t *testing.T) {
			t.Parallel()
			assert := require.New(t)
			_, cancel := context.WithCancel(t.Context())
			logger := testutils.Must(zap.NewDevelopment())
			randSrc, distFunc := distributions.New(distributions.Uniform, 10_000, uint64(seed), 0, 1)
			t.Cleanup(cancel)
			sc, err := schema.Get(typedef.SchemaConfig{
				ReplicationStrategy:       replication.NewSimpleStrategy(),
				OracleReplicationStrategy: replication.NewSimpleStrategy(),
				TableOptions:              []tableopts.Option{},
				MaxTables:                 1,
				MaxPartitionKeys:          8,
				MinPartitionKeys:          2,
				MaxClusteringKeys:         3,
				MinClusteringKeys:         2,
				MaxColumns:                12,
				MinColumns:                5,
				MaxUDTParts:               5,
				MaxTupleParts:             5,
				MaxBlobLength:             256,
				MaxStringLength:           128,
				MinBlobLength:             0,
				MinStringLength:           0,
				UseCounters:               false,
				UseLWT:                    false,
				UseMaterializedViews:      false,
				CQLFeature:                typedef.CQLFeatureNormal,
			}, strconv.FormatInt(seed, 10), "")
			assert.NoError(err)

			g := &Generator{
				cancel:            cancel,
				logger:            logger,
				table:             sc.Tables[0],
				routingKeyCreator: routingkey.New(sc.Tables[0]),
				r:                 rand.New(randSrc),
				idxFunc:           distFunc,
				partitions:        NewPartitions(10_000, 128, nil),
				partitionsConfig: typedef.PartitionRangeConfig{
					MaxBlobLength:   256,
					MinBlobLength:   0,
					MaxStringLength: 128,
					MinStringLength: 0,
					UseLWT:          false,
				},
				partitionCount:   10_000,
				oldValuesMetrics: metrics.NewChannelMetrics("generator", sc.Tables[0].Name+"_old_values"),
				valuesMetrics:    metrics.NewChannelMetrics("generator", sc.Tables[0].Name+"_values"),
				oldDroppedValues: metrics.GeneratorDroppedValues.WithLabelValues(sc.Tables[0].Name, "old"),
			}

			assert.NoError(g.FindAndMarkStalePartitions())
		})
	}
}
