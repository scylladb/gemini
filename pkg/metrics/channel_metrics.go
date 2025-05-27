// Copyright 2025 ScyllaDB
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

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/scylladb/gemini/pkg/utils"
)

var channelMetrics = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "channel_size",
	},
	[]string{"type", "context"},
)

type (
	ChannelMetrics struct {
		name string
		ty   string
	}
)

func NewChannelMetrics[T any](ty, name string, size uint64) ChannelMetrics {
	MemoryMetrics.WithLabelValues(ty, name).Set(float64(utils.Sizeof(make(chan T, size))))

	return ChannelMetrics{name: name, ty: ty}
}

func (c ChannelMetrics) Inc(data any) {
	channelMetrics.WithLabelValues(c.ty, c.name).Inc()
	MemoryMetrics.WithLabelValues(c.ty, c.name).Add(float64(utils.Sizeof(data)))
}

func (c ChannelMetrics) Dec(data any) {
	channelMetrics.WithLabelValues(c.ty, c.name).Dec()
	MemoryMetrics.WithLabelValues(c.ty, c.name).Sub(float64(utils.Sizeof(data)))
}
