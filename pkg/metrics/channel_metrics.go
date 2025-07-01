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
)

var channelMetrics = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Name: "channel_size",
	},
	[]string{"type", "context"},
)

type ChannelMetrics struct {
	channel prometheus.Gauge
}

func NewChannelMetrics(ty, name string) ChannelMetrics {
	return ChannelMetrics{
		channel: channelMetrics.WithLabelValues(ty, name),
	}
}

func (c ChannelMetrics) Inc() {
	c.channel.Inc()
}

func (c ChannelMetrics) Dec() {
	c.channel.Dec()
}
