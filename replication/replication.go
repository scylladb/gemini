package replication

import (
	"encoding/json"
	"strings"
)

type Replication map[string]interface{}

func (r *Replication) ToCQL() string {
	b, _ := json.Marshal(r)
	return strings.ReplaceAll(string(b), "\"", "'")
}

func NewSimpleStrategy() *Replication {
	return &Replication{
		"class":              "SimpleStrategy",
		"replication_factor": 1,
	}
}

func NewNetworkTopologyStrategy() *Replication {
	return &Replication{
		"class":       "NetworkTopologyStrategy",
		"datacenter1": 1,
	}
}
