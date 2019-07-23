package replication

import (
	"testing"
)

func TestToCQL(t *testing.T) {
	tests := map[string]struct {
		rs   *Replication
		want string
	}{
		"simple": {
			rs:   NewSimpleStrategy(),
			want: "{'class':'SimpleStrategy','replication_factor':1}",
		},
		"network": {
			rs:   NewNetworkTopologyStrategy(),
			want: "{'class':'NetworkTopologyStrategy','datacenter1':1}",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			got := test.rs.ToCQL()
			if got != test.want {
				t.Fatalf("expected '%s', got '%s'", test.want, got)
			}
		})
	}
}
