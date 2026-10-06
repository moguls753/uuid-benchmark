package cassandra

import (
	"strings"
	"testing"

	"github.com/gocql/gocql"

	"github.com/moguls753/uuid-benchmark/internal/cluster"
)

func TestParseConsistency(t *testing.T) {
	cases := map[cluster.Consistency]gocql.Consistency{
		cluster.ConsistencyOne:         gocql.One,
		cluster.ConsistencyLocalOne:    gocql.LocalOne,
		cluster.ConsistencyLocalQuorum: gocql.LocalQuorum,
		cluster.ConsistencyQuorum:      gocql.Quorum,
	}
	for input, want := range cases {
		t.Run(string(input), func(t *testing.T) {
			if got := parseConsistency(input); got != want {
				t.Errorf("parseConsistency(%q): got %v want %v", input, got, want)
			}
		})
	}
}

func TestParseConsistencyUnknownFallsBack(t *testing.T) {
	// Validate() rejects unknown values upstream, so the default branch is
	// defense-in-depth only. Pin the fallback behaviour so a future refactor
	// that changes the default doesn't silently shift consistency.
	got := parseConsistency(cluster.Consistency("nonsense"))
	if got != gocql.LocalOne {
		t.Errorf("parseConsistency unknown: got %v want %v", got, gocql.LocalOne)
	}
}

func TestReplicationStmt(t *testing.T) {
	t.Run("local single uses SimpleStrategy", func(t *testing.T) {
		cfg := cluster.ClusterConfig{Mode: cluster.ModeLocalSingle, ReplicationFactor: 1}
		got := replicationStmt(cfg)
		if !strings.Contains(got, "SimpleStrategy") {
			t.Errorf("got %q", got)
		}
		if !strings.Contains(got, "'replication_factor': 1") {
			t.Errorf("got %q", got)
		}
	})
	t.Run("local cluster uses NetworkTopologyStrategy with dc1", func(t *testing.T) {
		cfg := cluster.ClusterConfig{Mode: cluster.ModeLocalCluster, ReplicationFactor: 3}
		got := replicationStmt(cfg)
		if !strings.Contains(got, "NetworkTopologyStrategy") {
			t.Errorf("got %q", got)
		}
		if !strings.Contains(got, "'dc1': 3") {
			t.Errorf("got %q", got)
		}
	})
	t.Run("remote cluster uses NetworkTopologyStrategy with dc1", func(t *testing.T) {
		cfg := cluster.ClusterConfig{Mode: cluster.ModeRemoteCluster, ReplicationFactor: 3}
		got := replicationStmt(cfg)
		if !strings.Contains(got, "NetworkTopologyStrategy") {
			t.Errorf("got %q", got)
		}
		if !strings.Contains(got, "'dc1': 3") {
			t.Errorf("got %q", got)
		}
	})
}
