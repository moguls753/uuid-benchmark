package cluster

import "testing"

func TestClusterConfigDefaults(t *testing.T) {
	c := DefaultLocalSingle()
	if c.Mode != ModeLocalSingle {
		t.Errorf("default mode: got %v want %v", c.Mode, ModeLocalSingle)
	}
	if got, want := len(c.ContactPoints), 1; got != want {
		t.Errorf("contact points: got %d want %d", got, want)
	}
	if c.ContactPoints[0] != "127.0.0.1" {
		t.Errorf("contact point[0]: got %q want %q", c.ContactPoints[0], "127.0.0.1")
	}
	if c.ReplicationFactor != 1 {
		t.Errorf("RF: got %d want 1", c.ReplicationFactor)
	}
	if c.Consistency != ConsistencyLocalOne {
		t.Errorf("consistency: got %v want %v", c.Consistency, ConsistencyLocalOne)
	}
	if c.Keyspace != "uuid_benchmark" {
		t.Errorf("keyspace: got %q want %q", c.Keyspace, "uuid_benchmark")
	}
	if c.NumBuckets != 1000 {
		t.Errorf("num buckets: got %d want 1000", c.NumBuckets)
	}
}

func TestClusterConfigValidate(t *testing.T) {
	// Helper: a fully-valid remote cluster config that individual sub-cases
	// can mutate to isolate one failure mode at a time.
	validRemote := ClusterConfig{
		Mode:              ModeRemoteCluster,
		ContactPoints:     []string{"a", "b", "c"},
		Hostnames:         []string{"a", "b", "c"},
		SSHUser:           "u",
		SSHKeyPath:        "/home/u/.ssh/id_ed25519",
		ReplicationFactor: 3,
		Consistency:       ConsistencyLocalQuorum,
		Keyspace:          "uuid_benchmark",
		NumBuckets:        1000,
	}

	tests := []struct {
		name    string
		cfg     ClusterConfig
		wantErr bool
	}{
		// Happy paths
		{"valid local single", DefaultLocalSingle(), false},
		{"valid local cluster (RF=3, 1 contact point)", ClusterConfig{Mode: ModeLocalCluster, ContactPoints: []string{"127.0.0.1"}, ReplicationFactor: 3, Consistency: ConsistencyLocalQuorum, Keyspace: "uuid_benchmark", NumBuckets: 1000}, false},
		{"valid remote cluster", validRemote, false},

		// Mode-independent error paths
		{"unknown mode (empty)", func() ClusterConfig { c := validRemote; c.Mode = ""; return c }(), true},
		{"unknown mode (typo)", func() ClusterConfig { c := validRemote; c.Mode = "localcluster"; return c }(), true},
		{"empty contact points (mode-independent)", ClusterConfig{Mode: ModeLocalSingle, ReplicationFactor: 1, Consistency: ConsistencyLocalOne, Keyspace: "uuid_benchmark", NumBuckets: 1000}, true},
		{"RF zero", ClusterConfig{Mode: ModeLocalSingle, ContactPoints: []string{"x"}, Consistency: ConsistencyLocalOne, Keyspace: "uuid_benchmark", NumBuckets: 1000}, true},
		{"RF negative", ClusterConfig{Mode: ModeLocalSingle, ContactPoints: []string{"x"}, ReplicationFactor: -1, Consistency: ConsistencyLocalOne, Keyspace: "uuid_benchmark", NumBuckets: 1000}, true},
		{"empty keyspace", func() ClusterConfig { c := validRemote; c.Keyspace = ""; return c }(), true},
		{"unknown consistency", func() ClusterConfig { c := validRemote; c.Consistency = "strong"; return c }(), true},
		{"num buckets zero", func() ClusterConfig { c := validRemote; c.NumBuckets = 0; return c }(), true},

		// RemoteCluster-specific error paths
		{"remote RF greater than hostnames", func() ClusterConfig {
			c := validRemote
			c.Hostnames = []string{"a", "b"}
			c.ContactPoints = []string{"a", "b"}
			return c
		}(), true},
		{"remote missing SSH user", func() ClusterConfig { c := validRemote; c.SSHUser = ""; return c }(), true},
		{"remote missing hostnames", func() ClusterConfig { c := validRemote; c.Hostnames = nil; return c }(), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() err=%v wantErr=%v", err, tt.wantErr)
			}
		})
	}
}
