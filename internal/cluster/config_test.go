package cluster

import (
	"strings"
	"testing"
)

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

		// FIX 3: new RemoteCluster-specific rejections
		{"remote single hostname (use local-single)", func() ClusterConfig {
			c := validRemote
			c.Hostnames = []string{"a"}
			c.ContactPoints = []string{"a"}
			c.ReplicationFactor = 1
			return c
		}(), true},
		{"remote empty hostname in slice", func() ClusterConfig {
			c := validRemote
			c.Hostnames = []string{"a", "", "c"}
			c.ContactPoints = []string{"a", "", "c"}
			return c
		}(), true},
		{"remote duplicate hostname", func() ClusterConfig {
			c := validRemote
			c.Hostnames = []string{"a", "a", "b"}
			c.ContactPoints = []string{"a", "a", "b"}
			return c
		}(), true},
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

// TestClusterConfigValidateErrorContext pins the FIX 3 error-message
// contracts that the test-table above only confirms produce an error. The
// operator parses these strings to figure out which CLI flag is wrong,
// so the substrings here are load-bearing.
func TestClusterConfigValidateErrorContext(t *testing.T) {
	mk := func(mut func(c *ClusterConfig)) ClusterConfig {
		c := ClusterConfig{
			Mode:              ModeRemoteCluster,
			ContactPoints:     []string{"a", "b", "c"},
			Hostnames:         []string{"a", "b", "c"},
			SSHUser:           "u",
			ReplicationFactor: 3,
			Consistency:       ConsistencyLocalQuorum,
			Keyspace:          "uuid_benchmark",
			NumBuckets:        1000,
		}
		mut(&c)
		return c
	}
	cases := []struct {
		name      string
		cfg       ClusterConfig
		wantParts []string
	}{
		{
			"empty hostname surfaces index",
			mk(func(c *ClusterConfig) { c.Hostnames = []string{"a", "", "c"} }),
			[]string{"hostnames[1]", "empty"},
		},
		{
			"duplicate hostname surfaces value",
			mk(func(c *ClusterConfig) { c.Hostnames = []string{"a", "a", "b"} }),
			[]string{`duplicate hostname "a"`},
		},
		{
			"single hostname points at local-single",
			mk(func(c *ClusterConfig) {
				c.Hostnames = []string{"a"}
				c.ReplicationFactor = 1
			}),
			[]string{"at least 2", "local-single"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			for _, p := range tc.wantParts {
				if !strings.Contains(err.Error(), p) {
					t.Errorf("error %q missing expected substring %q", err.Error(), p)
				}
			}
		})
	}
}
