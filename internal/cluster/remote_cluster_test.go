package cluster

import (
	"slices"
	"testing"
)

func TestRemoteClusterImplementsBackend(t *testing.T) {
	var _ Backend = (*RemoteClusterBackend)(nil)
}

func validRemoteCfg() ClusterConfig {
	return ClusterConfig{
		Mode:              ModeRemoteCluster,
		ContactPoints:     []string{"taurus5", "taurus6", "taurus7"},
		Hostnames:         []string{"taurus5", "taurus6", "taurus7"},
		SSHUser:           "benchuser",
		SSHKeyPath:        "/home/u/.ssh/id_ed25519",
		ReplicationFactor: 3,
		Consistency:       ConsistencyLocalQuorum,
		Keyspace:          "uuid_benchmark",
		NumBuckets:        1000,
	}
}

func TestRemoteClusterBasics(t *testing.T) {
	b := NewRemoteCluster(validRemoteCfg())
	if got, want := b.NodeCount(), 3; got != want {
		t.Errorf("NodeCount: got %d want %d", got, want)
	}
	if got, want := b.Mode(), ModeRemoteCluster; got != want {
		t.Errorf("Mode: got %v want %v", got, want)
	}
	want := []string{"taurus5", "taurus6", "taurus7"}
	if got := b.NodeAddresses(); !slices.Equal(got, want) {
		t.Errorf("NodeAddresses: got %v want %v", got, want)
	}
}

func TestRemoteClusterNodeAddressesReturnsCopy(t *testing.T) {
	// Mutation of the returned slice must not corrupt the backend's
	// internal hostname list — the doc on Backend.NodeAddresses doesn't
	// forbid mutation, so the implementation should defensively copy.
	b := NewRemoteCluster(validRemoteCfg())
	addrs := b.NodeAddresses()
	addrs[0] = "evil"
	if b.NodeAddresses()[0] != "taurus5" {
		t.Errorf("NodeAddresses returned an aliased slice; internal state corrupted")
	}
}

func TestRemoteClusterExecOnNodeRejectsBadIndex(t *testing.T) {
	b := NewRemoteCluster(validRemoteCfg())
	if _, err := b.ExecOnNode(3, "true"); err == nil {
		t.Error("expected error for index 3 (out of range [0,3))")
	}
	if _, err := b.ExecOnNode(-1, "true"); err == nil {
		t.Error("expected error for negative index")
	}
}

func TestRemoteClusterExecOnNodeRejectsEmptyArgv(t *testing.T) {
	b := NewRemoteCluster(validRemoteCfg())
	if _, err := b.ExecOnNode(0); err == nil {
		t.Error("expected error for empty argv")
	}
}

func TestRemoteClusterCopyToNodeRejectsBadIndex(t *testing.T) {
	b := NewRemoteCluster(validRemoteCfg())
	if err := b.CopyToNode(3, "/tmp/foo", "/tmp/bar"); err == nil {
		t.Error("expected error for index 3 (out of range [0,3))")
	}
}

func TestNewRemoteClusterPanicsOnEmptyHostnames(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for empty Hostnames")
		}
	}()
	cfg := validRemoteCfg()
	cfg.Hostnames = nil
	_ = NewRemoteCluster(cfg)
}

func TestNewRemoteClusterCopiesHostnames(t *testing.T) {
	// Pins the constructor's defensive copy: mutating the caller's
	// cfg.Hostnames slice after construction must not corrupt the
	// backend's view. (TestRemoteClusterNodeAddressesReturnsCopy
	// pins the read side; this pins the write side.)
	cfg := validRemoteCfg()
	b := NewRemoteCluster(cfg)
	cfg.Hostnames[0] = "evil"
	if got := b.NodeAddresses()[0]; got != "taurus5" {
		t.Errorf("constructor did not defensively copy Hostnames; got %q after caller mutation", got)
	}
}

func TestNewRemoteClusterPanicsOnEmptySSHUser(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for empty SSHUser")
		}
	}()
	cfg := validRemoteCfg()
	cfg.SSHUser = ""
	_ = NewRemoteCluster(cfg)
}
