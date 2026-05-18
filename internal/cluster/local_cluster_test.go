package cluster

import (
	"slices"
	"testing"
)

func TestLocalClusterImplementsBackend(t *testing.T) {
	var _ Backend = (*LocalClusterBackend)(nil)
}

func TestLocalClusterBasics(t *testing.T) {
	b := NewLocalCluster(3)
	if got, want := b.NodeCount(), 3; got != want {
		t.Errorf("NodeCount: got %d want %d", got, want)
	}
	if got, want := b.Mode(), ModeLocalCluster; got != want {
		t.Errorf("Mode: got %v want %v", got, want)
	}
	// Only the seed publishes 9042 to the host (see network caveat in plan).
	addrs := b.NodeAddresses()
	if len(addrs) != 1 || addrs[0] != "127.0.0.1" {
		t.Errorf("NodeAddresses: got %v want [127.0.0.1]", addrs)
	}
}

func TestLocalClusterNodeAddressesShorterThanNodeCount(t *testing.T) {
	// Pins the documented LocalCluster contract that NodeAddresses returns
	// fewer entries than NodeCount: only the seed publishes 9042 to the
	// host (the other compose services bind the same port internally and
	// gocql can't reach them through Docker NAT). A future refactor that
	// switches LocalCluster to fan out per-port mappings would silently
	// change the gocql contact list shape; this test will catch it.
	b := NewLocalCluster(3)
	if got := len(b.NodeAddresses()); got != 1 {
		t.Errorf("len(NodeAddresses()) = %d, want 1 (seed only)", got)
	}
	if b.NodeCount() <= len(b.NodeAddresses()) {
		t.Errorf("expected NodeCount(%d) > len(NodeAddresses)(%d) for LocalCluster",
			b.NodeCount(), len(b.NodeAddresses()))
	}
}

func TestLocalClusterContainerNames(t *testing.T) {
	b := NewLocalCluster(3)
	want := []string{"uuid-bench-cassandra-1", "uuid-bench-cassandra-2", "uuid-bench-cassandra-3"}
	if got := b.containerNames(); !slices.Equal(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func TestLocalClusterExecOnNodeRejectsBadIndex(t *testing.T) {
	b := NewLocalCluster(3)
	if _, err := b.ExecOnNode(3, "true"); err == nil {
		t.Error("expected error for index 3 (out of range [0,3))")
	}
	if _, err := b.ExecOnNode(-1, "true"); err == nil {
		t.Error("expected error for negative index")
	}
}

func TestLocalClusterCopyToNodeRejectsBadIndex(t *testing.T) {
	b := NewLocalCluster(3)
	if err := b.CopyToNode(3, "/tmp/foo", "/tmp/bar"); err == nil {
		t.Error("expected error for index 3 (out of range [0,3))")
	}
}

func TestLocalClusterExecOnNodeRejectsEmptyArgv(t *testing.T) {
	b := NewLocalCluster(3)
	if _, err := b.ExecOnNode(0); err == nil {
		t.Error("expected error for empty argv")
	}
}

func TestNewLocalClusterPanicsOnBadNodeCount(t *testing.T) {
	// Pins the constructor's guard: a zero/negative node count would let
	// WaitForRing(b, 0, ...) succeed on the first poll (AllNodesUp on an
	// empty ring with expected=0 is true), masking a "ready 0-node
	// cluster" while `docker compose up` still brings up the compose
	// file's hard-coded service set.
	for _, n := range []int{0, -1, -5} {
		func(n int) {
			defer func() {
				if r := recover(); r == nil {
					t.Errorf("NewLocalCluster(%d): expected panic, did not panic", n)
				}
			}()
			_ = NewLocalCluster(n)
		}(n)
	}
}
