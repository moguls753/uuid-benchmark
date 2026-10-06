package cluster

import "testing"

func TestLocalSingleImplementsBackend(t *testing.T) {
	var _ Backend = (*LocalSingleBackend)(nil)
}

func TestLocalSingleNodeBasics(t *testing.T) {
	b := NewLocalSingle()
	if got, want := b.NodeCount(), 1; got != want {
		t.Errorf("NodeCount: got %d want %d", got, want)
	}
	if got, want := b.Mode(), ModeLocalSingle; got != want {
		t.Errorf("Mode: got %v want %v", got, want)
	}
	addrs := b.NodeAddresses()
	if len(addrs) != 1 || addrs[0] != "127.0.0.1" {
		t.Errorf("NodeAddresses: got %v want [127.0.0.1]", addrs)
	}
}

func TestLocalSingleExecOnNodeRejectsBadIndex(t *testing.T) {
	b := NewLocalSingle()
	if _, err := b.ExecOnNode(1, "true"); err == nil {
		t.Error("expected error for index 1")
	}
	if _, err := b.ExecOnNode(-1, "true"); err == nil {
		t.Error("expected error for negative index")
	}
}

func TestLocalSingleCopyToNodeRejectsBadIndex(t *testing.T) {
	b := NewLocalSingle()
	if err := b.CopyToNode(1, "/tmp/foo", "/tmp/bar"); err == nil {
		t.Error("expected error for index 1")
	}
}

func TestLocalSingleExecOnNodeRejectsEmptyArgv(t *testing.T) {
	b := NewLocalSingle()
	if _, err := b.ExecOnNode(0); err == nil {
		t.Error("expected error for empty argv")
	}
}
