package cluster

import (
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const sampleNodetoolStatus = `Datacenter: dc1
===============
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address       Load        Tokens  Owns  Host ID                               Rack
UN  172.18.0.2    250 KiB     16      ?     a1b2c3d4-0000-0000-0000-000000000001  rack1
UN  172.18.0.3    248 KiB     16      ?     a1b2c3d4-0000-0000-0000-000000000002  rack1
UN  172.18.0.4    245 KiB     16      ?     a1b2c3d4-0000-0000-0000-000000000003  rack1
`

const sampleNodetoolPartial = `Datacenter: dc1
===============
--  Address       Load   Tokens  Owns  Host ID  Rack
UN  172.18.0.2    250 K  16      ?     a-b-c    rack1
DN  172.18.0.3    0      16      ?     d-e-f    rack1
UJ  172.18.0.4    0      16      ?     g-h-i    rack1
`

func TestParseNodetoolStatus(t *testing.T) {
	t.Run("all UN nodes counted", func(t *testing.T) {
		nodes, err := ParseNodetoolStatus(sampleNodetoolStatus)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got, want := len(nodes), 3; got != want {
			t.Fatalf("got %d nodes want %d", got, want)
		}
		for _, n := range nodes {
			if n.Status != "UN" {
				t.Errorf("node %s: status %q != UN", n.Address, n.Status)
			}
		}
	})
	t.Run("non-UN nodes still parsed but flagged", func(t *testing.T) {
		nodes, err := ParseNodetoolStatus(sampleNodetoolPartial)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got, want := len(nodes), 3; got != want {
			t.Fatalf("got %d nodes want %d", got, want)
		}
		un := 0
		for _, n := range nodes {
			if n.Status == "UN" {
				un++
			}
		}
		if un != 1 {
			t.Errorf("expected 1 UN node, got %d", un)
		}
	})
	t.Run("empty input returns error", func(t *testing.T) {
		// Pins the documented "no nodes found" error so callers can rely on
		// it as the early-poll signal in WaitForRing.
		nodes, err := ParseNodetoolStatus("")
		if err == nil {
			t.Fatalf("expected error, got nodes: %v", nodes)
		}
	})
	t.Run("invalid status code is skipped", func(t *testing.T) {
		// A line with a syntactically valid 2-char status whose letters are
		// not in {U,D} × {N,J,L,M} must be rejected, not parsed as a node.
		// Otherwise stray header text or future status codes would silently
		// inflate the node count.
		const sample = `--  Address       Load   Tokens  Owns  Host ID  Rack
XY  172.18.0.99   1 K    16      ?     bogus    rack1
UN  172.18.0.2    250 K  16      ?     a-b-c    rack1
`
		nodes, err := ParseNodetoolStatus(sample)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got, want := len(nodes), 1; got != want {
			t.Fatalf("got %d nodes want %d (XY line should be skipped)", got, want)
		}
		if nodes[0].Address != "172.18.0.2" {
			t.Errorf("got address %q, want 172.18.0.2", nodes[0].Address)
		}
	})
}

func TestAllNodesUp(t *testing.T) {
	t.Run("all up returns true", func(t *testing.T) {
		nodes, _ := ParseNodetoolStatus(sampleNodetoolStatus)
		if !AllNodesUp(nodes, 3) {
			t.Error("expected AllNodesUp to be true")
		}
	})
	t.Run("expected count not met", func(t *testing.T) {
		nodes, _ := ParseNodetoolStatus(sampleNodetoolStatus)
		if AllNodesUp(nodes, 5) {
			t.Error("expected false when fewer UN nodes than expected")
		}
	})
	t.Run("not all UN", func(t *testing.T) {
		nodes, _ := ParseNodetoolStatus(sampleNodetoolPartial)
		if AllNodesUp(nodes, 3) {
			t.Error("expected false when some nodes not UN")
		}
	})
}

// fakeRingBackend is a minimal Backend implementation that scripts
// ExecOnNode("nodetool", "status") responses per node so WaitForRing's
// all-views logic can be unit-tested without a live cluster. Other
// Backend methods are stubs — they aren't reached by WaitForRing.
type fakeRingBackend struct {
	nodeCount int
	// responses[nodeIdx] is the slice of (output, error) pairs returned
	// from successive ExecOnNode(i, ...) calls; the call index counts up
	// per-node and saturates at the final scripted response (so a stable
	// final state can be expressed as a single-element slice).
	responses [][]fakeRingResp
	// per-node call counters, atomic so the test can also assert call counts.
	calls []atomic.Int32
}

type fakeRingResp struct {
	out string
	err error
}

func newFakeRingBackend(perNode [][]fakeRingResp) *fakeRingBackend {
	return &fakeRingBackend{
		nodeCount: len(perNode),
		responses: perNode,
		calls:     make([]atomic.Int32, len(perNode)),
	}
}

func (f *fakeRingBackend) Start() error        { return nil }
func (f *fakeRingBackend) Stop() error         { return nil }
func (f *fakeRingBackend) WaitForReady() error { return nil }
func (f *fakeRingBackend) ExecOnNode(i int, argv ...string) (string, error) {
	idx := int(f.calls[i].Add(1)) - 1
	resps := f.responses[i]
	if idx >= len(resps) {
		idx = len(resps) - 1
	}
	if len(resps) == 0 {
		return "", fmt.Errorf("no scripted response for node %d", i)
	}
	r := resps[idx]
	return r.out, r.err
}
func (f *fakeRingBackend) CopyToNode(i int, src, dst string) error { return nil }
func (f *fakeRingBackend) NodeAddresses() []string                 { return nil }
func (f *fakeRingBackend) NodeContainerIDs() ([]string, error)     { return nil, nil }
func (f *fakeRingBackend) NodeCount() int                          { return f.nodeCount }
func (f *fakeRingBackend) Mode() Mode                              { return ModeLocalCluster }

func TestWaitForRingAllNodesAgreeSucceeds(t *testing.T) {
	// Three nodes, every node's first poll already shows all 3 UN.
	// WaitForRing should return nil immediately (within one poll).
	f := newFakeRingBackend([][]fakeRingResp{
		{{out: sampleNodetoolStatus}},
		{{out: sampleNodetoolStatus}},
		{{out: sampleNodetoolStatus}},
	})
	if err := WaitForRing(f, 3, 2*time.Second); err != nil {
		t.Fatalf("WaitForRing: unexpected error %v", err)
	}
	// Every node should have been polled at least once.
	for i := 0; i < 3; i++ {
		if got := f.calls[i].Load(); got < 1 {
			t.Errorf("node %d: got %d calls, want >=1", i, got)
		}
	}
}

func TestWaitForRingRejectsSplitView(t *testing.T) {
	// Pins FIX 2: node 0 and node 1 see all 3 UN, but node 2 sees a DN
	// peer. The old single-node poll would have returned success; the
	// all-views poll must time out and the error must name the
	// disagreeing node and include its view.
	f := newFakeRingBackend([][]fakeRingResp{
		{{out: sampleNodetoolStatus}},
		{{out: sampleNodetoolStatus}},
		{{out: sampleNodetoolPartial}}, // persistent split view
	})
	err := WaitForRing(f, 3, 50*time.Millisecond)
	if err == nil {
		t.Fatal("WaitForRing: expected timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "node 2") {
		t.Errorf("error %q should name disagreeing node (2)", err.Error())
	}
	if !strings.Contains(err.Error(), "DN") {
		t.Errorf("error %q should include the disagreeing view (DN status)", err.Error())
	}
}

func TestWaitForRingConvergesAfterTransientDisagreement(t *testing.T) {
	// First poll: node 2 sees a DN peer; subsequent polls: all 3 nodes
	// agree on all-UN. WaitForRing must eventually succeed once views
	// align. Use a generous deadline so the 3s sleep between polls fits.
	f := newFakeRingBackend([][]fakeRingResp{
		{{out: sampleNodetoolStatus}},
		{{out: sampleNodetoolStatus}},
		{
			{out: sampleNodetoolPartial}, // first poll: split view
			{out: sampleNodetoolStatus},  // second poll onward: converged
		},
	})
	if err := WaitForRing(f, 3, 10*time.Second); err != nil {
		t.Fatalf("WaitForRing: unexpected error %v", err)
	}
}

func TestWaitForRingToleratesPerNodeError(t *testing.T) {
	// Pins the "tolerate transient errors" contract: node 2 always errors
	// (e.g. SSH not yet reachable). WaitForRing should keep polling until
	// the budget elapses; the timeout error must surface the persistent
	// node-2 error so the operator can debug it (rather than just "ring
	// did not stabilize").
	f := newFakeRingBackend([][]fakeRingResp{
		{{out: sampleNodetoolStatus}},
		{{out: sampleNodetoolStatus}},
		{{err: fmt.Errorf("ssh: connection refused")}},
	})
	err := WaitForRing(f, 3, 50*time.Millisecond)
	if err == nil {
		t.Fatal("WaitForRing: expected timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "node 2") {
		t.Errorf("error %q should name the offending node (2)", err.Error())
	}
	if !strings.Contains(err.Error(), "connection refused") {
		t.Errorf("error %q should include the underlying ssh error", err.Error())
	}
}
