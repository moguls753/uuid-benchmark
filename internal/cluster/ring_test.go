package cluster

import "testing"

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
