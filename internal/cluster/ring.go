package cluster

import (
	"fmt"
	"strings"
	"time"
)

// RingNode is a parsed line from `nodetool status`.
//
// Status is the 2-character code (e.g. "UN", "DN", "UJ"): the first byte is
// "U"p or "D"own; the second is "N"ormal, "J"oining, "L"eaving, or "M"oving.
// "UN" means the node is fully ready to serve queries.
type RingNode struct {
	Status  string
	Address string
}

// ParseNodetoolStatus extracts the per-node status/address pairs from
// `nodetool status` output. Header, separator, and datacenter lines are
// skipped; only lines beginning with a valid 2-char status code are
// retained. Returns an error if no node lines are found (caller likely
// invoked nodetool too early).
//
// Single-DC only: a multi-DC `nodetool status` output emits a separate
// `Datacenter:` block per DC and this parser flattens them into one list.
// The validation cluster and Taurus deployment are both single-DC (dc1),
// so flattening is correct today; revisit if the benchmark grows multi-DC.
func ParseNodetoolStatus(output string) ([]RingNode, error) {
	var nodes []RingNode
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		st := fields[0]
		if len(st) != 2 {
			continue
		}
		first := st[0]
		if first != 'U' && first != 'D' {
			continue
		}
		switch st[1] {
		case 'N', 'J', 'L', 'M':
		default:
			continue
		}
		nodes = append(nodes, RingNode{Status: st, Address: fields[1]})
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no nodes found in nodetool output")
	}
	return nodes, nil
}

// AllNodesUp reports whether the parsed ring has exactly `expected` nodes,
// all in "UN" state. The strict equality (rather than `>= expected`) catches
// the case where an unexpected extra node has joined.
//
// Examples (expected=3):
//
//	[UN, UN, UN]      → true
//	[UN, UN, DN]      → false (one node down)
//	[UN, UN, UN, DN]  → false (extra unexpected node, even though 3 UN)
//	[UN, UN]          → false (too few)
func AllNodesUp(nodes []RingNode, expected int) bool {
	un := 0
	for _, n := range nodes {
		if n.Status == "UN" {
			un++
		}
	}
	return un == expected && len(nodes) == expected
}

// WaitForRing polls `nodetool status` on EVERY node (via the backend) until
// all nodes' views agree that all `expected` nodes are UN, or the timeout
// elapses. Polling all nodes — not just node 0 — guards against the
// split-view bootstrap window in which one node has converged on the full
// ring while another still sees a peer as DN: a one-node poll would return
// success prematurely and gocql would then hit "host down" retries.
//
// Cost: N× the SSH/exec calls per poll attempt vs the old node-0-only
// scheme. This only fires during the ring-formation window (a few minutes
// at scenario start) so the absolute cost is bounded; the trade is worth
// it for cross-machine first-run reliability. The 3-second poll cadence is
// unchanged.
//
// Per-poll exec/parse errors on any node are tolerated during normal
// bootstrap (the seed may not yet accept nodetool, gossip may not have
// converged), but the most recent such diagnostic — including which node
// disagreed when applicable — is captured and included in the timeout
// failure so the operator can tell whether the cluster never converged or
// the backend itself was misconfigured.
func WaitForRing(b Backend, expected int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastDiag string
	for time.Now().Before(deadline) {
		ok, diag := allNodesAgreeAllUp(b, expected)
		if ok {
			return nil
		}
		lastDiag = diag
		time.Sleep(3 * time.Second)
	}
	if lastDiag != "" {
		return fmt.Errorf("ring did not stabilize within %s (expected %d UN nodes); last diagnostic: %s", timeout, expected, lastDiag)
	}
	return fmt.Errorf("ring did not stabilize within %s (expected %d UN nodes)", timeout, expected)
}

// allNodesAgreeAllUp asks every node for its view of the ring (via
// nodetool status) and reports true only when every node's view shows
// exactly `expected` nodes, all UN. If any node errors, parses empty,
// disagrees on the count, or sees any non-UN peer, returns false plus a
// short human-readable diagnostic naming the first disagreeing node and
// its view.
func allNodesAgreeAllUp(b Backend, expected int) (bool, string) {
	for i := 0; i < b.NodeCount(); i++ {
		out, err := b.ExecOnNode(i, "nodetool", "status")
		if err != nil {
			return false, fmt.Sprintf("node %d nodetool error: %v", i, err)
		}
		nodes, perr := ParseNodetoolStatus(out)
		if perr != nil {
			return false, fmt.Sprintf("node %d parse error: %v", i, perr)
		}
		if !AllNodesUp(nodes, expected) {
			return false, fmt.Sprintf("node %d view disagrees: %s", i, summarizeNodes(nodes))
		}
	}
	return true, ""
}

// summarizeNodes renders parsed nodes as "UN=2,DN=1 [addrs: 10.0.0.1(UN),
// 10.0.0.2(UN), 10.0.0.3(DN)]" so an operator can tell at a glance which
// peers a given node sees as down.
func summarizeNodes(nodes []RingNode) string {
	counts := map[string]int{}
	parts := make([]string, 0, len(nodes))
	for _, n := range nodes {
		counts[n.Status]++
		parts = append(parts, fmt.Sprintf("%s(%s)", n.Address, n.Status))
	}
	statuses := make([]string, 0, len(counts))
	for st, n := range counts {
		statuses = append(statuses, fmt.Sprintf("%s=%d", st, n))
	}
	return fmt.Sprintf("%s [addrs: %s]", strings.Join(statuses, ","), strings.Join(parts, ", "))
}
