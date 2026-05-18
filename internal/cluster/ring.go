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

// WaitForRing polls `nodetool status` on node 0 (via the backend) until all
// `expected` nodes report UN, or the timeout elapses. The 3-second poll
// cadence and 90s default timeout (set by callers) are tuned to the
// observed convergence time of the local 3-node compose cluster.
//
// Per-poll exec/parse errors are tolerated during normal bootstrap (the
// seed may not yet accept nodetool, gossip may not have converged), but
// the most recent such error is captured and included in the timeout
// failure so a misconfigured backend (wrong container name, bad index)
// surfaces a useful diagnostic rather than just "ring did not stabilize".
func WaitForRing(b Backend, expected int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		out, err := b.ExecOnNode(0, "nodetool", "status")
		if err != nil {
			lastErr = err
		} else {
			nodes, perr := ParseNodetoolStatus(out)
			if perr == nil && AllNodesUp(nodes, expected) {
				return nil
			}
			if perr != nil {
				lastErr = perr
			}
		}
		time.Sleep(3 * time.Second)
	}
	if lastErr != nil {
		return fmt.Errorf("ring did not stabilize within %s (expected %d UN nodes); last error: %w", timeout, expected, lastErr)
	}
	return fmt.Errorf("ring did not stabilize within %s (expected %d UN nodes)", timeout, expected)
}
