// Package cluster defines the configuration types and backend abstractions
// shared by the multi-node Cassandra benchmark runner. ClusterConfig captures
// "which Cassandra are we talking to and how" — single-node container, local
// 3-container cluster, or 3-machine remote cluster managed via SSH.
package cluster

import (
	"errors"
	"fmt"
)

// Mode names the deployment topology a ClusterConfig describes.
type Mode string

const (
	// ModeLocalSingle: existing single-node container, exposes one CQL port.
	ModeLocalSingle Mode = "local-single"
	// ModeLocalCluster: 3-container compose cluster on the host, only the seed
	// container publishes its CQL port; gossip discovers the other two
	// internally.
	ModeLocalCluster Mode = "local-cluster"
	// ModeRemoteCluster: one Cassandra container per remote host, managed via
	// SSH from the orchestrator. Each host's CQL port is reachable directly.
	ModeRemoteCluster Mode = "remote-cluster"
)

// Consistency names a CQL consistency level supported by this benchmark. The
// values map to gocql's consistency constants via parseConsistency in the
// cassandra package; the typed alias gives compile-time safety at call sites
// that use the named constants below, and Validate() rejects unknown strings.
type Consistency string

const (
	ConsistencyOne         Consistency = "one"
	ConsistencyLocalOne    Consistency = "local_one"
	ConsistencyLocalQuorum Consistency = "local_quorum"
	ConsistencyQuorum      Consistency = "quorum"
)

// ClusterConfig parameterises a Cassandra benchmark run across the three
// supported deployment topologies. ContactPoints is the seed list passed to
// gocql; for remote clusters Hostnames typically equals ContactPoints. SSH*
// fields are only consulted in ModeRemoteCluster.
type ClusterConfig struct {
	Mode              Mode
	ContactPoints     []string    // hostnames or IPs to pass to gocql
	Hostnames         []string    // SSH hostnames (RemoteCluster only); equal to ContactPoints when on private DNS
	SSHUser           string      // RemoteCluster only
	SSHKeyPath        string      // RemoteCluster only; optional — empty falls back to ssh-agent or default key locations
	ReplicationFactor int
	Consistency       Consistency // CQL consistency level; one of the Consistency* constants
	Keyspace          string
	NumBuckets        int // number of partition buckets for the bench table; controls Cassandra distribution granularity
}

// DefaultLocalSingle returns a sensible single-node baseline for the paper-
// extension benchmark: one local container, RF=1, LOCAL_ONE consistency, and
// the bucketed schema's default partition count (N=1000). Note this is NOT
// the thesis's degenerate `bucket=1` configuration — it is the paper-
// extension's single-node baseline, intentionally bucketed to be directly
// comparable to multi-node runs under the same schema.
func DefaultLocalSingle() ClusterConfig {
	return ClusterConfig{
		Mode:              ModeLocalSingle,
		ContactPoints:     []string{"127.0.0.1"},
		ReplicationFactor: 1,
		Consistency:       ConsistencyLocalOne,
		Keyspace:          "uuid_benchmark",
		NumBuckets:        1000,
	}
}

// Validate returns nil if the configuration is internally consistent, or a
// descriptive error otherwise.
//
// Mode-independent checks (Mode, ContactPoints, RF, Keyspace, Consistency,
// NumBuckets) fire for any mode. The RF-vs-host-count check is mode-
// conditional: only ModeRemoteCluster enforces RF <= len(Hostnames), because
// LocalSingle and LocalCluster expose one published seed regardless of how
// many nodes live behind it.
func (c ClusterConfig) Validate() error {
	switch c.Mode {
	case ModeLocalSingle, ModeLocalCluster, ModeRemoteCluster:
		// ok
	default:
		return fmt.Errorf("unknown mode %q (expected one of: %s, %s, %s)",
			c.Mode, ModeLocalSingle, ModeLocalCluster, ModeRemoteCluster)
	}
	if len(c.ContactPoints) == 0 {
		return errors.New("at least one contact point required")
	}
	if c.ReplicationFactor < 1 {
		return errors.New("replication factor must be >= 1")
	}
	if c.Keyspace == "" {
		return errors.New("keyspace required")
	}
	switch c.Consistency {
	case ConsistencyOne, ConsistencyLocalOne, ConsistencyLocalQuorum, ConsistencyQuorum:
		// ok
	default:
		return fmt.Errorf("unknown consistency %q (expected one of: %s, %s, %s, %s)",
			c.Consistency, ConsistencyOne, ConsistencyLocalOne, ConsistencyLocalQuorum, ConsistencyQuorum)
	}
	if c.NumBuckets < 1 {
		return errors.New("num buckets must be >= 1")
	}
	if c.Mode == ModeRemoteCluster {
		if c.SSHUser == "" {
			return errors.New("SSH user required for remote cluster")
		}
		if len(c.Hostnames) == 0 {
			return errors.New("SSH hostnames required for remote cluster")
		}
		// Reject < 2 hostnames: a single-host "cluster" is almost always
		// a misconfiguration (trailing-comma split, etc.) and silently
		// degrades to a single-node deployment without the operator
		// knowing. Point them at local-single explicitly.
		if len(c.Hostnames) < 2 {
			return errors.New("remote-cluster requires at least 2 hostnames; for a single host use -cluster-mode=local-single")
		}
		// Reject empty hostnames in the slice — usually a trailing or
		// double comma in the -nodes flag. Without this guard, the
		// downstream failure is a vague "remote.Exec: host is empty"
		// with no positional context.
		for i, h := range c.Hostnames {
			if h == "" {
				return fmt.Errorf("hostnames[%d] is empty", i)
			}
		}
		// Reject duplicates — two services named "cassandra" cannot
		// coexist on the same docker daemon, and the resulting "container
		// name in use" failure mid-Start is opaque without this check.
		seen := make(map[string]struct{}, len(c.Hostnames))
		for _, h := range c.Hostnames {
			if _, dup := seen[h]; dup {
				return fmt.Errorf("duplicate hostname %q", h)
			}
			seen[h] = struct{}{}
		}
		if c.ReplicationFactor > len(c.Hostnames) {
			return fmt.Errorf("replication factor %d exceeds host count %d", c.ReplicationFactor, len(c.Hostnames))
		}
	}
	return nil
}
