// Package cluster defines the configuration types and backend abstractions
// shared by the multi-node Cassandra benchmark runner. ClusterConfig captures
// "which Cassandra are we talking to and how" — single-node container, local
// 3-container cluster, or 3-machine remote cluster managed via SSH.
package cluster

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// parseMemoryQuantity converts strings like "8G", "512M", "2g", "1024k"
// to a byte count. Empty string returns 0 with no error (callers
// interpret 0 as "use default"). Recognised suffixes are case-
// insensitive K/M/G/T (1024-based). A bare integer is treated as bytes.
// Used in ClusterConfig.Validate to reject malformed or inverted
// heap/newGen pairs before they reach Cassandra.
func parseMemoryQuantity(s string) (int64, error) {
	if s == "" {
		return 0, nil
	}
	mult := int64(1)
	num := s
	switch s[len(s)-1] {
	case 'K', 'k':
		mult = 1024
		num = s[:len(s)-1]
	case 'M', 'm':
		mult = 1024 * 1024
		num = s[:len(s)-1]
	case 'G', 'g':
		mult = 1024 * 1024 * 1024
		num = s[:len(s)-1]
	case 'T', 't':
		mult = 1024 * 1024 * 1024 * 1024
		num = s[:len(s)-1]
	}
	n, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid quantity %q (expected e.g. 8G, 512M, or a bare byte count)", s)
	}
	if n <= 0 {
		return 0, fmt.Errorf("quantity %q must be positive", s)
	}
	return n * mult, nil
}

// validateCPUString accepts an empty string (use default) or any
// positive decimal number. Docker rejects --cpus values > host CPU count
// at run-time; we can't validate against the remote host's CPU count
// here, so we only catch syntactic garbage.
func validateCPUString(s string) error {
	if s == "" {
		return nil
	}
	n, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return fmt.Errorf("invalid cpu value %q (expected a number, e.g. 8 or 2.5)", s)
	}
	if n <= 0 {
		return fmt.Errorf("cpu value %q must be positive", s)
	}
	return nil
}

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

	// Cassandra-process resource sizing. Currently only consumed by
	// RemoteClusterBackend (LocalSingle and LocalCluster pin their own
	// sizing in the docker-compose files). All four are strings rather
	// than integers because they're passed verbatim to docker (--cpus,
	// --memory) and to Cassandra's env (MAX_HEAP_SIZE, HEAP_NEWSIZE),
	// which accept their own suffix conventions ("8G", "32g", "2"). An
	// empty value means "use the RemoteCluster default" (Taurus-sized:
	// heap=8G, newGen=2G, cpus=8, memory=32g) — keeps the test fixtures
	// and the local-cluster path from having to set every field.
	CassandraHeap   string // MAX_HEAP_SIZE
	CassandraNewGen string // HEAP_NEWSIZE (must be ≤ heap or Cassandra refuses to start)
	CassandraCPUs   string // docker run --cpus (strict: rejected if > host cpu count)
	CassandraMemory string // docker run --memory (soft: docker accepts even > host RAM)
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
		// Resource fields: where provided, must parse. Empty falls
		// through to the RemoteCluster defaults. If both heap and newGen
		// parse, reject inversion before Cassandra does — Cassandra's
		// own failure mode for HEAP_NEWSIZE > MAX_HEAP_SIZE is opaque
		// (silent crash mid-startup with no diagnostic in the docker log).
		heapBytes, herr := parseMemoryQuantity(c.CassandraHeap)
		if herr != nil {
			return fmt.Errorf("cassandra-heap: %w", herr)
		}
		newGenBytes, nerr := parseMemoryQuantity(c.CassandraNewGen)
		if nerr != nil {
			return fmt.Errorf("cassandra-newgen: %w", nerr)
		}
		if _, merr := parseMemoryQuantity(c.CassandraMemory); merr != nil {
			return fmt.Errorf("cassandra-memory: %w", merr)
		}
		if heapBytes > 0 && newGenBytes > 0 && newGenBytes > heapBytes {
			return fmt.Errorf("cassandra-newgen (%s) exceeds cassandra-heap (%s) — Cassandra requires HEAP_NEWSIZE <= MAX_HEAP_SIZE",
				c.CassandraNewGen, c.CassandraHeap)
		}
		if err := validateCPUString(c.CassandraCPUs); err != nil {
			return fmt.Errorf("cassandra-cpus: %w", err)
		}
	}
	return nil
}
