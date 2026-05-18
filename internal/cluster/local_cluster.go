package cluster

import (
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"
)

// localClusterCompose is the path (relative to the orchestrator's working
// directory) to the multi-node Cassandra docker-compose file.
const localClusterCompose = "docker/docker-compose.cassandra-cluster.yml"

// LocalClusterBackend implements Backend for the local 3-container
// Cassandra cluster defined in docker/docker-compose.cassandra-cluster.yml.
//
// Networking caveat: only cassandra-1 publishes 9042 to the host, so the
// gocql driver from the orchestrator connects only to "127.0.0.1" and
// routes through cassandra-1 as coordinator. Gossip-discovered IPs are
// Docker-internal and unreachable from the host; the driver falls back
// to coordinator routing. This backend is for code-correctness
// validation of multi-node code paths (ring formation, multi-host metric
// aggregation), NOT performance measurement. Real perf comes from
// RemoteCluster (Phase 4) where each Cassandra runs on its own host with
// a directly-reachable IP.
//
// Not goroutine-safe: a single orchestrator thread is expected to drive
// Start/Stop/exec calls. Concurrent invocations would race on the docker
// daemon and on this struct's internal state.
type LocalClusterBackend struct {
	nodes int
}

// NewLocalCluster constructs a backend for a cluster of n containers
// (each container's name follows the pattern "uuid-bench-cassandra-<n>"
// defined by the compose file). Panics if n <= 0: a zero-node backend
// would silently claim "ready" because WaitForRing(b, 0, ...) returns
// on the first poll (AllNodesUp([], 0) is true), masking the fact that
// `docker compose up` actually brings up the compose file's hard-coded
// service set regardless of n.
func NewLocalCluster(n int) *LocalClusterBackend {
	if n <= 0 {
		panic(fmt.Sprintf("NewLocalCluster: n must be >= 1, got %d", n))
	}
	return &LocalClusterBackend{nodes: n}
}

// containerNames returns the Docker container names for the cluster in
// index order: ["uuid-bench-cassandra-1", "uuid-bench-cassandra-2", ...].
// Names must match the container_name entries in
// docker/docker-compose.cassandra-cluster.yml.
func (b *LocalClusterBackend) containerNames() []string {
	out := make([]string, b.nodes)
	for i := 0; i < b.nodes; i++ {
		out[i] = fmt.Sprintf("uuid-bench-cassandra-%d", i+1)
	}
	return out
}

// Start brings the whole cluster up. Per the project's "fresh container
// per UUID type" invariant, it first tears down any existing stack with
// volumes removed so each Start delivers an empty cluster. The pre-
// teardown is best-effort: typically nothing is running and `down` exits
// 0 on a clean slate; failures are logged (not fatal) so a stuck
// container that would later collide with `up -d` remains diagnosable.
//
// Start is transactional: on any failure from `docker compose up -d`
// (typically a healthcheck timeout after partial container creation),
// it self-rolls-back by invoking Stop() best-effort before returning
// the error. This mirrors RemoteCluster's rollback() pattern and means
// callers do not need to remember to Stop() after a Start failure —
// the partial cluster is already gone. Stop's own errors during
// rollback are silenced; the original Start error is what the caller
// sees.
func (b *LocalClusterBackend) Start() error {
	if out, err := exec.Command("docker", "compose", "-f", localClusterCompose, "down", "-v", "--remove-orphans").CombinedOutput(); err != nil {
		log.Printf("LocalClusterBackend: pre-Start teardown returned error (continuing): %v; output: %s", err, strings.TrimSpace(string(out)))
	}
	out, err := exec.Command("docker", "compose", "-f", localClusterCompose, "up", "-d").CombinedOutput()
	if err != nil {
		// Self-rollback: tear the partial stack down so a retry sees a
		// clean slate and the operator doesn't need a manual `docker
		// compose down`. Stop errors are intentionally swallowed — the
		// caller is already getting one error (the Start failure) and a
		// second teardown error would obscure the root cause.
		if stopErr := b.Stop(); stopErr != nil {
			log.Printf("LocalClusterBackend: rollback after failed Start also failed (continuing to return original Start error): %v", stopErr)
		}
		return fmt.Errorf("compose up cluster: %w (output: %s)", err, out)
	}
	return nil
}

// Stop tears the cluster down with `docker compose down -v --remove-orphans`,
// removing volumes so the next Start gets a fresh database.
func (b *LocalClusterBackend) Stop() error {
	out, err := exec.Command("docker", "compose", "-f", localClusterCompose, "down", "-v", "--remove-orphans").CombinedOutput()
	if err != nil {
		return fmt.Errorf("compose down cluster: %w (output: %s)", err, out)
	}
	return nil
}

// localClusterReadyTimeout is the WaitForReady budget. Sized larger than
// the compose healthcheck's own worst-case for cassandra-1 alone
// (start_period 30s + 30 retries × 10s interval = 5m 30s) so a slow
// laptop has room for gossip convergence of cassandra-2/3 on top —
// those services depend on cassandra-1's `service_healthy` and only
// start once it's gossiping.
const localClusterReadyTimeout = 8 * time.Minute

// WaitForReady delegates to WaitForRing, which polls `nodetool status`
// on node 0 (via ExecOnNode) until all b.nodes nodes report UN.
func (b *LocalClusterBackend) WaitForReady() error {
	return WaitForRing(b, b.nodes, localClusterReadyTimeout)
}

func (b *LocalClusterBackend) ExecOnNode(i int, argv ...string) (string, error) {
	if i < 0 || i >= b.nodes {
		return "", fmt.Errorf("node index %d out of range [0, %d)", i, b.nodes)
	}
	if len(argv) == 0 {
		return "", fmt.Errorf("ExecOnNode: argv is empty")
	}
	full := append([]string{"exec", b.containerNames()[i]}, argv...)
	out, err := exec.Command("docker", full...).CombinedOutput()
	return string(out), err
}

// CopyToNode wraps `docker cp src container:dst`. Note that Docker uses
// the FIRST `:` in the second arg as the container/path separator, so
// `dst` must not contain a `:` before its leading `/` — passing e.g.
// "weird:path" would be misparsed by Docker itself, not by this wrapper.
func (b *LocalClusterBackend) CopyToNode(i int, src, dst string) error {
	if i < 0 || i >= b.nodes {
		return fmt.Errorf("node index %d out of range [0, %d)", i, b.nodes)
	}
	out, err := exec.Command("docker", "cp", src, fmt.Sprintf("%s:%s", b.containerNames()[i], dst)).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker cp: %w (output: %s)", err, out)
	}
	return nil
}

// NodeAddresses returns the gocql contact points reachable from the
// orchestrator. For LocalCluster this is just the seed (cassandra-1 at
// 127.0.0.1:9042 via the published host port) regardless of NodeCount,
// because the other containers only have Docker-internal IPs that
// gossip would hand to gocql but the host can't route to. The driver
// therefore uses cassandra-1 as a coordinator and Cassandra handles
// internal replication. See the type doc and Backend.NodeAddresses for
// the cross-backend rationale.
func (b *LocalClusterBackend) NodeAddresses() []string {
	return []string{"127.0.0.1"}
}

// NodeContainerIDs returns the long-form Docker container IDs for each
// node, in index order. The filter uses an anchored regex (^name$) per
// container so the substring "uuid-bench-cassandra-1" can't bleed into
// hypothetical longer names like "uuid-bench-cassandra-11". `--no-trunc`
// returns the long ID expected by cgroup-v2 paths in
// internal/benchmark/io/io_metrics.go. Errors if any node's container
// is not currently running, matching Backend.NodeContainerIDs contract
// (every returned entry must be a valid non-empty ID).
func (b *LocalClusterBackend) NodeContainerIDs() ([]string, error) {
	names := b.containerNames()
	ids := make([]string, b.nodes)
	for i, name := range names {
		out, err := exec.Command("docker", "ps",
			"--filter", "name=^"+name+"$",
			"--format", "{{.ID}}",
			"--no-trunc").Output()
		if err != nil {
			return nil, fmt.Errorf("docker ps for %q: %w", name, err)
		}
		id := strings.TrimSpace(string(out))
		if id == "" {
			return nil, fmt.Errorf("no running container named %q", name)
		}
		ids[i] = id
	}
	return ids, nil
}

func (b *LocalClusterBackend) NodeCount() int { return b.nodes }
func (b *LocalClusterBackend) Mode() Mode     { return ModeLocalCluster }
