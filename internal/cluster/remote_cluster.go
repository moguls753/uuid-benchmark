package cluster

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/remote"
)

// Hardcoded Cassandra config for remote deployments. Matches the
// single-node and local-cluster compose files: same cluster name and
// DC so keyspace replication maps (`'dc1': RF`) are interoperable.
const (
	remoteClusterCassandraName = "UUIDBenchCluster"
	remoteClusterDC            = "dc1"

	// remoteContainerName is the Docker container name used on each
	// Taurus host. Each host runs at most one Cassandra container, so a
	// bare "cassandra" suffices — namespace collisions are impossible
	// across hosts. Single-node and local-cluster modes use longer
	// prefixed names ("uuid-bench-cassandra", "uuid-bench-cassandra-N")
	// because they share a single Docker daemon and need disambiguation.
	remoteContainerName = "cassandra"

	// Stagger node bringup so the seed has time to come up before its
	// peers try to gossip with it. Empirical values; will be refined in
	// Task 7.4 (Taurus dry-run).
	// TODO(7.4): replace these sleeps with WaitForRing-style polling
	// once we know the real Taurus convergence times.
	remoteSeedBootDelay = 45 * time.Second
	remotePeerBootDelay = 15 * time.Second

	// remoteCassandraStartCmd is the container CMD applied at startup
	// via `bash -c`. It overrides the multi-partition unlogged-batch
	// thresholds before launching the Cassandra daemon, mirroring the
	// sed lines in docker/docker-compose.cassandra.yml and
	// docker-compose.cassandra-cluster.yml. The bucketed schema sends
	// ~108 KiB multi-partition batches, which trip the default 50 KiB
	// fail threshold.
	//
	// The `grep -q '^batch_size_fail_threshold' ... || exit 99` guard
	// fails loudly if a future Cassandra release renames the key,
	// instead of silently no-op'ing the sed and reverting to the
	// default 50 KiB.
	remoteCassandraStartCmd = "" +
		"grep -q '^batch_size_fail_threshold' /etc/cassandra/cassandra.yaml || " +
		"{ echo 'batch_size_fail_threshold key missing — Cassandra layout changed' >&2; exit 99; } && " +
		"sed -i 's/^batch_size_fail_threshold:.*/batch_size_fail_threshold: 200KiB/' /etc/cassandra/cassandra.yaml && " +
		"sed -i 's/^batch_size_warn_threshold:.*/batch_size_warn_threshold: 100KiB/' /etc/cassandra/cassandra.yaml && " +
		"exec docker-entrypoint.sh cassandra -f"

	// remoteReadyTimeout is the WaitForReady budget. Generous compared
	// to local-cluster's 8 minutes because the staged-sleep bringup in
	// Start already consumes ~75 s before WaitForReady is called and
	// remote nodes need extra slack for VPN-traversal gossip. Net
	// wall-clock budget (75 s + 6 min) is comparable to LocalCluster's
	// (0 + 8 min).
	remoteReadyTimeout = 6 * time.Minute
)

// RemoteClusterBackend manages a Cassandra cluster across N physical
// hosts via SSH. Each host runs a single `cassandra` Docker container,
// brought up with `docker run` and torn down with `docker rm -f` over
// SSH. The orchestrator never runs Cassandra itself; it only drives
// the remote daemons.
//
// Per Backend.Start's docstring, RemoteCluster relaxes the "fresh
// container per UUID type" invariant to "best-effort teardown then
// fresh bringup": each Start removes any existing container and its
// data volume on every host before launching a new container, so the
// next workload sees an empty database. The teardown step ignores
// errors (typical case: nothing was running).
//
// Not goroutine-safe: a single orchestrator thread is expected to
// drive Start/Stop/exec calls. Concurrent invocations would race on
// the remote docker daemons and on this struct's internal state.
type RemoteClusterBackend struct {
	hostnames []string
	user      string
	ssh       *remote.Client

	// Resource defaults sized for Taurus hardware (32G RAM, 8+ cores per
	// host). Override these in tests or smaller-host runs by mutating
	// the backend struct after NewRemoteCluster returns; the fields are
	// unexported because we don't yet have an external scale-up caller.
	heap   string // CASSANDRA MAX_HEAP_SIZE, e.g. "8G" — Taurus-sized default
	newGen string // CASSANDRA HEAP_NEWSIZE, e.g. "2G" — Taurus-sized default
	cpus   string // docker run --cpus value, e.g. "8"
	memory string // docker run --memory value, e.g. "32g"
}

// NewRemoteCluster builds a backend from a ClusterConfig. Panics if
// Hostnames is empty or SSHUser is unset — those are configuration
// errors the orchestrator can't recover from at runtime. Mirrors
// NewLocalCluster's defensive panic on n <= 0. ClusterConfig.Validate()
// catches these on the standard runner path; the panic guards direct
// callers that bypass Validate.
//
// An empty cfg.SSHKeyPath is INTENTIONAL — remote.NewClient interprets
// it as "fall back to ~/.ssh/id_ed25519 at dial time" (see signer() in
// internal/remote/ssh.go), which matches the usual ssh-agent /
// default-key conventions on the orchestrator host.
func NewRemoteCluster(cfg ClusterConfig) *RemoteClusterBackend {
	if len(cfg.Hostnames) == 0 {
		panic("NewRemoteCluster: cfg.Hostnames is empty")
	}
	if cfg.SSHUser == "" {
		panic("NewRemoteCluster: cfg.SSHUser is empty")
	}
	return &RemoteClusterBackend{
		hostnames: append([]string(nil), cfg.Hostnames...),
		user:      cfg.SSHUser,
		ssh:       remote.NewClient(cfg.SSHUser, cfg.SSHKeyPath),
		heap:      "8G",
		newGen:    "2G",
		cpus:      "8",
		memory:    "32g",
	}
}

// Start brings up a fresh Cassandra container on every host, in seed-
// first order. Each host first has any existing `cassandra` container
// and its `cassandra-data-<host>` volume removed (best-effort: errors
// ignored, typical case is nothing-to-remove), then a new container
// is launched via `docker run -d` with the batch-threshold sed dance
// applied as its CMD (see remoteCassandraStartCmd).
//
// Between launches the function sleeps remoteSeedBootDelay after the
// seed and remotePeerBootDelay after each subsequent peer so gossip
// has time to converge. These are coarse empirical values; Task 7.4
// will replace them with active polling once Taurus convergence times
// are measured.
func (b *RemoteClusterBackend) Start() error {
	seed := b.hostnames[0]
	started := make([]string, 0, len(b.hostnames))
	rollback := func() {
		// Best-effort cleanup of partially-started hosts. Errors are
		// silenced — we're already returning a Start error, the user
		// doesn't need a second teardown failure to debug.
		for _, host := range started {
			_, _ = b.ssh.Exec(host, "docker", "rm", "-f", remoteContainerName)
			_, _ = b.ssh.Exec(host, "docker", "volume", "rm", "cassandra-data-"+host)
		}
	}
	for i, host := range b.hostnames {
		// Best-effort teardown — ignore errors (typical: nothing was running).
		_, _ = b.ssh.Exec(host, "docker", "rm", "-f", remoteContainerName)
		_, _ = b.ssh.Exec(host, "docker", "volume", "rm", "cassandra-data-"+host)

		runArgs := []string{
			"docker", "run", "-d",
			"--name", remoteContainerName,
			"-e", "CASSANDRA_SEEDS=" + seed,
			"-e", "CASSANDRA_CLUSTER_NAME=" + remoteClusterCassandraName,
			"-e", "CASSANDRA_DC=" + remoteClusterDC,
			"-e", "CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch",
			"-e", "MAX_HEAP_SIZE=" + b.heap,
			"-e", "HEAP_NEWSIZE=" + b.newGen,
			"-p", "9042:9042",
			"-p", "7000:7000",
			"-p", "7001:7001",
			"-p", "7199:7199",
			"-v", "cassandra-data-" + host + ":/var/lib/cassandra",
			"--cpus", b.cpus,
			"--memory", b.memory,
			"cassandra:5",
			// CMD override: apply the batch-threshold sed dance, then
			// exec the standard entrypoint. See remoteCassandraStartCmd.
			"bash", "-c", remoteCassandraStartCmd,
		}
		if out, err := b.ssh.Exec(host, runArgs...); err != nil {
			rollback()
			return fmt.Errorf("start node %d (%s): %w (output: %s)", i, host, err, strings.TrimSpace(out))
		}
		started = append(started, host)
		// TODO(7.4): replace these sleeps with WaitForRing-style polling
		// once Taurus convergence times are known.
		if i == 0 {
			time.Sleep(remoteSeedBootDelay)
		} else {
			time.Sleep(remotePeerBootDelay)
		}
	}
	return nil
}

// Stop removes the cassandra container and its data volume on every
// host. Always attempts every host so a single offline node can't
// prevent cleanup of the rest. All per-host errors are joined via
// errors.Join so callers can inspect every failure (not just the
// first) — important when one node is genuinely unreachable and the
// operator needs to know which.
func (b *RemoteClusterBackend) Stop() error {
	var errs []error
	for _, host := range b.hostnames {
		if _, err := b.ssh.Exec(host, "docker", "rm", "-f", remoteContainerName); err != nil {
			errs = append(errs, fmt.Errorf("rm container on %s: %w", host, err))
		}
		if _, err := b.ssh.Exec(host, "docker", "volume", "rm", "cassandra-data-"+host); err != nil {
			errs = append(errs, fmt.Errorf("rm volume on %s: %w", host, err))
		}
	}
	return errors.Join(errs...)
}

// WaitForReady delegates to WaitForRing, which polls `nodetool status`
// on node 0 (via ExecOnNode) until all hostnames report UN.
func (b *RemoteClusterBackend) WaitForReady() error {
	return WaitForRing(b, len(b.hostnames), remoteReadyTimeout)
}

// ExecOnNode runs argv inside the cassandra container on the i-th
// host. Wraps the call as `docker exec cassandra <argv...>` over SSH.
func (b *RemoteClusterBackend) ExecOnNode(i int, argv ...string) (string, error) {
	if i < 0 || i >= len(b.hostnames) {
		return "", fmt.Errorf("node index %d out of range [0, %d)", i, len(b.hostnames))
	}
	if len(argv) == 0 {
		return "", fmt.Errorf("ExecOnNode: argv is empty")
	}
	full := append([]string{"docker", "exec", remoteContainerName}, argv...)
	return b.ssh.Exec(b.hostnames[i], full...)
}

// CopyToNode is a two-step transfer: scp the src to /tmp on the
// remote host, then `docker cp` it into the container. The /tmp file
// is not cleaned up — it's small (a workload binary, ~20-30 MB) and
// the host has plenty of room.
func (b *RemoteClusterBackend) CopyToNode(i int, src, dst string) error {
	if i < 0 || i >= len(b.hostnames) {
		return fmt.Errorf("node index %d out of range [0, %d)", i, len(b.hostnames))
	}
	host := b.hostnames[i]
	tmp := "/tmp/" + filepath.Base(src)
	if err := b.ssh.Copy(host, src, tmp); err != nil {
		return fmt.Errorf("scp to %s: %w", host, err)
	}
	if out, err := b.ssh.Exec(host, "docker", "cp", tmp, remoteContainerName+":"+dst); err != nil {
		return fmt.Errorf("docker cp on %s: %w (output: %s)", host, err, strings.TrimSpace(out))
	}
	return nil
}

// NodeAddresses returns a copy of the hostnames so callers can mutate
// the returned slice without affecting the backend's internal state.
// Each entry is a bare hostname (e.g. "taurus5") and gocql applies
// the default CQL port 9042.
func (b *RemoteClusterBackend) NodeAddresses() []string {
	out := make([]string, len(b.hostnames))
	copy(out, b.hostnames)
	return out
}

// NodeContainerIDs returns the long-form Docker container ID on each
// host. Uses `docker inspect --format {{.Id}}` rather than `docker ps`
// because the container name is constant ("cassandra") per remote
// host, making inspect by name the most direct lookup. Errors if any
// host's container ID comes back empty, matching Backend.NodeContainerIDs's
// contract (every returned entry must be a valid non-empty ID).
func (b *RemoteClusterBackend) NodeContainerIDs() ([]string, error) {
	ids := make([]string, len(b.hostnames))
	for i, host := range b.hostnames {
		out, err := b.ssh.Exec(host, "docker", "inspect", "--format", "{{.Id}}", remoteContainerName)
		if err != nil {
			return nil, fmt.Errorf("docker inspect on %s: %w", host, err)
		}
		id := strings.TrimSpace(out)
		if id == "" {
			return nil, fmt.Errorf("no container ID for %s", host)
		}
		ids[i] = id
	}
	return ids, nil
}

func (b *RemoteClusterBackend) NodeCount() int { return len(b.hostnames) }
func (b *RemoteClusterBackend) Mode() Mode     { return ModeRemoteCluster }
