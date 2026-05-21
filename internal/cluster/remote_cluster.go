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

	// remoteCassandraImage is the canonical image tag, kept in lockstep
	// with docker/docker-compose.cassandra.yml and the cluster compose
	// file. Pulled per host before each docker run (see Start).
	remoteCassandraImage = "cassandra:5"

	// remoteContainerName is the Docker container name used on each
	// Taurus host. Each host runs at most one Cassandra container, so a
	// bare "cassandra" suffices — namespace collisions are impossible
	// across hosts. Single-node and local-cluster modes use longer
	// prefixed names ("uuid-bench-cassandra", "uuid-bench-cassandra-N")
	// because they share a single Docker daemon and need disambiguation.
	remoteContainerName = "cassandra"

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

	// defaultNodeReadyBudget is the per-node deadline for waiting until
	// `nodetool status` on the seed reports the just-started node as UN.
	// Replaces the older gossip-running gate: gossip-running fires the
	// moment Cassandra's gossip Verb handler binds, which is long before
	// the node has finished bootstrap. Using gossip-running as the
	// per-node gate let consecutive non-seed nodes bootstrap concurrently
	// and ended in a Bootstrap Token collision on the seed; using UN
	// instead serialises bootstrap. Budget is generous because UN now
	// covers gossip handshake + bootstrap streaming + token claim.
	defaultNodeReadyBudget = 8 * time.Minute
	defaultPollInterval    = 5 * time.Second

	// remoteReadyTimeout is the WaitForReady budget covering the full
	// ring-convergence step (all nodes reporting UN in nodetool status
	// on the seed). Each node's individual gossip-running poll already
	// completes inside Start before WaitForReady is called, so this
	// budget covers only the additional time the ring takes to agree on
	// peer membership.
	remoteReadyTimeout = 6 * time.Minute
)

// sshExecutor is the slice of the *remote.Client surface that
// RemoteClusterBackend uses. Defined here so tests can substitute a
// fake without needing a live SSH daemon. *remote.Client implements
// this implicitly.
type sshExecutor interface {
	Exec(host string, argv ...string) (string, error)
	Copy(host, src, dst string) error
}

var _ sshExecutor = (*remote.Client)(nil)

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
	ssh       sshExecutor

	// Resource defaults sized for Taurus hardware (32G RAM, 8+ cores per
	// host). Override these in tests or smaller-host runs by mutating
	// the backend struct after NewRemoteCluster returns; the fields are
	// unexported because we don't yet have an external scale-up caller.
	heap   string // CASSANDRA MAX_HEAP_SIZE, e.g. "8G" — Taurus-sized default
	newGen string // CASSANDRA HEAP_NEWSIZE, e.g. "2G" — Taurus-sized default
	cpus   string // docker run --cpus value, e.g. "8"
	memory string // docker run --memory value, e.g. "32g"

	// nodeReadyBudget / pollInterval tune the per-node
	// "wait until nodetool status on the seed reports this node as UN"
	// poll in Start. Exposed as fields (not constants) so tests can
	// shrink them; not exposed as CLI flags because no operator scenario
	// has needed retuning.
	nodeReadyBudget time.Duration
	pollInterval    time.Duration
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
		// Per-field fallback to Taurus-sized defaults if the caller
		// left a field empty. Configurations that come through
		// cmd/benchmark/main.go always populate all four (flag defaults
		// are these same values), so the fallback exists mainly for
		// programmatic / test callers that don't bother to set them.
		heap:            stringOr(cfg.CassandraHeap, "8G"),
		newGen:          stringOr(cfg.CassandraNewGen, "2G"),
		cpus:            stringOr(cfg.CassandraCPUs, "8"),
		memory:          stringOr(cfg.CassandraMemory, "32g"),
		nodeReadyBudget: defaultNodeReadyBudget,
		pollInterval:    defaultPollInterval,
	}
}

// stringOr returns s if non-empty, otherwise fallback. Local helper
// because the standard library's cmp.Or only landed in Go 1.22 and we
// don't want to bump the module's minimum just for this.
func stringOr(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// Start brings up a fresh Cassandra container on every host, in
// seed-first order. For each host the sequence is:
//
//  1. Best-effort `docker rm -f cassandra` and `docker volume rm` so
//     stale state from a previous run cannot leak into this one.
//  2. `docker pull cassandra:5` so an outdated or missing image on the
//     host fails loudly here (with a clear error) instead of mid-run.
//  3. `docker run -d` with the batch-threshold sed CMD (see
//     remoteCassandraStartCmd) and CASSANDRA_BROADCAST_ADDRESS set to
//     the host's hostname.
//  4. Poll `nodetool status` on the seed every pollInterval until the
//     just-started host shows as UN, or fail with the seed's last output
//     if nodeReadyBudget elapses. UN — not gossip-running — is the
//     correct gate: gossip-running fires the moment Cassandra's gossip
//     Verb handler binds, long before bootstrap completes. Using
//     gossip-running let consecutive non-seed nodes bootstrap
//     concurrently, which ended in a Bootstrap Token collision on the
//     seed and a permanently wedged ring. The "did this node finish
//     joining" question is exactly what the seed's `nodetool status`
//     answers, so we wait on that.
//
// CASSANDRA_BROADCAST_ADDRESS is the structural fix that makes
// cross-host gossip work: without it, each container advertises its
// Docker bridge IP (e.g. 172.17.0.2) to peers, which is unroutable
// from other hosts and silently breaks ring formation. We set it to
// the host's hostname (the same identifier the caller passed in
// `-nodes`), relying on the Cassandra container's DNS to resolve it
// to the host's externally-routable IP. On HPC clusters with shared
// /etc/hosts or internal DNS this works out of the box. If a target
// environment doesn't have hostname resolution inside the container,
// the escape hatch is to resolve the hostname on the remote host
// (e.g. via `b.ssh.Exec(host, "getent", "hosts", host)`, which uses
// that host's resolver — the same view the container inherits) and
// pass the resulting IP here instead. Not done now because no
// environment we target requires it.
//
// On any failure for any host, the partial set of hosts that were
// successfully started is rolled back before returning.
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

		// Pull the image first so a missing-image failure surfaces here
		// (with a clean error) rather than as a vague `docker run` error.
		if out, err := b.ssh.Exec(host, "docker", "pull", remoteCassandraImage); err != nil {
			rollback()
			return fmt.Errorf("pull image on node %d (%s): %w (output: %s)", i, host, err, strings.TrimSpace(out))
		}

		runArgs := []string{
			"docker", "run", "-d",
			"--name", remoteContainerName,
			"-e", "CASSANDRA_SEEDS=" + seed,
			"-e", "CASSANDRA_CLUSTER_NAME=" + remoteClusterCassandraName,
			"-e", "CASSANDRA_DC=" + remoteClusterDC,
			"-e", "CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch",
			// Without this, peer-to-peer gossip uses the container's
			// internal Docker bridge IP — unroutable cross-host. See
			// the long comment on Start for the rationale.
			"-e", "CASSANDRA_BROADCAST_ADDRESS=" + host,
			"-e", "MAX_HEAP_SIZE=" + b.heap,
			"-e", "HEAP_NEWSIZE=" + b.newGen,
			"-p", "9042:9042",
			"-p", "7000:7000",
			"-p", "7001:7001",
			"-p", "7199:7199",
			"-v", "cassandra-data-" + host + ":/var/lib/cassandra",
			"--cpus", b.cpus,
			"--memory", b.memory,
			// memlock=-1 ⇒ unlimited mlock budget for the container.
			// Cassandra mlocks its heap to avoid the JVM heap being
			// swapped out (a well-known Cassandra anti-pattern that
			// causes long GC stalls). Without this, every node logs
			// "Unable to lock JVM memory (ENOMEM)" at startup and the
			// heap is eligible for swap, which is especially bad on
			// memory-constrained or oversubscribed hosts.
			"--ulimit", "memlock=-1:-1",
			remoteCassandraImage,
			// CMD override: apply the batch-threshold sed dance, then
			// exec the standard entrypoint. See remoteCassandraStartCmd.
			"bash", "-c", remoteCassandraStartCmd,
		}
		if out, err := b.ssh.Exec(host, runArgs...); err != nil {
			rollback()
			return fmt.Errorf("start node %d (%s): %w (output: %s)", i, host, err, strings.TrimSpace(out))
		}
		started = append(started, host)
		if err := b.waitForNodeUN(seed, host); err != nil {
			rollback()
			return fmt.Errorf("wait UN for node %d (%s): %w", i, host, err)
		}
		// UN-on-seed precedes the joining node opening its own CQL
		// listener — the seed sees the gossip state flip slightly
		// before the joining node binds 9042. Without this second
		// gate, gocql can race the bind and hit "connection refused"
		// on the host that just turned UN, and the IO-metric collector
		// can race the cgroup setup on the same host. See
		// [[project-io-metric-empty-container-id-bug]] for the
		// downstream effect.
		if err := b.waitForNativeTransport(host); err != nil {
			rollback()
			return fmt.Errorf("wait native transport for node %d (%s): %w", i, host, err)
		}
	}
	return nil
}

// waitForNativeTransport polls `nodetool statusbinary` on the target
// host until it reports "running", or nodeReadyBudget elapses.
//
// Distinct from waitForNodeUN: that one asks the seed whether the
// joining node has finished bootstrap (a ring-membership question).
// This one asks the joining node directly whether its CQL listener is
// bound (a client-reachability question). The two events are usually
// milliseconds apart on a healthy host but can be seconds apart on a
// slow host — long enough that a gocql dial scheduled right after
// waitForNodeUN can lose the race and surface as "connection refused".
//
// Transient errors and any non-"running" body are treated as "not
// ready, keep polling" — same convention as waitForNodeUN.
func (b *RemoteClusterBackend) waitForNativeTransport(host string) error {
	deadline := time.Now().Add(b.nodeReadyBudget)
	var lastOut string
	var lastErr error
	for {
		out, err := b.ssh.Exec(host, "docker", "exec", remoteContainerName, "nodetool", "statusbinary")
		// `nodetool statusbinary` prints exactly one of "running" or
		// "not running" (single line, no trailing punctuation). Match
		// the trimmed-and-lowered output equally, NOT via substring —
		// "not running" contains "running".
		if err == nil && strings.EqualFold(strings.TrimSpace(out), "running") {
			return nil
		}
		lastOut, lastErr = out, err
		if time.Now().After(deadline) {
			return fmt.Errorf("node %s native transport did not report running within %s (last err: %v, last output: %s)",
				host, b.nodeReadyBudget, lastErr, strings.TrimSpace(lastOut))
		}
		time.Sleep(b.pollInterval)
	}
}

// waitForNodeUN polls `nodetool status` on the seed until the target
// host appears with status "UN" (Up/Normal) — i.e. has finished joining
// the ring — or nodeReadyBudget elapses.
//
// Used as the per-node gate in Start(). The seed is the canonical
// source of truth for "has this peer finished bootstrap?" because it
// receives the gossip state change that flips a joining node from UJ to
// UN. Polling the joining node itself is unreliable: a node typically
// reports itself UN before the seed has acknowledged the transition,
// and starting the NEXT node while the previous one is still
// bootstrapping causes a Bootstrap Token collision (rare in absolute
// terms but consistent on slow hardware where the bootstrap window is
// wide enough to overlap).
//
// During the boot window nodetool can exit non-zero (JMX not yet up) or
// emit transient output that ParseNodetoolStatus rejects — both are
// treated as "not ready, keep polling". On timeout the returned error
// includes the seed's last output so the operator can tell whether
// Cassandra failed to start, SSH itself is unreachable, or the target
// host never joined the ring.
//
// For the seed itself (host == seed) this still works: the seed has no
// peers to wait for, so as soon as Cassandra is up and nodetool can
// talk to it, the seed reports itself UN.
func (b *RemoteClusterBackend) waitForNodeUN(seed, host string) error {
	deadline := time.Now().Add(b.nodeReadyBudget)
	var lastOut string
	var lastErr error
	for {
		out, err := b.ssh.Exec(seed, "docker", "exec", remoteContainerName, "nodetool", "status")
		if err == nil {
			if nodes, perr := ParseNodetoolStatus(out); perr == nil {
				for _, n := range nodes {
					if n.Address == host && n.Status == "UN" {
						return nil
					}
				}
			}
		}
		lastOut, lastErr = out, err
		if time.Now().After(deadline) {
			return fmt.Errorf("node %s did not reach UN within %s (last err: %v, last output: %s)",
				host, b.nodeReadyBudget, lastErr, strings.TrimSpace(lastOut))
		}
		time.Sleep(b.pollInterval)
	}
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
// host. Uses `docker ps --filter name=^cassandra$` rather than
// `docker inspect --format '{{.Id}}'` because Docker 29.x intermittently
// returns exit 0 with empty stdout for inspect's template path when
// the daemon is under load (observed 2026-05-20 on incus VMs during
// Cassandra compaction). `docker ps` is reliable under the same
// conditions. The `^name$` anchor mirrors LocalSingleBackend.NodeContainerIDs
// and prevents substring matches against unrelated containers.
func (b *RemoteClusterBackend) NodeContainerIDs() ([]string, error) {
	ids := make([]string, len(b.hostnames))
	for i, host := range b.hostnames {
		out, err := b.ssh.Exec(host, "docker", "ps", "-q", "--no-trunc", "--filter", "name=^"+remoteContainerName+"$")
		if err != nil {
			return nil, fmt.Errorf("docker ps on %s: %w", host, err)
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
