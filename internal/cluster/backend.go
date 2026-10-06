package cluster

// Backend abstracts the lifecycle and node access for a Cassandra deployment,
// whether it's a local single container, a local multi-container compose
// stack, or remote SSH-managed nodes. Implementations live in sibling files
// (local_single.go, local_cluster.go, remote_cluster.go) added in later
// tasks.
//
// Methods group into three concerns: lifecycle (Start/Stop/WaitForReady),
// per-node access (ExecOnNode/CopyToNode), and metadata (NodeAddresses/
// NodeContainerIDs/NodeCount/Mode).
type Backend interface {
	// Start brings the cluster up. Returns once the bringup command exits
	// (e.g. `docker compose up -d`); does NOT block on ring stabilization —
	// call WaitForReady for that. Implementations SHOULD ensure a fresh
	// cluster on every Start to honor the project's "fresh container per
	// UUID type" invariant (see CLAUDE.md): LocalSingle and LocalCluster
	// achieve this by tearing the previous stack down with volumes removed
	// before bringing it up. RemoteCluster (Phase 4) may relax this to
	// "operator-provided clean state" because tearing down and rebuilding
	// a multi-host SSH-managed cluster on every Start is prohibitively
	// expensive — see its docstring for the per-backend contract.
	Start() error

	// Stop tears the cluster down and removes all volumes, leaving no data
	// behind. Combined with Start, this implements the project's "fresh
	// container per UUID type" invariant (see CLAUDE.md).
	Stop() error

	// WaitForReady blocks until the cluster reports healthy. For multi-node
	// backends, this means all nodes UN in nodetool status.
	WaitForReady() error

	// ExecOnNode runs a command inside (or on the host of) the i-th node and
	// returns combined stdout. Args are passed as a varargs argv slice rather
	// than a shell string so callers don't need to worry about quoting; SSH
	// backends should join with spaces internally if their transport requires
	// a single command line. Index is 0-based.
	ExecOnNode(i int, argv ...string) (string, error)

	// CopyToNode copies a local file to the i-th node. Used when a backend
	// needs to deploy helpers; not used by the standard workload (which runs
	// on the orchestrator in multi-node modes). Index is 0-based.
	CopyToNode(i int, src, dst string) error

	// NodeAddresses returns the gocql contact points reachable from the
	// orchestrator. Each entry is a bare hostname or IP (e.g. "taurus5",
	// "127.0.0.1") and gocql applies the default CQL port 9042. Explicit
	// "host:port" form is also accepted by gocql for non-default ports.
	//
	// For backends where every node is independently reachable (LocalSingle,
	// RemoteCluster) this is one entry per node; for backends where only a
	// subset is reachable from the orchestrator (LocalCluster: only the seed
	// publishes 9042 to the host; gocql routes through it as coordinator and
	// gossip handles internal replication) this may be shorter than
	// NodeCount().
	NodeAddresses() []string

	// NodeContainerIDs returns the docker container ID on each node's host,
	// used for cgroup-v2 IO metrics collection. Returns an error if any
	// container ID cannot be discovered; on success every entry is a valid
	// non-empty container ID. The current planned backends (LocalSingle,
	// LocalCluster, RemoteCluster) are all container-backed.
	NodeContainerIDs() ([]string, error)

	// NodeCount returns the number of nodes managed by this backend. May
	// exceed len(NodeAddresses()) when only a subset of nodes is reachable
	// as a gocql contact point — see NodeAddresses for the rationale.
	NodeCount() int

	// Mode returns the cluster mode this backend implements. Returns the
	// same Mode the backend was constructed with; provided so callers that
	// hold a Backend without the originating ClusterConfig can still branch.
	Mode() Mode
}
