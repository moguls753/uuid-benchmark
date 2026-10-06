package cluster

import (
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRemoteClusterImplementsBackend(t *testing.T) {
	var _ Backend = (*RemoteClusterBackend)(nil)
}

func validRemoteCfg() ClusterConfig {
	return ClusterConfig{
		Mode:              ModeRemoteCluster,
		ContactPoints:     []string{"taurus5", "taurus6", "taurus7"},
		Hostnames:         []string{"taurus5", "taurus6", "taurus7"},
		SSHUser:           "benchuser",
		SSHKeyPath:        "/home/u/.ssh/id_ed25519",
		ReplicationFactor: 3,
		Consistency:       ConsistencyLocalQuorum,
		Keyspace:          "uuid_benchmark",
		NumBuckets:        1000,
	}
}

func TestRemoteClusterBasics(t *testing.T) {
	b := NewRemoteCluster(validRemoteCfg())
	if got, want := b.NodeCount(), 3; got != want {
		t.Errorf("NodeCount: got %d want %d", got, want)
	}
	if got, want := b.Mode(), ModeRemoteCluster; got != want {
		t.Errorf("Mode: got %v want %v", got, want)
	}
	want := []string{"taurus5", "taurus6", "taurus7"}
	if got := b.NodeAddresses(); !slices.Equal(got, want) {
		t.Errorf("NodeAddresses: got %v want %v", got, want)
	}
}

func TestRemoteClusterNodeAddressesReturnsCopy(t *testing.T) {
	// Mutation of the returned slice must not corrupt the backend's
	// internal hostname list — the doc on Backend.NodeAddresses doesn't
	// forbid mutation, so the implementation should defensively copy.
	b := NewRemoteCluster(validRemoteCfg())
	addrs := b.NodeAddresses()
	addrs[0] = "evil"
	if b.NodeAddresses()[0] != "taurus5" {
		t.Errorf("NodeAddresses returned an aliased slice; internal state corrupted")
	}
}

func TestRemoteClusterExecOnNodeRejectsBadIndex(t *testing.T) {
	b := NewRemoteCluster(validRemoteCfg())
	if _, err := b.ExecOnNode(3, "true"); err == nil {
		t.Error("expected error for index 3 (out of range [0,3))")
	}
	if _, err := b.ExecOnNode(-1, "true"); err == nil {
		t.Error("expected error for negative index")
	}
}

func TestRemoteClusterExecOnNodeRejectsEmptyArgv(t *testing.T) {
	b := NewRemoteCluster(validRemoteCfg())
	if _, err := b.ExecOnNode(0); err == nil {
		t.Error("expected error for empty argv")
	}
}

func TestRemoteClusterCopyToNodeRejectsBadIndex(t *testing.T) {
	b := NewRemoteCluster(validRemoteCfg())
	if err := b.CopyToNode(3, "/tmp/foo", "/tmp/bar"); err == nil {
		t.Error("expected error for index 3 (out of range [0,3))")
	}
}

func TestNewRemoteClusterPanicsOnEmptyHostnames(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for empty Hostnames")
		}
	}()
	cfg := validRemoteCfg()
	cfg.Hostnames = nil
	_ = NewRemoteCluster(cfg)
}

func TestNewRemoteClusterCopiesHostnames(t *testing.T) {
	// Pins the constructor's defensive copy: mutating the caller's
	// cfg.Hostnames slice after construction must not corrupt the
	// backend's view. (TestRemoteClusterNodeAddressesReturnsCopy
	// pins the read side; this pins the write side.)
	cfg := validRemoteCfg()
	b := NewRemoteCluster(cfg)
	cfg.Hostnames[0] = "evil"
	if got := b.NodeAddresses()[0]; got != "taurus5" {
		t.Errorf("constructor did not defensively copy Hostnames; got %q after caller mutation", got)
	}
}

func TestNewRemoteClusterPanicsOnEmptySSHUser(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic for empty SSHUser")
		}
	}()
	cfg := validRemoteCfg()
	cfg.SSHUser = ""
	_ = NewRemoteCluster(cfg)
}

// --- Start() coverage with a fake sshExecutor ----------------------

// fakeSSH records every Exec/Copy call and lets tests script per-call
// responses. The scripting is keyed by (host, command-prefix) so a
// test can say "on taurus5, the third nodetool status should report
// taurus6 as UN". Unmatched calls return ("", nil) — a benign default
// that keeps the Start() happy path moving when a test doesn't care.
//
// `Start()` waits for each just-launched node to reach UN status as
// reported by `nodetool status` on the seed (cluster.RemoteClusterBackend.
// waitForNodeUN). Tests script the seed's status responses to drive
// that wait — either via a popped per-call script
// (`nodetoolStatusResponses`) or a sticky default
// (`nodetoolStatusDefault`).
type fakeSSH struct {
	mu    sync.Mutex
	calls []fakeCall

	// nodetoolStatusResponses[host] is consumed front-to-back on each
	// `docker exec ... nodetool status` call for that host. When the
	// slice is exhausted, the fake falls through to
	// nodetoolStatusDefault[host]; if that is also empty, the call
	// returns ("", nil).
	nodetoolStatusResponses map[string][]fakeResp

	// nodetoolStatusDefault[host] is the sticky reply for any
	// `nodetool status` call on `host` after nodetoolStatusResponses is
	// exhausted. Use this in happy-path tests where every poll should
	// return the same "all nodes UN" view.
	nodetoolStatusDefault map[string]fakeResp

	// statusBinaryResponses[host] / statusBinaryDefault[host] script
	// the per-call and sticky replies for `nodetool statusbinary`,
	// the second per-node readiness gate that Start() consults after
	// waitForNodeUN. If both maps are empty for a host, the fake
	// returns "running" so the default test fixture stays happy.
	statusBinaryResponses map[string][]fakeResp
	statusBinaryDefault   map[string]fakeResp
}

type fakeCall struct {
	host string
	argv []string
}

type fakeResp struct {
	out string
	err error
}

func (f *fakeSSH) Exec(host string, argv ...string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeCall{host: host, argv: append([]string(nil), argv...)})

	// Detect `docker exec <name> nodetool status` and reply from the script.
	if len(argv) >= 5 && argv[0] == "docker" && argv[1] == "exec" && argv[3] == "nodetool" && argv[4] == "status" {
		if resps := f.nodetoolStatusResponses[host]; len(resps) > 0 {
			r := resps[0]
			f.nodetoolStatusResponses[host] = resps[1:]
			return r.out, r.err
		}
		if def, ok := f.nodetoolStatusDefault[host]; ok {
			return def.out, def.err
		}
	}
	// Detect `docker exec <name> nodetool statusbinary`. The default
	// happy-path reply is "running" so Start() proceeds. Tests that want
	// to script the native-transport gate can override via
	// statusBinaryDefault / statusBinaryResponses below.
	if len(argv) >= 5 && argv[0] == "docker" && argv[1] == "exec" && argv[3] == "nodetool" && argv[4] == "statusbinary" {
		if resps := f.statusBinaryResponses[host]; len(resps) > 0 {
			r := resps[0]
			f.statusBinaryResponses[host] = resps[1:]
			return r.out, r.err
		}
		if def, ok := f.statusBinaryDefault[host]; ok {
			return def.out, def.err
		}
		return "running", nil
	}
	return "", nil
}

func (f *fakeSSH) Copy(host, src, dst string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, fakeCall{host: host, argv: []string{"COPY", src, dst}})
	return nil
}

// newRemoteForTest builds a backend wired to the given fake SSH,
// with the gossip-poll cadence shrunk so tests don't sleep for real.
func newRemoteForTest(hosts []string, ssh *fakeSSH) *RemoteClusterBackend {
	b := NewRemoteCluster(ClusterConfig{
		Mode:              ModeRemoteCluster,
		ContactPoints:     hosts,
		Hostnames:         hosts,
		SSHUser:           "benchuser",
		ReplicationFactor: 3,
		Consistency:       ConsistencyLocalQuorum,
		Keyspace:          "uuid_benchmark",
		NumBuckets:        1000,
	})
	b.ssh = ssh
	b.nodeReadyBudget = 200 * time.Millisecond
	b.pollInterval = 5 * time.Millisecond
	return b
}

// nodetoolStatusText builds a synthetic `nodetool status` output where
// every host in `unHosts` is reported as UN. Used in tests to script the
// seed's reply to Start()'s per-node UN poll.
func nodetoolStatusText(unHosts []string) string {
	var b strings.Builder
	b.WriteString("Datacenter: dc1\n")
	b.WriteString("===============\n")
	b.WriteString("Status=Up/Down\n")
	b.WriteString("|/ State=Normal/Leaving/Joining/Moving\n")
	b.WriteString("--  Address  Load  Tokens  Owns  Host ID  Rack\n")
	for i, h := range unHosts {
		fmt.Fprintf(&b, "UN  %s  100KiB  16  100.0%%  host-%d  rack1\n", h, i)
	}
	return b.String()
}

// happyPathNodetoolStatus wires the seed's nodetoolStatusDefault so any
// `nodetool status` poll returns "all hosts UN" — i.e. every just-started
// node is immediately visible as joined. The simplest happy-path setup.
func happyPathNodetoolStatus(seed string, allHosts []string) map[string]fakeResp {
	return map[string]fakeResp{
		seed: {out: nodetoolStatusText(allHosts)},
	}
}

func TestStartSetsBroadcastAddressPerHost(t *testing.T) {
	hosts := []string{"vm1", "vm2", "vm3"}
	ssh := &fakeSSH{nodetoolStatusDefault: happyPathNodetoolStatus("vm1", hosts)}
	b := newRemoteForTest(hosts, ssh)

	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// For each host, find the `docker run` invocation and assert it
	// contains the CASSANDRA_BROADCAST_ADDRESS=<host> env.
	for _, h := range hosts {
		var found bool
		for _, c := range ssh.calls {
			if c.host != h || len(c.argv) < 2 || c.argv[0] != "docker" || c.argv[1] != "run" {
				continue
			}
			want := "CASSANDRA_BROADCAST_ADDRESS=" + h
			if slices.Contains(c.argv, want) {
				found = true
				break
			}
			t.Errorf("docker run on %s missing env %q in argv: %v", h, want, c.argv)
		}
		if !found {
			t.Errorf("no docker run call found for host %s with broadcast address", h)
		}
	}
}

func TestStartPullsImageBeforeRun(t *testing.T) {
	hosts := []string{"vm1", "vm2", "vm3"}
	ssh := &fakeSSH{nodetoolStatusDefault: happyPathNodetoolStatus("vm1", hosts)}
	b := newRemoteForTest(hosts, ssh)

	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// For each host: the index of the first `docker pull cassandra:5`
	// call must be lower than the index of the first `docker run` call.
	for _, h := range hosts {
		pullIdx, runIdx := -1, -1
		for i, c := range ssh.calls {
			if c.host != h || len(c.argv) < 2 || c.argv[0] != "docker" {
				continue
			}
			if pullIdx == -1 && c.argv[1] == "pull" && slices.Contains(c.argv, "cassandra:5") {
				pullIdx = i
			}
			if runIdx == -1 && c.argv[1] == "run" {
				runIdx = i
			}
		}
		if pullIdx == -1 {
			t.Errorf("no `docker pull cassandra:5` recorded for host %s", h)
		}
		if runIdx == -1 {
			t.Errorf("no `docker run` recorded for host %s", h)
		}
		if pullIdx > runIdx {
			t.Errorf("on %s: pull (idx %d) must precede run (idx %d)", h, pullIdx, runIdx)
		}
	}
}

func TestStartPollsUntilNodeReachesUN(t *testing.T) {
	// First two `nodetool status` calls on the seed exit non-zero (the
	// early boot window where Cassandra hasn't opened its JMX port yet);
	// the third returns a ring that includes the freshly started node
	// as UN. Start must succeed by retrying through the boot window.
	//
	// Three hosts ⇒ three per-iteration UN waits; we script the seed to
	// return error twice, then UN three times. The sticky default
	// (all-hosts-UN) catches anything beyond that, so the test is
	// resilient to budget changes.
	hosts := []string{"vm1", "vm2", "vm3"}
	bootErr := fmt.Errorf("nodetool: connection refused (Cassandra still booting)")
	ssh := &fakeSSH{
		nodetoolStatusResponses: map[string][]fakeResp{
			"vm1": {
				{err: bootErr},
				{out: "starting up\n", err: bootErr},
				{out: nodetoolStatusText([]string{"vm1"})},
				{out: nodetoolStatusText([]string{"vm1", "vm2"})},
				{out: nodetoolStatusText(hosts)},
			},
		},
		nodetoolStatusDefault: happyPathNodetoolStatus("vm1", hosts),
	}
	b := newRemoteForTest(hosts, ssh)

	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Sanity: at least the two scripted error responses must have been
	// consumed (i.e. the wait actually retried through the boot window).
	if remaining := len(ssh.nodetoolStatusResponses["vm1"]); remaining > 3 {
		t.Errorf("seed still has %d unconsumed scripted responses; "+
			"Start did not poll through the boot window", remaining)
	}
}

func TestStartFailsWhenNodeNeverReachesUN(t *testing.T) {
	// The fake never returns a parsable status with the new node UN —
	// Start must time out with a host-named error that explains it was
	// waiting on UN. The error must blame the FIRST failing node (vm1,
	// the seed) so the operator can locate the wedge.
	hosts := []string{"vm1", "vm2", "vm3"}
	// Empty maps: unmatched calls return ("", nil); ParseNodetoolStatus
	// rejects that as "no nodes found", so the wait budget elapses
	// without ever seeing vm1 UN.
	ssh := &fakeSSH{}
	b := newRemoteForTest(hosts, ssh)

	err := b.Start()
	if err == nil {
		t.Fatal("Start: expected timeout error, got nil")
	}
	msg := err.Error()
	if !strings.Contains(msg, "vm1") {
		t.Errorf("error %q should name the first failing host vm1", msg)
	}
	if !strings.Contains(msg, "UN") {
		t.Errorf("error %q should mention UN — the operator needs to know what was being waited on", msg)
	}
}

func TestStartRollsBackOnLaterHostFailure(t *testing.T) {
	// vm1 and vm2 reach UN; vm3 never reaches UN. Start must fail and
	// roll back vm1 + vm2 (docker rm -f / volume rm calls recorded).
	//
	// We script the seed's nodetool status to cycle through ring views
	// that include vm1 and vm2 as UN but never vm3. Once both happen-
	// path responses are consumed, the sticky default returns vm1+vm2
	// UN — vm3 never appears, so its UN wait times out.
	hosts := []string{"vm1", "vm2", "vm3"}
	ssh := &fakeSSH{
		nodetoolStatusDefault: map[string]fakeResp{
			"vm1": {out: nodetoolStatusText([]string{"vm1", "vm2"})},
		},
	}
	b := newRemoteForTest(hosts, ssh)

	if err := b.Start(); err == nil {
		t.Fatal("Start: expected error, got nil")
	}

	// After the failure, rollback should have run `docker rm -f cassandra`
	// on vm1 AND vm2 (the successfully-started set). Each host gets one
	// rm-f at iteration start (prep teardown) plus one at rollback time,
	// so we expect at least 2 calls per started host.
	rmCount := func(host string) int {
		n := 0
		for _, c := range ssh.calls {
			if c.host == host && len(c.argv) >= 4 && c.argv[0] == "docker" && c.argv[1] == "rm" && c.argv[2] == "-f" && c.argv[3] == remoteContainerName {
				n++
			}
		}
		return n
	}
	for _, h := range []string{"vm1", "vm2"} {
		if got := rmCount(h); got < 2 {
			t.Errorf("host %s: got %d docker rm -f calls, want >=2 (prep + rollback)", h, got)
		}
	}
}

func TestStartGatesNextDockerRunOnPreviousUN(t *testing.T) {
	// Regression test for the bootstrap-token-collision bug (see
	// project_remote_cluster_token_collision_bug.md): if Start launches
	// node N+1 before node N is fully joined (UN), both can pick
	// overlapping tokens and the ring wedges. Fix gates each docker run
	// on the previous node having reached UN as visible to the seed.
	//
	// Setup: script the seed's responses so vm2 NEVER reaches UN. After
	// docker-running vm1 (UN), the orchestrator should block on vm2's
	// UN wait, eventually time out, and roll back — without ever
	// docker-running vm3.
	hosts := []string{"vm1", "vm2", "vm3"}
	ssh := &fakeSSH{
		nodetoolStatusDefault: map[string]fakeResp{
			// vm1 visible UN, vm2 visible only as UJ, vm3 not visible.
			"vm1": {out: "Datacenter: dc1\n" +
				"===============\n" +
				"Status=Up/Down\n" +
				"|/ State=Normal/Leaving/Joining/Moving\n" +
				"--  Address  Load  Tokens  Owns  Host ID  Rack\n" +
				"UN  vm1  100KiB  16  100.0%  host-0  rack1\n" +
				"UJ  vm2  100KiB  16  ?       host-1  rack1\n"},
		},
	}
	b := newRemoteForTest(hosts, ssh)

	if err := b.Start(); err == nil {
		t.Fatal("Start: expected error, got nil")
	}

	// The critical assertion: no docker run for vm3 was ever issued,
	// because vm2 never reached UN and Start should not have advanced.
	for _, c := range ssh.calls {
		if c.host == "vm3" && len(c.argv) >= 2 && c.argv[0] == "docker" && c.argv[1] == "run" {
			t.Fatalf("vm3 docker run was issued before vm2 reached UN — "+
				"the per-node UN gate is broken; full argv was: %v", c.argv)
		}
	}
}

func TestStartSetsMemlockUlimit(t *testing.T) {
	// Cassandra mlocks its heap to prevent swap-induced GC stalls.
	// Without --ulimit memlock=-1:-1 the container's RLIMIT_MEMLOCK is
	// the default 64 KiB and the JVM's mlock call fails with ENOMEM
	// (logged as "Unable to lock JVM memory (ENOMEM)"). Validate that
	// the docker run on every host raises memlock to unlimited.
	hosts := []string{"vm1", "vm2", "vm3"}
	ssh := &fakeSSH{nodetoolStatusDefault: happyPathNodetoolStatus("vm1", hosts)}
	b := newRemoteForTest(hosts, ssh)

	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	for _, h := range hosts {
		var found bool
		for _, c := range ssh.calls {
			if c.host != h || len(c.argv) < 2 || c.argv[0] != "docker" || c.argv[1] != "run" {
				continue
			}
			// Look for the consecutive pair "--ulimit", "memlock=-1:-1"
			// (the value `memlock=-1:-1` sets both soft and hard limits
			// to unlimited).
			for i := 0; i < len(c.argv)-1; i++ {
				if c.argv[i] == "--ulimit" && c.argv[i+1] == "memlock=-1:-1" {
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			t.Errorf("docker run on %s missing --ulimit memlock=-1:-1", h)
		}
	}
}

func TestStartWaitsForNativeTransportPerNode(t *testing.T) {
	// Start() must gate each node on `nodetool statusbinary` returning
	// "running" — without this, gocql can dial the joining node's CQL
	// port before the listener has actually bound, surfacing as a
	// transient "connection refused". The fake's first two calls per
	// host return "not running", forcing the poll loop to spin until
	// the sticky default ("running") takes over.
	hosts := []string{"vm1", "vm2", "vm3"}
	statusBinaryResponses := map[string][]fakeResp{}
	for _, h := range hosts {
		statusBinaryResponses[h] = []fakeResp{
			{out: "not running"},
			{out: "not running"},
		}
	}
	ssh := &fakeSSH{
		nodetoolStatusDefault: happyPathNodetoolStatus("vm1", hosts),
		statusBinaryResponses: statusBinaryResponses,
		// statusBinaryDefault left nil — fake falls back to "running"
		// once responses are drained, see Exec().
	}
	b := newRemoteForTest(hosts, ssh)

	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Every host must have at least one statusbinary call.
	for _, h := range hosts {
		count := 0
		for _, c := range ssh.calls {
			if c.host == h && len(c.argv) >= 5 && c.argv[0] == "docker" &&
				c.argv[1] == "exec" && c.argv[3] == "nodetool" && c.argv[4] == "statusbinary" {
				count++
			}
		}
		if count < 3 {
			t.Errorf("host %s: expected >=3 statusbinary calls (2 not-running + at least 1 running), got %d", h, count)
		}
	}
}

func TestStartFailsWhenNativeTransportNeverRuns(t *testing.T) {
	// If statusbinary keeps reporting "not running" past the
	// nodeReadyBudget, Start() must fail with a descriptive error
	// rather than wedging forever or dropping into the next node.
	hosts := []string{"vm1", "vm2"}
	ssh := &fakeSSH{
		nodetoolStatusDefault: happyPathNodetoolStatus("vm1", hosts),
		statusBinaryDefault: map[string]fakeResp{
			"vm1": {out: "not running"},
			"vm2": {out: "not running"},
		},
	}
	b := newRemoteForTest(hosts, ssh)

	err := b.Start()
	if err == nil {
		t.Fatal("Start: expected error, got nil")
	}
	if !strings.Contains(err.Error(), "native transport") {
		t.Errorf("expected error mentioning 'native transport', got: %v", err)
	}
}
