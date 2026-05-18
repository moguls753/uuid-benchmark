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
// test can say "on taurus5, the third statusgossip should return
// running". Unmatched calls return ("", nil) — a benign default that
// keeps the Start() happy path moving when a test doesn't care.
type fakeSSH struct {
	mu    sync.Mutex
	calls []fakeCall

	// statusgossipResponses[host] is consumed front-to-back on each
	// `docker exec ... nodetool statusgossip` call for that host. If
	// the slice is exhausted, the call returns ("", nil).
	statusgossipResponses map[string][]fakeResp
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

	// Detect `docker exec <name> nodetool statusgossip` and pop a scripted response.
	if len(argv) >= 5 && argv[0] == "docker" && argv[1] == "exec" && argv[3] == "nodetool" && argv[4] == "statusgossip" {
		if resps := f.statusgossipResponses[host]; len(resps) > 0 {
			r := resps[0]
			f.statusgossipResponses[host] = resps[1:]
			return r.out, r.err
		}
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
	b.gossipBudget = 200 * time.Millisecond
	b.gossipPollInterval = 5 * time.Millisecond
	return b
}

// gossipRunningOnEveryHost wires the fake so every host's first
// statusgossip call returns "running" — i.e. happy-path Start.
func gossipRunningOnEveryHost(hosts []string) map[string][]fakeResp {
	m := make(map[string][]fakeResp, len(hosts))
	for _, h := range hosts {
		m[h] = []fakeResp{{out: "gossip running\n"}}
	}
	return m
}

func TestStartSetsBroadcastAddressPerHost(t *testing.T) {
	hosts := []string{"vm1", "vm2", "vm3"}
	ssh := &fakeSSH{statusgossipResponses: gossipRunningOnEveryHost(hosts)}
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
	ssh := &fakeSSH{statusgossipResponses: gossipRunningOnEveryHost(hosts)}
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

func TestStartPollsUntilGossipRunning(t *testing.T) {
	// Each host's first two statusgossip calls return non-"running"
	// output (and an error to simulate the early boot window where
	// nodetool exits non-zero); the third returns "running". Start
	// must succeed.
	hosts := []string{"vm1", "vm2", "vm3"}
	ssh := &fakeSSH{statusgossipResponses: map[string][]fakeResp{}}
	bootErr := fmt.Errorf("nodetool: connection refused (Cassandra still booting)")
	for _, h := range hosts {
		ssh.statusgossipResponses[h] = []fakeResp{
			{err: bootErr},
			{out: "starting up\n", err: bootErr},
			{out: "running\n"},
		}
	}
	b := newRemoteForTest(hosts, ssh)

	if err := b.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Each host should have received exactly 3 statusgossip calls.
	gossipCalls := func(host string) int {
		n := 0
		for _, c := range ssh.calls {
			if c.host == host && len(c.argv) >= 5 && c.argv[0] == "docker" && c.argv[1] == "exec" && c.argv[3] == "nodetool" && c.argv[4] == "statusgossip" {
				n++
			}
		}
		return n
	}
	for _, h := range hosts {
		if got := gossipCalls(h); got != 3 {
			t.Errorf("host %s: got %d statusgossip calls, want 3", h, got)
		}
	}
}

func TestStartFailsWhenGossipNeverReachesRunning(t *testing.T) {
	// The fake never returns "running" — Start must time out with a
	// host-named error that includes the last output.
	hosts := []string{"vm1", "vm2", "vm3"}
	// Empty default (unmatched calls return ("", nil)) is "not running",
	// so we don't even need to script vm1 — the budget will elapse
	// without ever seeing "running".
	ssh := &fakeSSH{statusgossipResponses: map[string][]fakeResp{}}
	b := newRemoteForTest(hosts, ssh)

	err := b.Start()
	if err == nil {
		t.Fatal("Start: expected timeout error, got nil")
	}
	// Error must blame vm1 (the first host that fails) and mention
	// gossip so the operator knows what was being waited on.
	msg := err.Error()
	if !strings.Contains(msg, "vm1") {
		t.Errorf("error %q should name the offending host vm1", msg)
	}
	if !strings.Contains(msg, "gossip") {
		t.Errorf("error %q should mention gossip", msg)
	}
}

func TestStartRollsBackOnLaterHostFailure(t *testing.T) {
	// vm1 and vm2 succeed; vm3 never reports running. Start must fail
	// AND roll back vm1+vm2 (docker rm -f / volume rm calls recorded).
	hosts := []string{"vm1", "vm2", "vm3"}
	// vm1 and vm2 report running; vm3 is unscripted so every poll
	// returns ("", nil) — gossip-running substring never matches and
	// the budget elapses.
	ssh := &fakeSSH{statusgossipResponses: map[string][]fakeResp{
		"vm1": {{out: "running\n"}},
		"vm2": {{out: "running\n"}},
	}}
	b := newRemoteForTest(hosts, ssh)

	if err := b.Start(); err == nil {
		t.Fatal("Start: expected error, got nil")
	}

	// After the failure, rollback should have run `docker rm -f cassandra`
	// on vm1 AND vm2 (the started set). Count those calls AFTER the
	// first gossip-success window — i.e. there should be more than one
	// rm-f call per started host (the prep-teardown at the top of each
	// iteration plus the rollback teardown), but we just need at least
	// two total per host.
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
