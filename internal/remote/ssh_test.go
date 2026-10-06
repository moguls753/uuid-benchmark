package remote

import (
	"slices"
	"strings"
	"testing"
	"time"

	"golang.org/x/crypto/ssh"
)

func TestBuildShellCommand(t *testing.T) {
	cases := map[string]struct {
		argv []string
		want string
	}{
		// buildShellCommand always quotes every element. Single-word
		// elements like "nodetool" become 'nodetool' on the wire — the
		// remote shell still parses them back identically.
		"simple argv":         {[]string{"nodetool", "status"}, "'nodetool' 'status'"},
		"spaces in element":   {[]string{"echo", "hello world"}, "'echo' 'hello world'"},
		"embedded single":     {[]string{"echo", "it's"}, `'echo' 'it'\''s'`},
		"shell metacharacter": {[]string{"echo", "$HOME"}, "'echo' '$HOME'"},
		// Pins the helper's contract on empty input. Exec rejects this
		// before calling, but the helper itself must return "" rather
		// than panic so an accidental inlined caller fails loudly later
		// (Session.Run("") returns an error from the remote shell).
		"empty argv": {[]string{}, ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := buildShellCommand(tc.argv)
			if got != tc.want {
				t.Errorf("buildShellCommand(%v): got %q want %q", tc.argv, got, tc.want)
			}
		})
	}
}

func TestShellQuote(t *testing.T) {
	cases := map[string]string{
		"":            `''`,
		"abc":         `'abc'`,
		"hello world": `'hello world'`,
		"it's":        `'it'\''s'`,
		"$HOME":       `'$HOME'`,
	}
	for in, want := range cases {
		if got := shellQuote(in); got != want {
			t.Errorf("shellQuote(%q): got %q want %q", in, got, want)
		}
	}
}

func TestHasPort(t *testing.T) {
	cases := map[string]bool{
		"taurus5":      false,
		"taurus5:22":   true,
		"127.0.0.1":    false,
		"127.0.0.1:22": true,
		"":             false,
		// Bracketed IPv6 forms work; raw IPv6 literals are not supported
		// (see hasPort doc) — pinned here so the limitation is testable.
		"[::1]:22": true,
	}
	for in, want := range cases {
		if got := hasPort(in); got != want {
			t.Errorf("hasPort(%q): got %v want %v", in, got, want)
		}
	}
}

// fakeSession implements sessionRunner. Run blocks on `block` until
// either the test closes it (returning nil) or Signal is called (which
// also closes `block` so Run returns). This lets a test pin the
// timeout path without standing up a real ssh server.
type fakeSession struct {
	block      chan struct{}
	runErr     error // error returned from Run when block closes naturally
	signalled  bool
	closed     bool
	signalDone chan struct{}
}

func newFakeSession() *fakeSession {
	return &fakeSession{
		block:      make(chan struct{}),
		signalDone: make(chan struct{}, 1),
	}
}

func (f *fakeSession) Run(cmd string) error {
	<-f.block
	return f.runErr
}

func (f *fakeSession) Signal(sig ssh.Signal) error {
	f.signalled = true
	// Unblock Run so the goroutine in runSession can exit cleanly.
	// Use a non-blocking close pattern to tolerate repeat signal calls.
	select {
	case <-f.block:
		// already closed
	default:
		close(f.block)
	}
	select {
	case f.signalDone <- struct{}{}:
	default:
	}
	return nil
}

func (f *fakeSession) Close() error {
	f.closed = true
	return nil
}

func TestRunSessionReturnsCompletedRunResult(t *testing.T) {
	// Happy path: Run returns before timeout elapses. The returned error
	// must be exactly what Run returned (nil here), and Signal/Close must
	// not have been invoked by runSession.
	sess := newFakeSession()
	close(sess.block) // Run returns immediately
	if err := runSession(sess, "true", time.Second); err != nil {
		t.Fatalf("runSession: unexpected error %v", err)
	}
	if sess.signalled {
		t.Error("runSession signalled on successful run")
	}
}

func TestRunSessionTimesOutOnHungRun(t *testing.T) {
	// Pins the FIX 1 behavior: a session whose Run hangs forever must be
	// bounded by the timeout. The returned error must clearly identify
	// this as a session-level timeout (not a connection timeout), and
	// the session must be Signal'd + Close'd so resources are released.
	sess := newFakeSession()
	start := time.Now()
	err := runSession(sess, "sleep forever", 50*time.Millisecond)
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("runSession: expected timeout error, got nil")
	}
	if !strings.Contains(err.Error(), "timed out") {
		t.Errorf("error %q should mention 'timed out'", err.Error())
	}
	if !strings.Contains(err.Error(), "session") {
		t.Errorf("error %q should mention 'session' (distinguishes from connection timeout)", err.Error())
	}
	// Allow generous slack so a loaded CI host doesn't flake; we only
	// care that we didn't wait orders of magnitude longer than the
	// timeout (which would mean the timeout wasn't honored).
	if elapsed > 2*time.Second {
		t.Errorf("runSession took %s, expected ~50ms timeout", elapsed)
	}
	if !sess.signalled {
		t.Error("runSession did not Signal on timeout")
	}
	if !sess.closed {
		t.Error("runSession did not Close on timeout")
	}
}

func TestRunSessionPropagatesRunError(t *testing.T) {
	// Non-timeout error returned by Run must be passed through verbatim,
	// not wrapped in a timeout error.
	sess := newFakeSession()
	wantErr := strings.Repeat("x", 1) // any non-nil error
	sess.runErr = &fakeErr{msg: wantErr}
	close(sess.block)
	err := runSession(sess, "false", time.Second)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if strings.Contains(err.Error(), "timed out") {
		t.Errorf("error %q should NOT be a timeout error", err.Error())
	}
}

type fakeErr struct{ msg string }

func (e *fakeErr) Error() string { return e.msg }

func TestClientTimeoutDefaultsWhenZero(t *testing.T) {
	// A zero-valued Timeout must fall back to defaultClientTimeout so a
	// Client constructed via a struct literal (rather than NewClient)
	// still gets the safety net.
	c := &Client{user: "x"}
	if got := c.timeout(); got != defaultClientTimeout {
		t.Errorf("timeout(): got %v want %v", got, defaultClientTimeout)
	}
}

func TestClientTimeoutHonorsOverride(t *testing.T) {
	c := &Client{user: "x", Timeout: 5 * time.Second}
	if got := c.timeout(); got != 5*time.Second {
		t.Errorf("timeout(): got %v want 5s", got)
	}
}

func TestBuildScpArgs(t *testing.T) {
	cases := map[string]struct {
		keyPath string
		user    string
		host    string
		src     string
		dst     string
		want    []string
	}{
		// src != dst here so a future refactor that swapped the two
		// positional args in buildScpArgs would fail this test.
		"with key path": {
			keyPath: "/home/u/.ssh/id_ed25519",
			user:    "benchuser",
			host:    "taurus5",
			src:     "/local/workload",
			dst:     "/opt/workload",
			want: []string{
				"-i", "/home/u/.ssh/id_ed25519",
				"-o", "StrictHostKeyChecking=no",
				"-o", "UserKnownHostsFile=/dev/null",
				"-o", "BatchMode=yes",
				"/local/workload",
				"benchuser@taurus5:/opt/workload",
			},
		},
		"without key path falls back to ssh-agent / default keys": {
			keyPath: "",
			user:    "benchuser",
			host:    "taurus5",
			src:     "/tmp/workload",
			dst:     "/tmp/workload",
			want: []string{
				"-o", "StrictHostKeyChecking=no",
				"-o", "UserKnownHostsFile=/dev/null",
				"-o", "BatchMode=yes",
				"/tmp/workload",
				"benchuser@taurus5:/tmp/workload",
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := buildScpArgs(tc.keyPath, tc.user, tc.host, tc.src, tc.dst)
			if !slices.Equal(got, tc.want) {
				t.Errorf("buildScpArgs: got %v want %v", got, tc.want)
			}
		})
	}
}
