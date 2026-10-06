// Package remote provides a minimal SSH executor used by RemoteClusterBackend
// to run commands and copy files on Cassandra nodes over SSH. The executor
// is intentionally thin: no connection pooling, no host-key verification
// (Taurus deployments run on a private VPN), and no retry logic. Higher
// layers (RemoteCluster, WaitForRing) handle transient failure retries.
package remote

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"golang.org/x/crypto/ssh"
)

// syncBuffer is a goroutine-safe io.Writer wrapper around a bytes.Buffer.
// ssh.Session.Run launches SEPARATE copier goroutines for Stdout and Stderr
// (see session.go start/stdout/stderr), so pointing both at one bare
// *bytes.Buffer is a data race — concurrent Writes corrupt the buffer and,
// in practice, silently drop output (which made remote command failures
// show up with an empty "(output: )" and undiagnosable). Guarding writes
// with a mutex keeps the combined, interleaved-output semantics while being
// race-free.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *syncBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *syncBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

// defaultClientTimeout caps each Exec/Copy call. The ssh.ClientConfig.Timeout
// only bounds the TCP+handshake phase; after that a stalled remote (PAM,
// NFS, wedged docker daemon — all real failure modes on first-allocation HPC
// nodes) would otherwise hang session.Run forever. 2 minutes is comfortably
// above any legitimate nodetool / docker call this benchmark issues, while
// well below the higher-level gossip / ring budgets so a single wedged call
// surfaces as a clear timeout instead of silently wedging a whole run.
const defaultClientTimeout = 2 * time.Minute

// Client holds SSH credentials and authentication material. It does NOT
// cache connections — each Exec call opens a fresh TCP+SSH session and
// closes it on return. This is intentionally simple for the benchmark's
// low-frequency use (a few exec calls per scenario, not per query).
//
// Timeout caps every Exec and Copy call end-to-end (dial + session
// execution / scp). Distinct from ssh.ClientConfig.Timeout, which only
// bounds the TCP+handshake phase. A zero value falls back to
// defaultClientTimeout; tests can shrink it.
type Client struct {
	user    string
	keyPath string // empty → falls back to ~/.ssh/id_ed25519
	Timeout time.Duration
}

// NewClient constructs a client that will authenticate as user. If keyPath
// is empty, the client falls back to ~/.ssh/id_ed25519 at dial time. The
// per-call Timeout defaults to defaultClientTimeout; callers may override
// by mutating the returned struct's Timeout field.
func NewClient(user, keyPath string) *Client {
	return &Client{user: user, keyPath: keyPath, Timeout: defaultClientTimeout}
}

// timeout returns the effective per-call timeout, defaulting if the field
// is zero so a Client constructed via a struct literal still gets the safety
// net.
func (c *Client) timeout() time.Duration {
	if c.Timeout <= 0 {
		return defaultClientTimeout
	}
	return c.Timeout
}

// sessionRunner is the slice of *ssh.Session that runSession needs. Defined
// as an interface so runSession can be unit-tested with a fake whose Run
// blocks indefinitely — exercising the timeout path without a real ssh
// server.
type sessionRunner interface {
	Run(cmd string) error
	Signal(sig ssh.Signal) error
	Close() error
}

// runSession invokes sess.Run(cmd) under timeout. If timeout elapses first,
// it best-effort signals the remote with SIGKILL and closes the session,
// then returns a session-timeout error distinct from a connection timeout
// so callers (and operators reading logs) can tell which phase hung.
//
// Note: ssh.Session.Run is a blocking call with no built-in cancellation.
// The goroutine-plus-channel pattern is the only practical way to bound it
// without rewriting the underlying ssh package.
func runSession(sess sessionRunner, cmd string, timeout time.Duration) error {
	done := make(chan error, 1)
	go func() { done <- sess.Run(cmd) }()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		// Best-effort: tell the remote shell to die and tear the session
		// down. Close() unblocks the abandoned Run goroutine above — its
		// stdout/stderr copiers get EOF.
		_ = sess.Signal(ssh.SIGKILL)
		_ = sess.Close()
		// WAIT for that goroutine to actually return before we do. Without
		// this, runSession returns immediately and the caller's
		// `defer session.Close()` / `defer cli.Close()` (see Exec) tear the
		// connection down while the copier goroutines are still reading the
		// SSH channel — a data race inside x/crypto/ssh that nil-derefs the
		// channel buffer (b.head) and panics the whole process. Bounded so a
		// Close that somehow fails to unblock Run can't wedge the call
		// indefinitely; in that (unobserved) case we accept the residual
		// leak rather than hang.
		select {
		case <-done:
		case <-time.After(10 * time.Second):
		}
		return fmt.Errorf("ssh session timed out after %s", timeout)
	}
}

// Exec runs the given argv on the remote host and returns combined
// stdout+stderr. Each argv element is shell-quoted (single-quotes with
// '\'' escapes) before being space-joined, so shell metacharacters in
// args are preserved literally rather than expanded by the remote shell.
//
// Bounded by the Client's Timeout (default defaultClientTimeout). The
// timeout covers the session-Run phase only — the TCP+SSH dial is bounded
// separately by ssh.ClientConfig.Timeout. If the session hangs, the call
// returns a "ssh session timed out" error after roughly Timeout elapses
// and best-effort signals SIGKILL on the remote.
func (c *Client) Exec(host string, argv ...string) (string, error) {
	if host == "" {
		return "", fmt.Errorf("remote.Exec: host is empty")
	}
	if c.user == "" {
		return "", fmt.Errorf("remote.Exec: client user is empty")
	}
	if len(argv) == 0 {
		return "", fmt.Errorf("remote.Exec: argv is empty")
	}

	cli, err := c.dial(host)
	if err != nil {
		return "", err
	}
	defer cli.Close()

	session, err := cli.NewSession()
	if err != nil {
		return "", fmt.Errorf("ssh new session: %w", err)
	}
	defer session.Close()

	// One race-free buffer for both streams — see syncBuffer. The stdout and
	// stderr copier goroutines started by Run write here concurrently.
	var buf syncBuffer
	session.Stdout = &buf
	session.Stderr = &buf

	full := buildShellCommand(argv)
	if err := runSession(session, full, c.timeout()); err != nil {
		// Trim the buffer on the error path for readable error logs;
		// the success path below returns raw bytes since callers parse
		// structured output (e.g. `nodetool status`) and care about exact
		// whitespace.
		return buf.String(), fmt.Errorf("ssh %s: %w (output: %s)", host, err, strings.TrimSpace(buf.String()))
	}
	return buf.String(), nil
}

// Copy uses the system `scp` binary to transfer src on the local host to
// dst on the remote host (over SSH). Authenticates using the same key
// material as Exec; host must be a bare hostname (no ":port" suffix —
// scp would misparse it as part of the destination path).
//
// Bounded by the Client's Timeout (default defaultClientTimeout) via
// exec.CommandContext: if scp stalls (wedged network, remote disk hang)
// the context's deadline kills the scp process and Copy returns a
// "context deadline exceeded" error. Without this, scp has no native
// timeout flag — a stuck transfer would hang the whole benchmark.
//
// scp is required on the orchestrator's PATH. This shells out rather
// than reimplementing the SCP protocol in Go (golang.org/x/crypto/ssh
// does not ship an SCP client).
func (c *Client) Copy(host, src, dst string) error {
	if host == "" {
		return fmt.Errorf("remote.Copy: host is empty")
	}
	if c.user == "" {
		return fmt.Errorf("remote.Copy: client user is empty")
	}
	if src == "" {
		return fmt.Errorf("remote.Copy: src is empty")
	}
	if dst == "" {
		return fmt.Errorf("remote.Copy: dst is empty")
	}
	// scp parses the first ":" in the destination as the host/path
	// separator, so a host of "taurus5:22" would produce destination
	// "22:/path/...", a confusing runtime failure. Reject up-front.
	// Asymmetric with Exec's dial(), which deliberately accepts a
	// ":port" suffix on host — the doc on Copy calls this out.
	if strings.Contains(host, ":") {
		return fmt.Errorf("remote.Copy: host %q must not contain ':' (port suffix not supported by scp)", host)
	}
	args := buildScpArgs(c.keyPath, c.user, host, src, dst)
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout())
	defer cancel()
	out, err := exec.CommandContext(ctx, "scp", args...).CombinedOutput()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("scp %s -> %s:%s timed out after %s (output: %s)", src, host, dst, c.timeout(), strings.TrimSpace(string(out)))
		}
		return fmt.Errorf("scp %s -> %s:%s: %w (output: %s)", src, host, dst, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// signer loads and parses the SSH private key. Falls back to
// ~/.ssh/id_ed25519 when c.keyPath is empty.
func (c *Client) signer() (ssh.Signer, error) {
	path := c.keyPath
	if path == "" {
		path = filepath.Join(os.Getenv("HOME"), ".ssh", "id_ed25519")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read key %s: %w", path, err)
	}
	signer, err := ssh.ParsePrivateKey(data)
	if err != nil {
		return nil, fmt.Errorf("parse private key %s: %w", path, err)
	}
	return signer, nil
}

// dial opens a TCP+SSH connection to host. If host lacks a ":port" suffix,
// ":22" is appended.
func (c *Client) dial(host string) (*ssh.Client, error) {
	signer, err := c.signer()
	if err != nil {
		return nil, err
	}
	cfg := &ssh.ClientConfig{
		User:            c.user,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // Taurus runs on a private VPN; host-key verification deferred (documented in CLAUDE.md by Task 7.1)
		Timeout:         15 * time.Second,
	}
	addr := host
	if !hasPort(host) {
		addr = host + ":22"
	}
	cli, err := ssh.Dial("tcp", addr, cfg)
	if err != nil {
		return nil, fmt.Errorf("ssh dial %s: %w", addr, err)
	}
	return cli, nil
}

// hasPort reports whether host already includes a ":port" suffix. Uses
// net.SplitHostPort, which correctly classifies bracketed IPv6 forms
// (`[::1]:22`) but rejects raw IPv6 literals (`::1`) as malformed. Raw
// IPv6 callers must therefore bracket their addresses; this benchmark
// only ever uses DNS names so the limitation is academic.
func hasPort(host string) bool {
	if host == "" {
		return false
	}
	_, _, err := net.SplitHostPort(host)
	return err == nil
}

// buildShellCommand joins argv into a single shell-safe command line:
// each element is shell-quoted, then space-joined. The remote shell
// parses the result back into tokens; per-element quoting prevents
// $VAR expansion and other metacharacter surprises.
func buildShellCommand(argv []string) string {
	quoted := make([]string, len(argv))
	for i, a := range argv {
		quoted[i] = shellQuote(a)
	}
	return strings.Join(quoted, " ")
}

// shellQuote wraps s in single quotes, escaping any internal single
// quotes as '\''. The result is safe to splice into a POSIX shell
// command line.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

// buildScpArgs assembles the argv passed to the system `scp` binary for
// copying src → user@host:dst. If keyPath is empty, no -i flag is emitted
// and scp falls back to ssh-agent / default key locations. The
// StrictHostKeyChecking=no + UserKnownHostsFile=/dev/null pair mirrors
// the InsecureIgnoreHostKey policy in dial() — Taurus runs on a private
// VPN; see CLAUDE.md (Task 7.1).
func buildScpArgs(keyPath, user, host, src, dst string) []string {
	var args []string
	if keyPath != "" {
		args = append(args, "-i", keyPath)
	}
	args = append(args,
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		// Batch mode: never prompt for a password. With key auth this is
		// implicit, but adding it explicitly turns a misconfigured key
		// from a hung scp (no timeout flag exists) into an immediate
		// failure visible in the error chain.
		"-o", "BatchMode=yes",
		src,
		fmt.Sprintf("%s@%s:%s", user, host, dst),
	)
	return args
}
