package cluster

import (
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"
)

// localSingleContainer is the Docker container name for the single-node
// Cassandra used by ModeLocalSingle. Duplicates cassandra.ContainerName in
// internal/benchmark/cassandra/connection.go (cluster cannot import cassandra
// due to a package import cycle); keep both in sync if renaming.
const localSingleContainer = "uuid-bench-cassandra"

// localSingleCompose is the path (relative to the orchestrator's working
// directory) to the single-node Cassandra docker-compose file.
const localSingleCompose = "docker/docker-compose.cassandra.yml"

// LocalSingleBackend implements Backend for a single-node Cassandra running
// in a docker-compose-managed container on the host. It is the historical
// single-node deployment used by the thesis benchmarks.
type LocalSingleBackend struct{}

// NewLocalSingle returns a backend for the single-node compose stack.
func NewLocalSingle() *LocalSingleBackend { return &LocalSingleBackend{} }

// Start brings the single-node container up. Per the project's "fresh
// container per UUID type" invariant, it first tears down any existing
// stack with volumes removed so each Start delivers an empty database.
//
// Compose calls go through composeCmd so the V1/V2 docker-compose
// fallback applies (Taurus HPC nodes only have V1; the laptop has V2).
// `compose up -d` is retried up to startRetries times with retryBackoff
// between attempts; container init on Taurus is slower and flakier than
// on a developer laptop, so a single attempt is not robust enough. Each
// retry repeats the `down -v --remove-orphans` so partial state from a
// failed `up` doesn't poison the next attempt.
func (b *LocalSingleBackend) Start() error {
	if out, err := composeCmd(localSingleCompose, "down", "-v", "--remove-orphans").CombinedOutput(); err != nil {
		log.Printf("LocalSingleBackend: pre-Start teardown returned error (continuing): %v; output: %s", err, strings.TrimSpace(string(out)))
	}
	var lastErr error
	var lastOut []byte
	for attempt := 1; attempt <= startRetries; attempt++ {
		if attempt > 1 {
			log.Printf("LocalSingleBackend: retry %d/%d", attempt, startRetries)
			_, _ = composeCmd(localSingleCompose, "down", "-v", "--remove-orphans").CombinedOutput()
			time.Sleep(retryBackoff)
		}
		out, err := composeCmd(localSingleCompose, "up", "-d").CombinedOutput()
		if err == nil {
			return nil
		}
		lastErr, lastOut = err, out
	}
	return fmt.Errorf("compose up after %d attempts: %w (last output: %s)", startRetries, lastErr, lastOut)
}

func (b *LocalSingleBackend) Stop() error {
	out, err := composeCmd(localSingleCompose, "down", "-v", "--remove-orphans").CombinedOutput()
	if err != nil {
		return fmt.Errorf("compose down: %w (output: %s)", err, out)
	}
	return nil
}

// WaitForReady polls cqlsh until Cassandra accepts a query. 120s timeout
// gives cqlsh-via-docker-exec headroom for its startup overhead on top of
// Cassandra's own bringup.
func (b *LocalSingleBackend) WaitForReady() error {
	deadline := time.Now().Add(120 * time.Second)
	var lastErr error
	var lastOut []byte
	for time.Now().Before(deadline) {
		out, err := exec.Command("docker", "exec", localSingleContainer,
			"cqlsh", "-e", "SELECT release_version FROM system.local").CombinedOutput()
		if err == nil {
			return nil
		}
		lastErr = err
		lastOut = out
		time.Sleep(2 * time.Second)
	}
	if lastErr != nil {
		return fmt.Errorf("Cassandra single-node not ready within 120s; last cqlsh error: %v; output: %s", lastErr, strings.TrimSpace(string(lastOut)))
	}
	return fmt.Errorf("Cassandra single-node not ready within 120s")
}

func (b *LocalSingleBackend) ExecOnNode(i int, argv ...string) (string, error) {
	if i != 0 {
		return "", fmt.Errorf("LocalSingleBackend has only node 0, got %d", i)
	}
	if len(argv) == 0 {
		return "", fmt.Errorf("ExecOnNode: argv is empty")
	}
	full := append([]string{"exec", localSingleContainer}, argv...)
	out, err := exec.Command("docker", full...).CombinedOutput()
	return string(out), err
}

// CopyToNode wraps `docker cp src container:dst`. Note that Docker uses
// the FIRST `:` in the second arg as the container/path separator, so
// `dst` must not contain a `:` before its leading `/` — passing e.g.
// "weird:path" would be misparsed by Docker itself, not by this wrapper.
func (b *LocalSingleBackend) CopyToNode(i int, src, dst string) error {
	if i != 0 {
		return fmt.Errorf("LocalSingleBackend has only node 0, got %d", i)
	}
	out, err := exec.Command("docker", "cp", src, fmt.Sprintf("%s:%s", localSingleContainer, dst)).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker cp: %w (output: %s)", err, out)
	}
	return nil
}

func (b *LocalSingleBackend) NodeAddresses() []string {
	return []string{"127.0.0.1"}
}

// NodeContainerIDs returns the running container's ID. The filter uses an
// anchored regex (^name$) so the substring "uuid-bench-cassandra" doesn't
// also match the cluster compose's "uuid-bench-cassandra-1/2/3" if those
// happen to be running concurrently. `--no-trunc` returns the long ID
// expected by cgroup-v2 paths in internal/benchmark/io/io_metrics.go.
func (b *LocalSingleBackend) NodeContainerIDs() ([]string, error) {
	out, err := exec.Command("docker", "ps",
		"--filter", "name=^"+localSingleContainer+"$",
		"--format", "{{.ID}}",
		"--no-trunc").Output()
	if err != nil {
		return nil, fmt.Errorf("docker ps: %w", err)
	}
	id := strings.TrimSpace(string(out))
	if id == "" {
		return nil, fmt.Errorf("no running container named %q", localSingleContainer)
	}
	return []string{id}, nil
}

func (b *LocalSingleBackend) NodeCount() int { return 1 }
func (b *LocalSingleBackend) Mode() Mode     { return ModeLocalSingle }
