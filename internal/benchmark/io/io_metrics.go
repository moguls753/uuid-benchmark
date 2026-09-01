package docker

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/remote"
)

// IOStats represents I/O statistics from cgroup io.stat
type IOStats struct {
	ReadBytes  uint64
	WriteBytes uint64
	ReadOps    uint64
	WriteOps   uint64
	Timestamp  time.Time
	// Fields counts the recognized counters this snapshot was built from.
	// Zero means the cgroup file was empty or unparseable, which yields the
	// same all-zero struct as a container that genuinely did no I/O. The
	// consumer needs the two apart: a zero read_iops is a real and common
	// result in the cached regime, so a dropped measurement must not be able
	// to enter the analysis wearing that value.
	Fields int
}

// IOMetrics represents calculated I/O metrics over a time period
type IOMetrics struct {
	ReadIOPS          float64
	WriteIOPS         float64
	ReadThroughputMB  float64
	WriteThroughputMB float64
	// Valid is false when the four values above are zero because the
	// measurement was dropped rather than because no I/O happened: an empty
	// or unparseable io.stat on either side, a counter regression, or a
	// non-positive window. Callers must not report the numbers without it.
	Valid bool
}

// NodeRef identifies a Cassandra node for IO metrics collection.
//
// Host == "" or "localhost" indicates a node running on the orchestrator
// host (LocalSingle, LocalCluster modes); the cgroup file is read
// directly. Any other Host is treated as a remote SSH target and the
// cgroup file is read via `cat` over SSH. ContainerID is the long-form
// Docker container ID as returned by Backend.NodeContainerIDs.
//
// Note: `127.0.0.1` and other loopback addresses are treated as remote
// SSH targets (anything not literally `""` or `"localhost"`). Use the
// empty string or `"localhost"` for orchestrator-host nodes to avoid a
// confusing SSH-dial-to-self failure.
type NodeRef struct {
	Host        string
	ContainerID string
}

// GetContainerIOStats reads I/O statistics from cgroup v2 io.stat for a container
// identified by name (single-node, orchestrator-host case).
func GetContainerIOStats(containerName string) (*IOStats, error) {
	cgroupPath, err := findContainerCgroupPath(containerName)
	if err != nil {
		return nil, fmt.Errorf("failed to find container cgroup: %w", err)
	}
	return readIOStatFile(cgroupPath + "/io.stat")
}

// GetClusterIOStats sums IO across the given nodes. Returns one
// aggregated IOStats representing total cluster IO at the moment of
// capture. Empty refs slice returns a zero-valued stats with the
// current timestamp.
//
// For remote nodes, sshUser/sshKey configure the SSH client used to
// `cat` the cgroup file. A fresh client is constructed for the call;
// connection caching is a future optimization (see Task 7.4) — for
// today's low-frequency captures (a handful per scenario) the per-call
// dial overhead is acceptable.
func GetClusterIOStats(refs []NodeRef, sshUser, sshKey string) (*IOStats, error) {
	total := &IOStats{Timestamp: time.Now()}
	if len(refs) == 0 {
		return total, nil
	}

	var sshClient *remote.Client // lazily constructed only if a remote ref appears
	for i, ref := range refs {
		// Empty/whitespace container ID is always a bug upstream
		// (NodeContainerIDs() should have rejected it). Catching it
		// here keeps the failure mode obvious instead of producing
		// a vague "no cgroup io.stat at docker-.scope" lower down,
		// and prevents the empty-ID value from leaking into a
		// downstream uint64 underflow in CalculateIOMetrics. See
		// [[project-io-metric-empty-container-id-bug]].
		if strings.TrimSpace(ref.ContainerID) == "" {
			return nil, fmt.Errorf("io stats refs[%d] host=%q: empty container ID", i, ref.Host)
		}
		var (
			stats *IOStats
			err   error
		)
		if ref.Host == "" || ref.Host == "localhost" {
			stats, err = getLocalIOStatsByID(ref.ContainerID)
		} else {
			if sshClient == nil {
				sshClient = remote.NewClient(sshUser, sshKey)
			}
			stats, err = getRemoteIOStats(sshClient, ref.Host, ref.ContainerID)
		}
		if err != nil {
			return nil, fmt.Errorf("io stats for host=%q container=%s: %w", ref.Host, ref.ContainerID, err)
		}
		total.ReadBytes += stats.ReadBytes
		total.WriteBytes += stats.WriteBytes
		total.ReadOps += stats.ReadOps
		total.WriteOps += stats.WriteOps
		total.Fields += stats.Fields
	}
	return total, nil
}

// CalculateIOMetrics calculates I/O metrics from two IOStats snapshots.
//
// Deltas are clamped at 0. cgroup io.stat counters are monotonically
// increasing for a given container's lifetime, so end >= start is the
// only valid case. A "negative" delta (end < start) means one of:
// (a) the container was recreated between snapshots and we re-aliased
//     the cgroup path to a fresh, smaller-counter container,
// (b) one snapshot summed a different set of block devices than the
//     other (e.g. a device went away),
// (c) one snapshot was a partial / zero read that didn't error
//     upstream.
// In every case the correct user-visible value is "we don't have a
// reliable measurement here" — i.e. 0 — NOT a uint64 underflow cast to
// float (~1.8e19) divided by a few seconds (~3.6e18). The latter would
// silently corrupt the I/O column of the comparison table without any
// operator-visible warning. See [[project-io-metric-empty-container-id-bug]].
func CalculateIOMetrics(start, end *IOStats) IOMetrics {
	duration := end.Timestamp.Sub(start.Timestamp).Seconds()
	if duration <= 0 {
		return IOMetrics{}
	}
	// No recognized counter on either side means the cgroup file was empty or
	// unparseable. The resulting zeros are indistinguishable from a container
	// that did no I/O, so the measurement is marked invalid instead.
	if start.Fields == 0 || end.Fields == 0 {
		fmt.Fprintf(os.Stderr, "Warning: I/O snapshot held no recognized counters (before=%d fields, after=%d fields); measurement dropped\n", start.Fields, end.Fields)
		return IOMetrics{}
	}

	// Surface counter regressions before clamping. Distinguishing
	// "I/O field reported 0 because no I/O happened" from "I/O field
	// reported 0 because we dropped the measurement" matters for the
	// thesis comparison table: a silent 0 in the I/O column would look
	// like a real result. The warning gives the operator a paper trail.
	var regressions []string
	if end.ReadBytes < start.ReadBytes {
		regressions = append(regressions, fmt.Sprintf("rbytes %d<%d", end.ReadBytes, start.ReadBytes))
	}
	if end.WriteBytes < start.WriteBytes {
		regressions = append(regressions, fmt.Sprintf("wbytes %d<%d", end.WriteBytes, start.WriteBytes))
	}
	if end.ReadOps < start.ReadOps {
		regressions = append(regressions, fmt.Sprintf("rios %d<%d", end.ReadOps, start.ReadOps))
	}
	if end.WriteOps < start.WriteOps {
		regressions = append(regressions, fmt.Sprintf("wios %d<%d", end.WriteOps, start.WriteOps))
	}
	if len(regressions) > 0 {
		fmt.Fprintf(os.Stderr, "Warning: I/O counter regression (clamped to 0): %s\n", strings.Join(regressions, ", "))
	}

	readBytes := clampedDelta(end.ReadBytes, start.ReadBytes)
	writeBytes := clampedDelta(end.WriteBytes, start.WriteBytes)
	readOps := clampedDelta(end.ReadOps, start.ReadOps)
	writeOps := clampedDelta(end.WriteOps, start.WriteOps)

	return IOMetrics{
		ReadIOPS:          readOps / duration,
		WriteIOPS:         writeOps / duration,
		ReadThroughputMB:  readBytes / duration / (1024 * 1024),
		WriteThroughputMB: writeBytes / duration / (1024 * 1024),
		// A regression means at least one counter was clamped, so the numbers
		// understate by an unknown amount. Reported as dropped, not as data.
		Valid: len(regressions) == 0,
	}
}

// clampedDelta returns end-start as a float64, with end<start treated
// as 0 rather than allowed to underflow uint64 arithmetic.
func clampedDelta(end, start uint64) float64 {
	if end < start {
		return 0
	}
	return float64(end - start)
}

// cgroupPathCandidates returns the cgroup v2 paths to probe for a Docker
// container's io.stat file, in priority order. Systemd-managed Docker
// (the common case on modern distros) uses the first form; the legacy
// non-systemd cgroup layout uses the second.
func cgroupPathCandidates(containerID string) []string {
	return []string{
		fmt.Sprintf("/sys/fs/cgroup/system.slice/docker-%s.scope/io.stat", containerID),
		fmt.Sprintf("/sys/fs/cgroup/docker/%s/io.stat", containerID),
	}
}

// parseIOStatContent parses cgroup v2 io.stat content into an IOStats.
// Pure string parser: no I/O. Malformed lines and unknown keys are
// silently skipped (defense-in-depth against future cgroup v2 format
// additions).
//
// The returned error is currently always nil; the signature is reserved
// for a future strict mode (e.g. rejecting input with zero recognized
// fields) without breaking callers.
func parseIOStatContent(content string) (*IOStats, error) {
	stats := &IOStats{Timestamp: time.Now()}
	for _, line := range strings.Split(content, "\n") {
		// Format: <major>:<minor> rbytes=X wbytes=Y rios=Z wios=W
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		for _, field := range fields[1:] {
			parts := strings.SplitN(field, "=", 2)
			if len(parts) != 2 {
				continue
			}
			value, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				continue
			}
			switch parts[0] {
			case "rbytes":
				stats.ReadBytes += value
				stats.Fields++
			case "wbytes":
				stats.WriteBytes += value
				stats.Fields++
			case "rios":
				stats.ReadOps += value
				stats.Fields++
			case "wios":
				stats.WriteOps += value
				stats.Fields++
			}
		}
	}
	return stats, nil
}

// readIOStatFile reads and parses a cgroup io.stat file from the local
// filesystem.
func readIOStatFile(path string) (*IOStats, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read io.stat at %s: %w", path, err)
	}
	return parseIOStatContent(string(data))
}

// errNoCgroupLocal prefixes the "couldn't find a cgroup io.stat
// candidate" error message produced by getLocalIOStatsByID. Exported
// (within the package) as a constant so tests that route through
// GetClusterIOStats can pin the local-vs-remote dispatch via error
// substring matching without coupling silently to the wording.
const errNoCgroupLocal = "no cgroup io.stat found for container ID"

// getLocalIOStatsByID reads io.stat for a container running on the
// orchestrator host, given its long-form container ID. Tries each
// cgroup-path candidate in order and returns the first that exists.
func getLocalIOStatsByID(containerID string) (*IOStats, error) {
	paths := cgroupPathCandidates(containerID)
	for _, path := range paths {
		if _, err := os.Stat(path); err == nil {
			return readIOStatFile(path)
		}
	}
	return nil, fmt.Errorf("%s %s (tried %v)", errNoCgroupLocal, containerID, paths)
}

// getRemoteIOStats reads io.stat for a container running on a remote
// host via SSH `cat`. Tries each cgroup-path candidate in order; on
// each candidate it incurs one SSH dial (no connection reuse across
// candidates or calls). For today's low capture frequency this is
// acceptable; connection caching is tracked as Task 7.4.
func getRemoteIOStats(client *remote.Client, host, containerID string) (*IOStats, error) {
	var lastErr error
	for _, path := range cgroupPathCandidates(containerID) {
		out, err := client.Exec(host, "cat", path)
		if err != nil {
			lastErr = err
			continue
		}
		return parseIOStatContent(out)
	}
	return nil, fmt.Errorf("no cgroup io.stat found on host %s for container ID %s: %w", host, containerID, lastErr)
}

// findContainerCgroupPath finds the cgroup path for a Docker container
// running on the orchestrator host, by name.
func findContainerCgroupPath(containerName string) (string, error) {
	containerID, err := getContainerID(containerName)
	if err != nil {
		return "", err
	}
	// cgroupPathCandidates returns paths ending in "/io.stat"; this
	// function's historical contract returns the parent directory, so
	// strip the suffix.
	for _, ioStatPath := range cgroupPathCandidates(containerID) {
		if _, err := os.Stat(ioStatPath); err == nil {
			return strings.TrimSuffix(ioStatPath, "/io.stat"), nil
		}
	}
	return "", fmt.Errorf("could not find cgroup path for container %s (ID: %s)", containerName, containerID)
}

// getContainerID retrieves the container ID from the container name using docker ps.
// The filter uses anchored regex (^name$) to avoid substring matches: an
// unanchored `name=uuid-bench-cassandra` filter also matches
// `uuid-bench-cassandra-1`/`-2`/`-3` when a multi-node compose is running
// concurrently, producing a multi-line output that breaks the cgroup lookup.
func getContainerID(containerName string) (string, error) {
	cmd := exec.Command("docker", "ps", "--filter", "name=^"+containerName+"$", "--format", "{{.ID}}", "--no-trunc")

	var out bytes.Buffer
	cmd.Stdout = &out

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to run docker ps: %w", err)
	}

	containerID := strings.TrimSpace(out.String())
	if containerID == "" {
		return "", fmt.Errorf("container not found: %s", containerName)
	}

	return containerID, nil
}
