package workload

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"sync"
)

var (
	binaryPath string
	buildOnce  sync.Once
	buildErr   error

	copiedTo   = make(map[string]bool)
	copiedToMu sync.Mutex
)

// ExecutionMode selects how the workload binary is launched.
type ExecutionMode string

const (
	// ExecutionModeContainer is the default for single-node benchmarks: it
	// docker cp's the workload binary into the named container and runs it
	// via docker exec.
	ExecutionModeContainer ExecutionMode = "container"
	// ExecutionModeNative runs the workload binary directly on the host where
	// the orchestrator runs. Used for multi-node clusters where the workload
	// connects over the network to one of N nodes.
	ExecutionModeNative ExecutionMode = "native"
)

// BuildBinary compiles the workload binary as a static Linux binary.
// It is safe to call multiple times; the binary is built only once.
func BuildBinary() (string, error) {
	buildOnce.Do(func() {
		tmpDir, err := os.MkdirTemp("", "uuid-workload-*")
		if err != nil {
			buildErr = fmt.Errorf("create temp dir: %w", err)
			return
		}
		binaryPath = tmpDir + "/workload"

		cmd := exec.Command("go", "build", "-o", binaryPath, "./cmd/workload/main.go")
		cmd.Env = append(os.Environ(),
			"CGO_ENABLED=0",
			"GOOS=linux",
			"GOARCH=amd64",
		)

		var stderr bytes.Buffer
		cmd.Stderr = &stderr

		if err := cmd.Run(); err != nil {
			buildErr = fmt.Errorf("build workload binary: %w (stderr: %s)", err, stderr.String())
			return
		}
	})

	return binaryPath, buildErr
}

// CopyToContainer copies the workload binary into a Docker container.
// The copy is only performed once per container name.
func CopyToContainer(containerName string) error {
	copiedToMu.Lock()
	if copiedTo[containerName] {
		copiedToMu.Unlock()
		return nil
	}
	copiedToMu.Unlock()

	binPath, err := BuildBinary()
	if err != nil {
		return fmt.Errorf("build binary: %w", err)
	}

	cmd := exec.Command("docker", "cp", binPath, fmt.Sprintf("%s:/tmp/workload", containerName))
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("docker cp: %w (stderr: %s)", err, stderr.String())
	}

	// Make executable
	chmodCmd := exec.Command("docker", "exec", containerName, "chmod", "+x", "/tmp/workload")
	if err := chmodCmd.Run(); err != nil {
		return fmt.Errorf("chmod: %w", err)
	}

	copiedToMu.Lock()
	copiedTo[containerName] = true
	copiedToMu.Unlock()

	return nil
}

// ResetCopyCache clears the per-container copy cache.
// Call this when containers are restarted between UUID types.
func ResetCopyCache() {
	copiedToMu.Lock()
	copiedTo = make(map[string]bool)
	copiedToMu.Unlock()
}

// ExecutorConfig configures a workload binary execution.
type ExecutorConfig struct {
	Mode             ExecutionMode // Execution mode (default "" → ExecutionModeContainer)
	ContainerName    string        // Container name (required for ExecutionModeContainer)
	BinaryPath       string        // Path to workload binary on host (used in ExecutionModeNative). If empty, the package-level binaryPath populated by BuildBinary() is used. If neither is set, ExecutionModeNative returns an explicit error rather than guessing a default path.
	DBType           string        // mongodb, cassandra, or mysql
	Op               string        // insert, read, update, mixed
	KeyType          string
	NumRecords       int
	NumOps           int
	BatchSize        int
	Threads          int
	ConnectionString string
	InsertPct        int    // Insert percentage for mixed workload
	ReadPct          int    // Read percentage for mixed workload
	UpdatePct        int    // Update percentage for mixed workload
	TableName        string // Table/collection name (default "bench")
	NumBuckets       int    // Number of Cassandra partition buckets (Cassandra-only; forwarded when > 0)
	Consistency      string // CQL consistency level (Cassandra-only); forwarded as --consistency when non-empty. Empty preserves the workload binary's own default (local_one).
	// IDFile is the read-set handoff between the two workload invocations of a
	// read or update scenario (Cassandra-only). On Op "insert" the binary
	// samples SampleSize ids uniformly over insert order and writes them here;
	// on Op "read"/"update" it reads them back instead of querying the database
	// for target ids. Empty keeps the legacy partition-head fetch (bridge arm).
	IDFile     string
	SampleSize int
	SampleSeed int64
}

// buildExecArgs assembles the workload binary CLI arguments from cfg.
// It does not include the binary path itself or any docker-exec prefix.
func buildExecArgs(cfg ExecutorConfig) []string {
	args := []string{
		"--db-type", cfg.DBType,
		"--op", cfg.Op,
		"--key-type", cfg.KeyType,
		"--connection-string", cfg.ConnectionString,
	}

	if cfg.NumRecords > 0 {
		args = append(args, "--num-records", fmt.Sprintf("%d", cfg.NumRecords))
	}
	if cfg.NumOps > 0 {
		args = append(args, "--num-ops", fmt.Sprintf("%d", cfg.NumOps))
	}
	if cfg.BatchSize > 0 {
		args = append(args, "--batch-size", fmt.Sprintf("%d", cfg.BatchSize))
	}
	if cfg.Threads > 0 {
		args = append(args, "--threads", fmt.Sprintf("%d", cfg.Threads))
	}

	if cfg.TableName != "" {
		args = append(args, "--table-name", cfg.TableName)
	}

	if cfg.NumBuckets > 0 {
		args = append(args, "--num-buckets", fmt.Sprintf("%d", cfg.NumBuckets))
	}

	if cfg.Consistency != "" {
		args = append(args, "--consistency", cfg.Consistency)
	}

	if cfg.IDFile != "" {
		args = append(args, "--id-file", cfg.IDFile)
		if cfg.Op == "insert" {
			args = append(args,
				"--sample-size", fmt.Sprintf("%d", cfg.SampleSize),
				"--sample-seed", fmt.Sprintf("%d", cfg.SampleSeed),
			)
		}
	}

	if cfg.Op == "mixed" {
		args = append(args,
			"--insert-pct", fmt.Sprintf("%d", cfg.InsertPct),
			"--read-pct", fmt.Sprintf("%d", cfg.ReadPct),
			"--update-pct", fmt.Sprintf("%d", cfg.UpdatePct),
		)
	}

	return args
}

// Execute runs the workload binary (in a container or natively) and returns parsed results.
func Execute(cfg ExecutorConfig) (*WorkloadResult, error) {
	mode := cfg.Mode
	if mode == "" {
		mode = ExecutionModeContainer
	}

	args := buildExecArgs(cfg)

	var cmd *exec.Cmd
	switch mode {
	case ExecutionModeContainer:
		if cfg.ContainerName == "" {
			return nil, fmt.Errorf("container mode requires ContainerName")
		}
		if err := CopyToContainer(cfg.ContainerName); err != nil {
			return nil, fmt.Errorf("copy to container: %w", err)
		}
		dockerArgs := append([]string{"exec", cfg.ContainerName, "/tmp/workload"}, args...)
		cmd = exec.Command("docker", dockerArgs...)
	case ExecutionModeNative:
		path := cfg.BinaryPath
		if path == "" {
			path = binaryPath // populated by BuildBinary; empty if it wasn't called
		}
		if path == "" {
			return nil, fmt.Errorf("native mode requires BinaryPath in ExecutorConfig or a prior workload.BuildBinary() call; neither was set")
		}
		cmd = exec.Command(path, args...)
	default:
		return nil, fmt.Errorf("unknown execution mode: %q", mode)
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("execute workload (%s): %w (stderr: %s)", mode, err, stderr.String())
	}

	result, err := ParseResult(stdout.String())
	if err != nil {
		return nil, fmt.Errorf("parse result: %w (stdout: %s, stderr: %s)", err, stdout.String(), stderr.String())
	}

	return result, nil
}
