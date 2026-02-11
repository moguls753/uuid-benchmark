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
	ContainerName    string
	DBType           string // mongodb or cassandra
	Op               string // insert, read, update, mixed
	KeyType          string
	NumRecords       int
	NumOps           int
	BatchSize        int
	Threads          int
	ConnectionString string
	InsertPct        int // Insert percentage for mixed workload
	ReadPct          int // Read percentage for mixed workload
	UpdatePct        int // Update percentage for mixed workload
}

// Execute runs the workload binary inside a container and returns parsed results.
func Execute(cfg ExecutorConfig) (*WorkloadResult, error) {
	if err := CopyToContainer(cfg.ContainerName); err != nil {
		return nil, fmt.Errorf("copy to container: %w", err)
	}

	args := []string{
		"exec", cfg.ContainerName,
		"/tmp/workload",
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

	if cfg.Op == "mixed" {
		args = append(args,
			"--insert-pct", fmt.Sprintf("%d", cfg.InsertPct),
			"--read-pct", fmt.Sprintf("%d", cfg.ReadPct),
			"--update-pct", fmt.Sprintf("%d", cfg.UpdatePct),
		)
	}

	cmd := exec.Command("docker", args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("execute workload: %w (stderr: %s)", err, stderr.String())
	}

	result, err := ParseResult(stdout.String())
	if err != nil {
		return nil, fmt.Errorf("parse result: %w (stdout: %s, stderr: %s)", err, stdout.String(), stderr.String())
	}

	return result, nil
}
