package pgbench

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// ExecutorConfig holds configuration for pgbench execution
type ExecutorConfig struct {
	ContainerName string
	Connections   int    // Number of concurrent clients (-c flag)
	Transactions  int    // Total transactions to run (-t flag)
	ScriptPath    string // Path to SQL script inside container
	Duration      int    // Duration in seconds (-T flag, alternative to -t)
}

// ExecuteResult holds the output from pgbench execution
type ExecuteResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

// Execute runs pgbench inside the container with the given configuration
func Execute(cfg ExecutorConfig) (*ExecuteResult, error) {
	if cfg.ContainerName == "" {
		return nil, fmt.Errorf("container name is required")
	}
	if cfg.ScriptPath == "" {
		return nil, fmt.Errorf("script path is required")
	}
	if cfg.Transactions == 0 && cfg.Duration == 0 {
		return nil, fmt.Errorf("either transactions (-t) or duration (-T) must be specified")
	}

	// Build pgbench command
	args := []string{
		"exec",
		cfg.ContainerName,
		"pgbench",
		"-U", "benchmark",
		"-d", "uuid_benchmark",
		"-n",                                      // Skip vacuum (we have custom tables, not pgbench defaults)
		"-c", fmt.Sprintf("%d", cfg.Connections),
		"-j", fmt.Sprintf("%d", cfg.Connections), // Number of threads (match connections)
		"-f", cfg.ScriptPath,
		"--progress=1", // Show progress every 1 second
	}

	// Add either transactions or duration
	if cfg.Transactions > 0 {
		args = append(args, "-t", fmt.Sprintf("%d", cfg.Transactions))
	} else {
		args = append(args, "-T", fmt.Sprintf("%d", cfg.Duration))
	}

	cmd := exec.Command("docker", args...)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	result := &ExecuteResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: 0,
	}

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		} else {
			return nil, fmt.Errorf("failed to execute pgbench: %w", err)
		}
	}

	return result, nil
}

// CopyScriptToContainer copies a SQL script to the container's /tmp directory
func CopyScriptToContainer(containerName, scriptContent, scriptName string) (string, error) {
	// Create temporary file on host
	tmpFile, err := os.CreateTemp("", scriptName)
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	// Write script content
	if _, err := tmpFile.WriteString(scriptContent); err != nil {
		return "", fmt.Errorf("failed to write script: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return "", fmt.Errorf("failed to close temp file: %w", err)
	}

	// Copy to container
	containerPath := filepath.Join("/tmp", scriptName)
	cmd := exec.Command("docker", "cp", tmpFile.Name(), fmt.Sprintf("%s:%s", containerName, containerPath))

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to copy script to container: %w (stderr: %s)", err, stderr.String())
	}

	return containerPath, nil
}

// ExecuteSQL executes arbitrary SQL via psql (for setup/teardown, not workload)
func ExecuteSQL(containerName, sql string) error {
	cmd := exec.Command("docker", "exec", containerName,
		"psql", "-U", "benchmark", "-d", "uuid_benchmark", "-c", sql)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to execute SQL: %w (stderr: %s)", err, stderr.String())
	}

	return nil
}

// ExecuteSQLFile executes a SQL file inside the container via psql
func ExecuteSQLFile(containerName, filePath string) (*ExecuteResult, error) {
	cmd := exec.Command("docker", "exec", containerName,
		"psql", "-U", "benchmark", "-d", "uuid_benchmark", "-f", filePath)

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()

	result := &ExecuteResult{
		Stdout:   stdout.String(),
		Stderr:   stderr.String(),
		ExitCode: 0,
	}

	if err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		} else {
			return nil, fmt.Errorf("failed to execute SQL file: %w", err)
		}
	}

	return result, nil
}
