package pgbench

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ExecutorConfig struct {
	ContainerName string
	Connections   int
	Transactions  int
	ScriptPath    string
	Duration      int
}

type ExecuteResult struct {
	Stdout   string
	Stderr   string
	ExitCode int
}

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

	args := []string{
		"exec",
		cfg.ContainerName,
		"pgbench",
		"-U", "benchmark",
		"-d", "uuid_benchmark",
		"-n",
		"-r",
		"-l",
		"-c", fmt.Sprintf("%d", cfg.Connections),
		"-j", fmt.Sprintf("%d", cfg.Connections),
		"-f", cfg.ScriptPath,
	}

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

func CopyScriptToContainer(containerName, scriptContent, scriptName string) (string, error) {
	tmpFile, err := os.CreateTemp("", scriptName)
	if err != nil {
		return "", fmt.Errorf("failed to create temp file: %w", err)
	}
	defer os.Remove(tmpFile.Name())

	if _, err := tmpFile.WriteString(scriptContent); err != nil {
		return "", fmt.Errorf("failed to write script: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return "", fmt.Errorf("failed to close temp file: %w", err)
	}

	containerPath := filepath.Join("/tmp", scriptName)
	cmd := exec.Command("docker", "cp", tmpFile.Name(), fmt.Sprintf("%s:%s", containerName, containerPath))

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("failed to copy script to container: %w (stderr: %s)", err, stderr.String())
	}

	return containerPath, nil
}

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

// ParseLogFilesForPercentiles reads pgbench log files and calculates latency percentiles
func ParseLogFilesForPercentiles(containerName string) (p50, p95, p99 time.Duration, err error) {
	// List all pgbench log files in container (they're in the pgbench working directory)
	cmd := exec.Command("docker", "exec", containerName, "sh", "-c", "ls pgbench_log.* 2>/dev/null || true")
	var stdout bytes.Buffer
	cmd.Stdout = &stdout
	if err := cmd.Run(); err != nil {
		return 0, 0, 0, fmt.Errorf("failed to list log files: %w", err)
	}

	logFiles := strings.Fields(stdout.String())
	if len(logFiles) == 0 {
		return 0, 0, 0, fmt.Errorf("no pgbench log files found")
	}

	// Read and parse all log files
	var latencies []int64 // latencies in microseconds

	for _, logFile := range logFiles {
		cmd := exec.Command("docker", "exec", containerName, "cat", logFile)
		var output bytes.Buffer
		cmd.Stdout = &output
		if err := cmd.Run(); err != nil {
			continue // Skip files we can't read
		}

		// Parse log file: format is "client_id txn_id latency_us script_no epoch_sec epoch_usec"
		lines := strings.Split(output.String(), "\n")
		for _, line := range lines {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 3 {
				continue
			}
			latencyUS, err := strconv.ParseInt(fields[2], 10, 64)
			if err != nil {
				continue
			}
			latencies = append(latencies, latencyUS)
		}
	}

	if len(latencies) == 0 {
		return 0, 0, 0, fmt.Errorf("no latency data found in log files")
	}

	// Sort latencies for percentile calculation
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	// Calculate percentiles
	p50Index := int(float64(len(latencies)) * 0.50)
	p95Index := int(float64(len(latencies)) * 0.95)
	p99Index := int(float64(len(latencies)) * 0.99)

	if p50Index >= len(latencies) {
		p50Index = len(latencies) - 1
	}
	if p95Index >= len(latencies) {
		p95Index = len(latencies) - 1
	}
	if p99Index >= len(latencies) {
		p99Index = len(latencies) - 1
	}

	p50 = time.Duration(latencies[p50Index]) * time.Microsecond
	p95 = time.Duration(latencies[p95Index]) * time.Microsecond
	p99 = time.Duration(latencies[p99Index]) * time.Microsecond

	return p50, p95, p99, nil
}

// CleanupLogFiles removes pgbench log files from the container
func CleanupLogFiles(containerName string) error {
	cmd := exec.Command("docker", "exec", containerName, "sh", "-c", "rm -f pgbench_log.*")
	return cmd.Run()
}
