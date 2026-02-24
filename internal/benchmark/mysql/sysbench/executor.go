package sysbench

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
	Threads       int    // Number of concurrent threads
	Events        int    // Total number of events (transactions)
	Duration      int    // Alternative: time-based (seconds)
	ScriptPath    string // Path inside container to lua script
	TableName     string
	DBName        string
	NumRecords    int // For scripts that need to know total record count
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
	if cfg.Events == 0 && cfg.Duration == 0 {
		return nil, fmt.Errorf("either events or duration must be specified")
	}

	// Build sysbench command
	args := []string{
		"exec",
		cfg.ContainerName,
		"sysbench",
		cfg.ScriptPath,
		"--mysql-host=127.0.0.1",
		"--mysql-port=3306",
		"--mysql-user=benchmark",
		"--mysql-password=benchmark123",
		fmt.Sprintf("--mysql-db=%s", cfg.DBName),
		fmt.Sprintf("--threads=%d", cfg.Threads),
		"--report-interval=0",
		"--histogram=on",
	}

	// Add table name if specified (for custom scripts)
	if cfg.TableName != "" {
		args = append(args, fmt.Sprintf("--table_name=%s", cfg.TableName))
	}

	// Add num_records if specified
	if cfg.NumRecords > 0 {
		args = append(args, fmt.Sprintf("--num_records=%d", cfg.NumRecords))
	}

	if cfg.Events > 0 {
		args = append(args, fmt.Sprintf("--events=%d", cfg.Events))
		args = append(args, "--time=0") // Disable default 10s time limit
	} else {
		args = append(args, fmt.Sprintf("--time=%d", cfg.Duration))
	}

	args = append(args, "run")

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
			return nil, fmt.Errorf("failed to execute sysbench: %w", err)
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
		"mysql", "-ubenchmark", "-pbenchmark123", "uuid_benchmark", "-e", sql)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("failed to execute SQL: %w (stderr: %s)", err, stderr.String())
	}

	return nil
}

func ExecuteSQLFile(containerName, filePath string) (*ExecuteResult, error) {
	cmd := exec.Command("docker", "exec", containerName,
		"mysql", "-ubenchmark", "-pbenchmark123", "uuid_benchmark", "-e", fmt.Sprintf("source %s", filePath))

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

// ParseLatencyHistogram reads sysbench histogram output for percentiles
func ParseLatencyHistogram(output string) (p50, p95, p99 time.Duration, err error) {
	// Sysbench histogram format (when --histogram=on):
	// Latency histogram (values are in milliseconds)
	//        value  ------------- distribution ------------- count
	//        0.147 |                                         1
	// ...
	// We also look for the percentile summary in the output

	lines := strings.Split(output, "\n")
	var latencies []float64

	inHistogram := false
	for _, line := range lines {
		line = strings.TrimSpace(line)

		if strings.Contains(line, "Latency histogram") {
			inHistogram = true
			continue
		}

		if inHistogram && strings.Contains(line, "value") && strings.Contains(line, "distribution") {
			continue
		}

		if inHistogram && line != "" {
			// Parse histogram line: "0.147 |*** 5"
			parts := strings.Split(line, "|")
			if len(parts) >= 2 {
				valueStr := strings.TrimSpace(parts[0])
				countPart := strings.TrimSpace(parts[1])
				// Extract count from end of line
				fields := strings.Fields(countPart)
				if len(fields) > 0 {
					countStr := fields[len(fields)-1]
					if val, e := strconv.ParseFloat(valueStr, 64); e == nil {
						if count, e2 := strconv.Atoi(countStr); e2 == nil {
							for i := 0; i < count; i++ {
								latencies = append(latencies, val)
							}
						}
					}
				}
			}
		}

		// Stop histogram parsing at next section
		if inHistogram && (strings.Contains(line, "Threads fairness") || line == "") {
			if len(latencies) > 0 {
				break
			}
		}
	}

	if len(latencies) == 0 {
		return 0, 0, 0, fmt.Errorf("no latency data found in histogram")
	}

	// Sort for percentile calculation
	sort.Float64s(latencies)

	n := len(latencies)
	p50Index := int(float64(n) * 0.50)
	p95Index := int(float64(n) * 0.95)
	p99Index := int(float64(n) * 0.99)

	if p50Index >= n {
		p50Index = n - 1
	}
	if p95Index >= n {
		p95Index = n - 1
	}
	if p99Index >= n {
		p99Index = n - 1
	}

	// Sysbench reports latencies in milliseconds
	p50 = time.Duration(latencies[p50Index] * float64(time.Millisecond))
	p95 = time.Duration(latencies[p95Index] * float64(time.Millisecond))
	p99 = time.Duration(latencies[p99Index] * float64(time.Millisecond))

	return p50, p95, p99, nil
}
