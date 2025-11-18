package docker

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

// IOStats represents I/O statistics from cgroup io.stat
type IOStats struct {
	ReadBytes  uint64
	WriteBytes uint64
	ReadOps    uint64
	WriteOps   uint64
	Timestamp  time.Time
}

// IOMetrics represents calculated I/O metrics over a time period
type IOMetrics struct {
	ReadIOPS          float64
	WriteIOPS         float64
	ReadThroughputMB  float64
	WriteThroughputMB float64
}

// GetContainerIOStats reads I/O statistics from cgroup v2 io.stat for a container
func GetContainerIOStats(containerName string) (*IOStats, error) {
	// Path to cgroup v2 io.stat for the container
	// Docker containers are typically under /sys/fs/cgroup/system.slice/docker-<container_id>.scope/
	// But we can also find them by container name via docker inspect

	// For simplicity, we'll search for the container in the cgroup hierarchy
	cgroupPath, err := findContainerCgroupPath(containerName)
	if err != nil {
		return nil, fmt.Errorf("failed to find container cgroup: %w", err)
	}

	ioStatPath := cgroupPath + "/io.stat"
	file, err := os.Open(ioStatPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open io.stat: %w", err)
	}
	defer file.Close()

	stats := &IOStats{
		Timestamp: time.Now(),
	}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		// Format: <major>:<minor> rbytes=X wbytes=Y rios=Z wios=W
		// Example: 259:0 rbytes=12345 wbytes=67890 rios=100 wios=200

		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		// Parse key=value pairs
		for _, field := range fields[1:] {
			parts := strings.Split(field, "=")
			if len(parts) != 2 {
				continue
			}

			key := parts[0]
			value, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				continue
			}

			switch key {
			case "rbytes":
				stats.ReadBytes += value
			case "wbytes":
				stats.WriteBytes += value
			case "rios":
				stats.ReadOps += value
			case "wios":
				stats.WriteOps += value
			}
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading io.stat: %w", err)
	}

	return stats, nil
}

// CalculateIOMetrics calculates I/O metrics from two IOStats snapshots
func CalculateIOMetrics(start, end *IOStats) IOMetrics {
	duration := end.Timestamp.Sub(start.Timestamp).Seconds()
	if duration <= 0 {
		return IOMetrics{}
	}

	readBytes := float64(end.ReadBytes - start.ReadBytes)
	writeBytes := float64(end.WriteBytes - start.WriteBytes)
	readOps := float64(end.ReadOps - start.ReadOps)
	writeOps := float64(end.WriteOps - start.WriteOps)

	return IOMetrics{
		ReadIOPS:          readOps / duration,
		WriteIOPS:         writeOps / duration,
		ReadThroughputMB:  readBytes / duration / (1024 * 1024),
		WriteThroughputMB: writeBytes / duration / (1024 * 1024),
	}
}

// findContainerCgroupPath finds the cgroup path for a Docker container
func findContainerCgroupPath(containerName string) (string, error) {
	// First, try to get container ID from docker
	// We'll use the container name directly and search in the cgroup hierarchy

	// Common paths for Docker containers in cgroup v2:
	// /sys/fs/cgroup/system.slice/docker-<container_id>.scope
	// /sys/fs/cgroup/docker/<container_id>

	// We'll use docker inspect to get the container ID
	containerID, err := getContainerID(containerName)
	if err != nil {
		return "", err
	}

	// Try different possible paths
	possiblePaths := []string{
		fmt.Sprintf("/sys/fs/cgroup/system.slice/docker-%s.scope", containerID),
		fmt.Sprintf("/sys/fs/cgroup/docker/%s", containerID),
	}

	for _, path := range possiblePaths {
		if _, err := os.Stat(path + "/io.stat"); err == nil {
			return path, nil
		}
	}

	return "", fmt.Errorf("could not find cgroup path for container %s (ID: %s)", containerName, containerID)
}

// getContainerID retrieves the container ID from the container name using docker ps
func getContainerID(containerName string) (string, error) {
	// Use docker ps to get the full container ID
	cmd := exec.Command("docker", "ps", "--filter", "name="+containerName, "--format", "{{.ID}}", "--no-trunc")

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
