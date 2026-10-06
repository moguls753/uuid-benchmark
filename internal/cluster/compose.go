package cluster

import (
	"os/exec"
	"time"
)

// composeCmd invokes docker compose with V2-then-V1 fallback. The V1
// fallback (`docker-compose`, hyphenated) is required on the FernUni
// HPC nodes which ship V1 only. Duplicates internal/container.composeCmd
// to avoid a cycle (container imports the per-DB benchmark packages).
func composeCmd(composeFile string, args ...string) *exec.Cmd {
	if exec.Command("docker", "compose", "version").Run() == nil {
		return exec.Command("docker", append([]string{"compose", "-f", composeFile}, args...)...)
	}
	return exec.Command("docker-compose", append([]string{"-f", composeFile}, args...)...)
}

const (
	startRetries = 3
	retryBackoff = 5 * time.Second
)
