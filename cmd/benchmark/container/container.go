package container

import (
	"fmt"
	"log"
	"os/exec"

	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres"
)

// Config defines configuration for database container startup
type Config struct {
	Name         string       // Database name (e.g., "PostgreSQL", "MySQL")
	ComposeFile  string       // Path to docker-compose file
	WaitForReady func() error // Database-specific readiness check
}

// PostgresConfig is the default configuration for PostgreSQL containers
var PostgresConfig = Config{
	Name:         "PostgreSQL",
	ComposeFile:  "docker/docker-compose.postgres.yml",
	WaitForReady: postgres.WaitForReady,
}

// Start starts a database container with the given configuration
func Start(cfg Config) {
	fmt.Printf("🐳 Starting fresh %s container...\n", cfg.Name)

	cmd := exec.Command("docker", "compose", "-f", cfg.ComposeFile, "up", "-d")
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("Failed to start container: %v\nOutput: %s", err, string(output))
	}

	// Wait for database to be ready using database-specific check
	fmt.Printf("⏳ Waiting for %s to initialize...\n", cfg.Name)
	if err := cfg.WaitForReady(); err != nil {
		log.Fatalf("%s failed to start: %v", cfg.Name, err)
	}

	fmt.Println("✅ Container ready\n")
}

// Stop stops and removes a database container
func Stop(composeFile string) {
	fmt.Println("\n🧹 Cleaning up container...")

	cmd := exec.Command("docker", "compose", "-f", composeFile, "down", "-v")
	// Ignore errors on cleanup - container might already be stopped
	cmd.Run()

	fmt.Println("✅ Container stopped and removed")
}
