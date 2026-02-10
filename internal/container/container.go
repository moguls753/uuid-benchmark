package container

import (
	"fmt"
	"log"
	"os/exec"

	"github.com/moguls753/uuid-benchmark/internal/benchmark/cassandra"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/mongodb"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/mysql"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
)

type Config struct {
	Name         string
	ComposeFile  string
	WaitForReady func() error
}

var PostgresConfig = Config{
	Name:         "PostgreSQL",
	ComposeFile:  "docker/docker-compose.postgres.yml",
	WaitForReady: postgres.WaitForReady,
}

var MySQLConfig = Config{
	Name:         "MySQL",
	ComposeFile:  "docker/docker-compose.mysql.yml",
	WaitForReady: mysql.WaitForReady,
}

var MongoDBConfig = Config{
	Name:         "MongoDB",
	ComposeFile:  "docker/docker-compose.mongo.yml",
	WaitForReady: mongodb.WaitForReady,
}

var CassandraConfig = Config{
	Name:         "Cassandra",
	ComposeFile:  "docker/docker-compose.cassandra.yml",
	WaitForReady: cassandra.WaitForReady,
}

func Start(cfg Config) {
	fmt.Printf("Starting fresh %s container...\n", cfg.Name)

	cmd := exec.Command("docker", "compose", "-f", cfg.ComposeFile, "up", "-d")
	output, err := cmd.CombinedOutput()
	if err != nil {
		log.Fatalf("Failed to start container: %v\nOutput: %s", err, string(output))
	}

	fmt.Printf("Waiting for %s to initialize...\n", cfg.Name)
	if err := cfg.WaitForReady(); err != nil {
		log.Fatalf("%s failed to start: %v", cfg.Name, err)
	}

	fmt.Println("Container ready")
}

func Stop(composeFile string) {
	fmt.Println("\nCleaning up container...")

	cmd := exec.Command("docker", "compose", "-f", composeFile, "down", "-v")
	// Ignore errors on cleanup - container might already be stopped
	cmd.Run()

	// Reset workload binary copy cache since container is destroyed
	workload.ResetCopyCache()

	fmt.Println("Container stopped and removed")
}
