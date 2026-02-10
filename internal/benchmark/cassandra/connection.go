package cassandra

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

const (
	dbHost   = "localhost"
	dbPort   = 9042
	keyspace = "uuid_benchmark"

	// ContainerName is the Docker container name for Cassandra.
	ContainerName = "uuid-bench-cassandra"

	// WorkloadConnString is the host used by the workload binary
	// running inside the container (connects via loopback).
	WorkloadConnString = "127.0.0.1"
)

func (c *CassandraBenchmarker) Connect() error {
	cluster := gocql.NewCluster(dbHost)
	cluster.Port = dbPort
	cluster.Consistency = gocql.LocalOne
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 30 * time.Second

	// First connect without keyspace to create it
	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}

	// Create keyspace if not exists
	err = session.Query(`
		CREATE KEYSPACE IF NOT EXISTS uuid_benchmark
		WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
	`).Exec()
	if err != nil {
		session.Close()
		return fmt.Errorf("create keyspace: %w", err)
	}
	session.Close()

	// Reconnect with keyspace
	cluster.Keyspace = keyspace
	session, err = cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("create session with keyspace: %w", err)
	}

	c.session = session
	return nil
}

func (c *CassandraBenchmarker) CreateTable(keyType string) error {
	c.keyType = keyType
	c.tableName = "bench"

	// Drop existing table
	_ = c.session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", keyspace, c.tableName)).Exec()

	var createSQL string
	switch keyType {
	case "bigserial":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s.%s (
				id bigint PRIMARY KEY,
				payload blob
			) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}
		`, keyspace, c.tableName)
	case "uuidv1":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s.%s (
				id timeuuid PRIMARY KEY,
				payload blob
			) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}
		`, keyspace, c.tableName)
	case "uuidv4", "uuidv7":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s.%s (
				id uuid PRIMARY KEY,
				payload blob
			) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}
		`, keyspace, c.tableName)
	case "ulid", "ulid_monotonic":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s.%s (
				id blob PRIMARY KEY,
				payload blob
			) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}
		`, keyspace, c.tableName)
	default:
		return fmt.Errorf("unknown key type: %s", keyType)
	}

	if err := c.session.Query(createSQL).Exec(); err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	return nil
}

func (c *CassandraBenchmarker) Close() error {
	if c.session != nil {
		c.session.Close()
	}
	return nil
}

func WaitForReady() error {
	timeout := 90 * time.Second
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		cluster := gocql.NewCluster(dbHost)
		cluster.Port = dbPort
		cluster.Timeout = 5 * time.Second
		cluster.ConnectTimeout = 5 * time.Second

		session, err := cluster.CreateSession()
		if err == nil {
			session.Close()
			return nil
		}
		time.Sleep(2 * time.Second)
	}

	return fmt.Errorf("timeout waiting for Cassandra after %v", timeout)
}

func (c *CassandraBenchmarker) ResetStats() error {
	// Cassandra doesn't have a direct stats reset mechanism
	// Metrics are tracked via deltas (before/after snapshots)
	return nil
}
