package cassandra

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"

	"github.com/moguls753/uuid-benchmark/internal/cluster"
)

// ContainerName is the Docker container name for Cassandra.
const ContainerName = "uuid-bench-cassandra"

func (c *CassandraBenchmarker) Connect() error {
	cl := gocql.NewCluster(c.cfg.ContactPoints...)
	cl.Consistency = parseConsistency(c.cfg.Consistency)
	cl.Timeout = 30 * time.Second
	cl.ConnectTimeout = 30 * time.Second
	cl.NumConns = 4

	// First session without keyspace, to create the keyspace.
	bootstrap, err := cl.CreateSession()
	if err != nil {
		return fmt.Errorf("bootstrap session: %w", err)
	}
	repl := replicationStmt(c.cfg)
	createKS := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = %s",
		c.cfg.Keyspace, repl,
	)
	if err := bootstrap.Query(createKS).Exec(); err != nil {
		bootstrap.Close()
		return fmt.Errorf("create keyspace: %w", err)
	}
	bootstrap.Close()

	cl.Keyspace = c.cfg.Keyspace
	c.session, err = cl.CreateSession()
	if err != nil {
		return fmt.Errorf("session with keyspace: %w", err)
	}
	return nil
}

// parseConsistency converts a typed cluster.Consistency into the matching gocql
// constant. Validate() rejects unknown values upstream, so the default branch
// is defense-in-depth only.
func parseConsistency(c cluster.Consistency) gocql.Consistency {
	switch c {
	case cluster.ConsistencyOne:
		return gocql.One
	case cluster.ConsistencyLocalOne:
		return gocql.LocalOne
	case cluster.ConsistencyLocalQuorum:
		return gocql.LocalQuorum
	case cluster.ConsistencyQuorum:
		return gocql.Quorum
	default:
		return gocql.LocalOne
	}
}

// replicationStmt returns the CQL replication map for the given cluster
// config. ModeLocalSingle uses SimpleStrategy (no DC awareness); the cluster
// modes use NetworkTopologyStrategy with DC name "dc1" (matches the
// docker-compose CASSANDRA_DC env var and the Taurus deployment plan).
func replicationStmt(c cluster.ClusterConfig) string {
	if c.Mode == cluster.ModeLocalSingle {
		return fmt.Sprintf("{'class': 'SimpleStrategy', 'replication_factor': %d}", c.ReplicationFactor)
	}
	return fmt.Sprintf("{'class': 'NetworkTopologyStrategy', 'dc1': %d}", c.ReplicationFactor)
}

func (c *CassandraBenchmarker) CreateTable(keyType string) error {
	c.keyType = keyType
	c.tableName = "bench"

	// Drop existing table
	_ = c.session.Query(fmt.Sprintf("DROP TABLE IF EXISTS %s.%s", c.cfg.Keyspace, c.tableName)).Exec()

	var createSQL string
	switch keyType {
	case "sequential":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s.%s (
				bucket int,
				id bigint,
				payload blob,
				PRIMARY KEY ((bucket), id)
			) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}
		`, c.cfg.Keyspace, c.tableName)
	case "uuidv1":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s.%s (
				bucket int,
				id timeuuid,
				payload blob,
				PRIMARY KEY ((bucket), id)
			) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}
		`, c.cfg.Keyspace, c.tableName)
	case "uuidv4", "uuidv7":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s.%s (
				bucket int,
				id uuid,
				payload blob,
				PRIMARY KEY ((bucket), id)
			) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}
		`, c.cfg.Keyspace, c.tableName)
	case "ulid", "ulid_monotonic":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s.%s (
				bucket int,
				id blob,
				payload blob,
				PRIMARY KEY ((bucket), id)
			) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}
		`, c.cfg.Keyspace, c.tableName)
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

func (c *CassandraBenchmarker) ResetStats() error {
	// Cassandra doesn't have a direct stats reset mechanism
	// Metrics are tracked via deltas (before/after snapshots)
	return nil
}
