package cassandra

import (
	"github.com/gocql/gocql"

	"github.com/moguls753/uuid-benchmark/internal/cluster"
)

type CassandraBenchmarker struct {
	session   *gocql.Session
	keyType   string
	tableName string
	// metricsBeforeNodes holds the per-node before-snapshot from the most
	// recent CaptureMetricsBeforeAll. Stored per-node (rather than as a
	// pre-aggregated cluster snapshot) so MeasureMetricsAll can compute
	// per-node deltas with negative-delta clamping applied before summing —
	// avoids the masking that would happen if one node's compaction-induced
	// decrease cancelled another node's workload-induced increase in the
	// cluster sum. See buildBenchmarkResultPerNode.
	metricsBeforeNodes []*CassandraMetricsSnapshot
	cfg                cluster.ClusterConfig
}

func New(cfg cluster.ClusterConfig) *CassandraBenchmarker {
	return &CassandraBenchmarker{
		cfg: cfg,
	}
}
