package cassandra

import (
	"github.com/gocql/gocql"

	"github.com/moguls753/uuid-benchmark/internal/cluster"
)

type CassandraBenchmarker struct {
	session       *gocql.Session
	keyType       string
	tableName     string
	metricsBefore *CassandraMetricsSnapshot
	cfg           cluster.ClusterConfig
}

func New(cfg cluster.ClusterConfig) *CassandraBenchmarker {
	return &CassandraBenchmarker{
		cfg: cfg,
	}
}
