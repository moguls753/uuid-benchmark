package cassandra

import (
	"github.com/gocql/gocql"
)

type CassandraBenchmarker struct {
	session       *gocql.Session
	keyType       string
	tableName     string
	metricsBefore *CassandraMetricsSnapshot
}

func New() *CassandraBenchmarker {
	return &CassandraBenchmarker{}
}
