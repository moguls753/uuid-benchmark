package cassandra

import (
	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
)

// InsertRecords runs the insert workload binary against the configured
// cluster. The runner owns metric-capture timing (CaptureMetricsBeforeAll
// + MeasureMetricsAll); this method is purely the workload execution.
// See internal/runner/cassandra.go for the surrounding capture pattern.
func (c *CassandraBenchmarker) InsertRecords(keyType string, numRecords, batchSize, connections int, connString string, execMode workload.ExecutionMode, consistency string) (*workload.WorkloadResult, error) {
	cfg := workload.ExecutorConfig{
		Mode:             execMode,
		DBType:           "cassandra",
		Op:               "insert",
		KeyType:          keyType,
		NumRecords:       numRecords,
		BatchSize:        batchSize,
		Threads:          connections,
		ConnectionString: connString,
		NumBuckets:       c.cfg.NumBuckets,
		Consistency:      consistency,
	}
	if execMode == workload.ExecutionModeContainer {
		cfg.ContainerName = ContainerName
	}
	return workload.Execute(cfg)
}
