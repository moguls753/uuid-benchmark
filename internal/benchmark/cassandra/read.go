package cassandra

import (
	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
)

func (c *CassandraBenchmarker) ReadRecords(keyType string, numOps, connections int, connString string, execMode workload.ExecutionMode, consistency string) (*workload.WorkloadResult, error) {
	cfg := workload.ExecutorConfig{
		Mode:             execMode,
		DBType:           "cassandra",
		Op:               "read",
		KeyType:          keyType,
		NumOps:           numOps,
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
