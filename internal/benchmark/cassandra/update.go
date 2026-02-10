package cassandra

import (
	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
)

func (c *CassandraBenchmarker) UpdateRecords(keyType string, numOps, connections int) (*workload.WorkloadResult, error) {
	return workload.Execute(workload.ExecutorConfig{
		ContainerName:    ContainerName,
		DBType:           "cassandra",
		Op:               "update",
		KeyType:          keyType,
		NumOps:           numOps,
		Threads:          connections,
		ConnectionString: WorkloadConnString,
	})
}
