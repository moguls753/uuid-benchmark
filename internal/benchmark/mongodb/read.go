package mongodb

import (
	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
)

func (m *MongoDBBenchmarker) ReadRecords(keyType string, numOps, connections int) (*workload.WorkloadResult, error) {
	return workload.Execute(workload.ExecutorConfig{
		ContainerName:    ContainerName,
		DBType:           "mongodb",
		Op:               "read",
		KeyType:          keyType,
		NumOps:           numOps,
		Threads:          connections,
		ConnectionString: WorkloadConnString,
	})
}
