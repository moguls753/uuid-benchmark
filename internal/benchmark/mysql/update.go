package mysql

import (
	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
)

func (m *MySQLBenchmarker) UpdateRecords(keyType string, numOps, connections int) (*workload.WorkloadResult, error) {
	return workload.Execute(workload.ExecutorConfig{
		ContainerName:    ContainerName,
		DBType:           "mysql",
		Op:               "update",
		KeyType:          keyType,
		NumOps:           numOps,
		Threads:          connections,
		ConnectionString: WorkloadConnString,
		TableName:        m.tableName,
	})
}
