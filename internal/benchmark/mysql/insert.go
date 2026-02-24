package mysql

import (
	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
)

func (m *MySQLBenchmarker) InsertRecords(keyType string, numRecords, batchSize, connections int) (*workload.WorkloadResult, error) {
	return workload.Execute(workload.ExecutorConfig{
		ContainerName:    ContainerName,
		DBType:           "mysql",
		Op:               "insert",
		KeyType:          keyType,
		NumRecords:       numRecords,
		BatchSize:        batchSize,
		Threads:          connections,
		ConnectionString: WorkloadConnString,
		TableName:        m.tableName,
	})
}

func (m *MySQLBenchmarker) InsertRecordsSingle(keyType string, numRecords, batchSize int) (*workload.WorkloadResult, error) {
	return m.InsertRecords(keyType, numRecords, batchSize, 1)
}
