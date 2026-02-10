package cassandra

import (
	"fmt"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
)

func (c *CassandraBenchmarker) InsertRecords(keyType string, numRecords, batchSize, connections int) (*workload.WorkloadResult, error) {
	if err := c.CaptureMetricsBefore(); err != nil {
		fmt.Printf("Warning: Could not capture metrics before insert: %v\n", err)
	}

	return workload.Execute(workload.ExecutorConfig{
		ContainerName:    ContainerName,
		DBType:           "cassandra",
		Op:               "insert",
		KeyType:          keyType,
		NumRecords:       numRecords,
		BatchSize:        batchSize,
		Threads:          connections,
		ConnectionString: WorkloadConnString,
	})
}

func (c *CassandraBenchmarker) InsertRecordsSingle(keyType string, numRecords, batchSize int) (time.Duration, error) {
	result, err := c.InsertRecords(keyType, numRecords, batchSize, 1)
	if err != nil {
		return 0, err
	}
	return result.Duration, nil
}
