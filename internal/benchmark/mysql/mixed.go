package mysql

import (
	"fmt"

	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
)

func (m *MySQLBenchmarker) RunMixedWorkload(keyType string, initialDataset, totalOps, connections int, insertWeight, readWeight, updateWeight int) (*workload.WorkloadResult, error) {
	// Create initial dataset
	fmt.Printf("Creating initial dataset (%d records)...\n", initialDataset)
	_, err := m.InsertRecords(keyType, initialDataset, 100, 1)
	if err != nil {
		return nil, fmt.Errorf("create initial dataset: %w", err)
	}

	// Capture page splits after initial dataset, before mixed workload.
	// Matches MongoDB/Cassandra pattern (CaptureMetricsBefore after initial insert).
	m.CapturePageSplitsBefore()

	return workload.Execute(workload.ExecutorConfig{
		ContainerName:    ContainerName,
		DBType:           "mysql",
		Op:               "mixed",
		KeyType:          keyType,
		NumOps:           totalOps,
		Threads:          connections,
		ConnectionString: WorkloadConnString,
		TableName:        m.tableName,
		InsertPct:        insertWeight,
		ReadPct:          readWeight,
		UpdatePct:        updateWeight,
	})
}
