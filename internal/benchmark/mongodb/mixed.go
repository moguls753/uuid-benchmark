package mongodb

import (
	"fmt"

	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
)

func (m *MongoDBBenchmarker) RunMixedWorkload(keyType string, initialDataset, totalOps, connections int, insertWeight, readWeight, updateWeight int) (*workload.WorkloadResult, error) {
	// Create initial dataset
	fmt.Printf("Creating initial dataset (%d records)...\n", initialDataset)
	_, err := m.InsertRecords(keyType, initialDataset, 100, 1)
	if err != nil {
		return nil, fmt.Errorf("create initial dataset: %w", err)
	}

	if err := m.CaptureMetricsBefore(); err != nil {
		fmt.Printf("Warning: Could not capture metrics before mixed workload: %v\n", err)
	}

	return workload.Execute(workload.ExecutorConfig{
		ContainerName:    ContainerName,
		DBType:           "mongodb",
		Op:               "mixed",
		KeyType:          keyType,
		NumOps:           totalOps,
		Threads:          connections,
		ConnectionString: WorkloadConnString,
		InsertPct:        insertWeight,
		ReadPct:          readWeight,
		UpdatePct:        updateWeight,
	})
}
