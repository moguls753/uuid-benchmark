package cassandra

import (
	"fmt"

	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
)

func (c *CassandraBenchmarker) RunMixedWorkload(keyType string, initialDataset, totalOps, connections int, insertWeight, readWeight, updateWeight int) (*workload.WorkloadResult, error) {
	// Create initial dataset
	fmt.Printf("Creating initial dataset (%d records)...\n", initialDataset)
	_, err := c.InsertRecords(keyType, initialDataset, 100, 1)
	if err != nil {
		return nil, fmt.Errorf("create initial dataset: %w", err)
	}

	if err := c.CaptureMetricsBefore(); err != nil {
		fmt.Printf("Warning: Could not capture metrics before mixed workload: %v\n", err)
	}

	var op string
	switch {
	case insertWeight == 90:
		op = "mixed-insert-heavy"
	case readWeight == 90:
		op = "mixed-read-heavy"
	default:
		op = "mixed-balanced"
	}

	return workload.Execute(workload.ExecutorConfig{
		ContainerName:    ContainerName,
		DBType:           "cassandra",
		Op:               op,
		KeyType:          keyType,
		NumOps:           totalOps,
		Threads:          connections,
		ConnectionString: WorkloadConnString,
	})
}
