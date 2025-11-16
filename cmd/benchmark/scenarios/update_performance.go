package scenarios

import (
	"fmt"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/docker"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres"
)

// UpdatePerformance evaluates update operation performance and fragmentation impact
//
// This scenario measures:
//   - Update throughput
//   - Update latency percentiles
//   - Fragmentation after update cycles
//
// Workflow:
//  1. Insert records (creates initial dataset)
//  2. Run update workload (point or batch updates)
//  3. Measure fragmentation and performance
//
// Thesis relevance: Updates can cause additional page splits for fragmented
// indexes, potentially worsening performance for UUIDv4 compared to sequential keys.
func UpdatePerformance(keyType string, numRecords, numUpdates, batchSize int) (*benchmark.UpdatePerformanceResult, error) {
	bench := postgres.New()

	// Connect to database
	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	// Create table for benchmark
	if err := bench.CreateTable(keyType); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	result := &benchmark.UpdatePerformanceResult{
		KeyType:    keyType,
		NumRecords: numRecords,
		NumUpdates: numUpdates,
		BatchSize:  batchSize,
	}

	// Step 1: Insert records to create initial dataset
	fmt.Printf("→ Inserting %d records...\n", numRecords)
	_, err := bench.InsertRecords(keyType, numRecords, 100) // Batch size 100 for speed
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}
	fmt.Printf("✓ Inserted %d records\n", numRecords)

	// Step 2: Run update workload
	fmt.Printf("→ Running %d updates (batch size=%d)...\n", numUpdates, batchSize)

	// Capture I/O stats before updates
	ioStatsBefore, err := docker.GetContainerIOStats("uuid-bench-postgres")
	if err != nil {
		fmt.Printf("⚠ Failed to capture I/O stats before updates: %v\n", err)
	}

	var updateResult *benchmark.UpdateBenchmarkResult
	if batchSize > 1 {
		updateResult, err = bench.UpdateBatchRecords(keyType, numUpdates, batchSize, numRecords)
	} else {
		updateResult, err = bench.UpdateRandomRecords(keyType, numUpdates, numRecords)
	}
	if err != nil {
		return nil, fmt.Errorf("update records: %w", err)
	}

	// Capture I/O stats after updates
	ioStatsAfter, err := docker.GetContainerIOStats("uuid-bench-postgres")
	if err != nil {
		fmt.Printf("⚠ Failed to capture I/O stats after updates: %v\n", err)
	}

	// Calculate I/O metrics
	if ioStatsBefore != nil && ioStatsAfter != nil {
		ioMetrics := docker.CalculateIOMetrics(ioStatsBefore, ioStatsAfter)
		result.ReadIOPS = ioMetrics.ReadIOPS
		result.WriteIOPS = ioMetrics.WriteIOPS
		result.ReadThroughputMB = ioMetrics.ReadThroughputMB
		result.WriteThroughputMB = ioMetrics.WriteThroughputMB
	}

	result.UpdateDuration = updateResult.Duration
	result.UpdateThroughput = updateResult.Throughput
	result.LatencyP50 = updateResult.LatencyP50
	result.LatencyP95 = updateResult.LatencyP95
	result.LatencyP99 = updateResult.LatencyP99

	fmt.Printf("✓ Completed %d updates in %s\n", numUpdates, updateResult.Duration)
	fmt.Printf("✓ Update throughput: %.2f ops/sec\n", updateResult.Throughput)

	// Step 3: Measure fragmentation after updates
	fmt.Println("→ Measuring fragmentation...")
	metrics, err := bench.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}
	result.Fragmentation = metrics.Fragmentation

	return result, nil
}
