package scenarios

import (
	"fmt"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/docker"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres"
)

// ReadAfterFragmentation evaluates memory efficiency impact of index fragmentation
//
// This scenario measures:
//   - Insert performance (creates fragmented index for UUIDv4)
//   - Read performance after fragmentation
//   - Buffer pool hit ratio (shows cache efficiency)
//   - Index buffer hit ratio (shows index-specific cache behavior)
//   - Read latency percentiles
//
// Workflow:
//  1. Insert records (creates fragmentation)
//  2. Reset PostgreSQL statistics
//  3. Run read workload (point lookups)
//  4. Measure buffer hit ratios
//
// Thesis relevance: Demonstrates that UUIDv4's fragmentation leads to lower
// buffer hit ratios (more disk I/O) compared to sequential keys. This proves
// fragmented indexes require more memory OR suffer worse performance.
//
// Expected results:
//   - BIGSERIAL: ~99% hit ratio (sequential pages, good locality)
//   - UUIDv4: ~70-85% hit ratio (scattered pages, poor locality)
func ReadAfterFragmentation(keyType string, numRecords, numReads int) (*benchmark.ReadAfterFragmentationResult, error) {
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

	result := &benchmark.ReadAfterFragmentationResult{
		KeyType:    keyType,
		NumRecords: numRecords,
		NumReads:   numReads,
	}

	// Step 1: Insert records to create fragmentation
	fmt.Printf("→ Inserting %d records to create index...\n", numRecords)
	insertDuration, err := bench.InsertRecords(keyType, numRecords, 100) // Batch size 100 for speed
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}
	result.InsertDuration = insertDuration
	fmt.Printf("✓ Inserted %d records in %s\n", numRecords, insertDuration)

	// Measure fragmentation after inserts
	fmt.Println("→ Measuring fragmentation...")
	metrics, err := bench.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}
	result.Fragmentation = metrics.Fragmentation
	fmt.Printf("✓ Index fragmentation: %.2f%%\n", metrics.Fragmentation.FragmentationPercent)

	// Step 2: Reset PostgreSQL statistics to measure ONLY the read workload
	fmt.Println("→ Resetting PostgreSQL statistics...")
	if err := bench.ResetStats(); err != nil {
		return nil, fmt.Errorf("reset stats: %w", err)
	}

	// Step 3: Run read workload (point lookups)
	fmt.Printf("→ Running %d point lookups...\n", numReads)

	// Capture I/O stats before read workload
	ioStatsBefore, err := docker.GetContainerIOStats("uuid-bench-postgres")
	if err != nil {
		fmt.Printf("⚠ Failed to capture I/O stats before reads: %v\n", err)
	}

	readResult, err := bench.ReadRandomRecords(keyType, numReads, numRecords)
	if err != nil {
		return nil, fmt.Errorf("read records: %w", err)
	}
	result.ReadDuration = readResult.Duration
	result.ReadThroughput = readResult.Throughput
	result.LatencyP50 = readResult.LatencyP50
	result.LatencyP95 = readResult.LatencyP95
	result.LatencyP99 = readResult.LatencyP99

	// Capture I/O stats after read workload
	ioStatsAfter, err := docker.GetContainerIOStats("uuid-bench-postgres")
	if err != nil {
		fmt.Printf("⚠ Failed to capture I/O stats after reads: %v\n", err)
	}

	// Calculate I/O metrics
	if ioStatsBefore != nil && ioStatsAfter != nil {
		ioMetrics := docker.CalculateIOMetrics(ioStatsBefore, ioStatsAfter)
		result.ReadIOPS = ioMetrics.ReadIOPS
		result.WriteIOPS = ioMetrics.WriteIOPS
		result.ReadThroughputMB = ioMetrics.ReadThroughputMB
		result.WriteThroughputMB = ioMetrics.WriteThroughputMB
	}

	fmt.Printf("✓ Completed %d reads in %s\n", numReads, readResult.Duration)
	fmt.Printf("✓ Read throughput: %.2f ops/sec\n", readResult.Throughput)

	// Step 4: Measure buffer hit ratios
	fmt.Println("→ Measuring buffer pool hit ratios...")
	finalMetrics, err := bench.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure final metrics: %w", err)
	}
	result.BufferHitRatio = finalMetrics.BufferHitRatio
	result.IndexBufferHitRatio = finalMetrics.IndexBufferHitRatio

	return result, nil
}
