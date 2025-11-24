package runner

import (
	"fmt"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	iometrics "github.com/moguls753/uuid-benchmark/internal/benchmark/io"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres"
)

// InsertPerformance evaluates disk efficiency during insert operations
//
// This scenario measures:
//   - Insert throughput
//   - Page splits (B-tree index maintenance cost)
//   - Index fragmentation
//   - Disk usage (table and index size)
//   - Latency percentiles (p50, p95, p99)
func InsertPerformance(keyType string, numRecords, batchSize, connections int) (*benchmark.InsertPerformanceResult, error) {
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

	fmt.Printf("Inserting %d records (connections=%d, batch=%d)...\n", numRecords, connections, batchSize)

	result := &benchmark.InsertPerformanceResult{
		KeyType:     keyType,
		NumRecords:  numRecords,
		BatchSize:   batchSize,
		Connections: connections,
	}

	// Capture I/O stats before insert
	ioStatsBefore, err := iometrics.GetContainerIOStats("uuid-bench-postgres")
	if err != nil {
		fmt.Printf("Warning:Failed to capture I/O stats before insert: %v\n", err)
	}

	// Execute insert operation (sequential or concurrent)
	// Using pgbench-based implementation for methodologically sound benchmarks
	if connections == 1 {
		// Sequential insert
		duration, err := bench.InsertRecordsPgbench(keyType, numRecords, batchSize)
		if err != nil {
			return nil, fmt.Errorf("insert records: %w", err)
		}
		result.Duration = duration
		result.Throughput = float64(numRecords) / duration.Seconds()
	} else {
		// Concurrent insert
		concResult, err := bench.InsertRecordsPgbenchConcurrent(keyType, numRecords, connections, batchSize)
		if err != nil {
			return nil, fmt.Errorf("insert records concurrent: %w", err)
		}
		result.Duration = concResult.Duration
		result.Throughput = concResult.Throughput
		result.LatencyP50 = concResult.LatencyP50
		result.LatencyP95 = concResult.LatencyP95
		result.LatencyP99 = concResult.LatencyP99
	}

	// Capture I/O stats after insert
	ioStatsAfter, err := iometrics.GetContainerIOStats("uuid-bench-postgres")
	if err != nil {
		fmt.Printf("Warning:Failed to capture I/O stats after insert: %v\n", err)
	}

	// Calculate I/O metrics
	if ioStatsBefore != nil && ioStatsAfter != nil {
		ioMetrics := iometrics.CalculateIOMetrics(ioStatsBefore, ioStatsAfter)
		result.ReadIOPS = ioMetrics.ReadIOPS
		result.WriteIOPS = ioMetrics.WriteIOPS
		result.ReadThroughputMB = ioMetrics.ReadThroughputMB
		result.WriteThroughputMB = ioMetrics.WriteThroughputMB
	}

	fmt.Printf("Inserted %d records in %s\n", numRecords, result.Duration)
	fmt.Printf("Throughput: %.2f records/sec\n", result.Throughput)

	// Measure metrics (page splits, fragmentation, disk usage)
	fmt.Println("Measuring metrics...")
	metrics, err := bench.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}

	// Populate result with metrics
	result.PageSplits = metrics.PageSplits
	result.TableSize = metrics.TableSize
	result.IndexSize = metrics.IndexSize
	result.Fragmentation = metrics.Fragmentation

	return result, nil
}

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
	fmt.Printf("Inserting %d records to create index...\n", numRecords)
	insertDuration, err := bench.InsertRecordsPgbench(keyType, numRecords, 100) // Batch size 100 for speed
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}
	result.InsertDuration = insertDuration
	fmt.Printf("Inserted %d records in %s\n", numRecords, insertDuration)

	// Measure fragmentation after inserts
	fmt.Println("Measuring fragmentation...")
	metrics, err := bench.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}
	result.Fragmentation = metrics.Fragmentation
	fmt.Printf("Index fragmentation: %.2f%%\n", metrics.Fragmentation.FragmentationPercent)

	// Step 2: Reset PostgreSQL statistics to measure ONLY the read workload
	fmt.Println("Resetting PostgreSQL statistics...")
	if err := bench.ResetStats(); err != nil {
		return nil, fmt.Errorf("reset stats: %w", err)
	}

	// Step 3: Run read workload (point lookups)
	fmt.Printf("Running %d point lookups...\n", numReads)

	// Capture I/O stats before read workload
	ioStatsBefore, err := iometrics.GetContainerIOStats("uuid-bench-postgres")
	if err != nil {
		fmt.Printf("Warning:Failed to capture I/O stats before reads: %v\n", err)
	}

	// Use pgbench for read workload
	readDuration, err := bench.ReadRecordsPgbench(keyType, numRecords, numReads)
	if err != nil {
		return nil, fmt.Errorf("read records: %w", err)
	}
	result.ReadDuration = readDuration
	result.ReadThroughput = float64(numReads) / readDuration.Seconds()
	// Note: Sequential pgbench doesn't provide latency percentiles
	// For latency, would need concurrent mode

	// Capture I/O stats after read workload
	ioStatsAfter, err := iometrics.GetContainerIOStats("uuid-bench-postgres")
	if err != nil {
		fmt.Printf("Warning:Failed to capture I/O stats after reads: %v\n", err)
	}

	// Calculate I/O metrics
	if ioStatsBefore != nil && ioStatsAfter != nil {
		ioMetrics := iometrics.CalculateIOMetrics(ioStatsBefore, ioStatsAfter)
		result.ReadIOPS = ioMetrics.ReadIOPS
		result.WriteIOPS = ioMetrics.WriteIOPS
		result.ReadThroughputMB = ioMetrics.ReadThroughputMB
		result.WriteThroughputMB = ioMetrics.WriteThroughputMB
	}

	fmt.Printf("Completed %d reads in %s\n", numReads, readDuration)
	fmt.Printf("Read throughput: %.2f ops/sec\n", result.ReadThroughput)

	// Step 4: Measure buffer hit ratios
	fmt.Println("Measuring buffer pool hit ratios...")
	finalMetrics, err := bench.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure final metrics: %w", err)
	}
	result.BufferHitRatio = finalMetrics.BufferHitRatio
	result.IndexBufferHitRatio = finalMetrics.IndexBufferHitRatio

	return result, nil
}

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
	fmt.Printf("Inserting %d records...\n", numRecords)
	_, err := bench.InsertRecordsPgbench(keyType, numRecords, 100) // Batch size 100 for speed
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}
	fmt.Printf("Inserted %d records\n", numRecords)

	// Step 2: Run update workload
	fmt.Printf("Running %d updates (batch size=%d)...\n", numUpdates, batchSize)

	// Capture I/O stats before updates
	ioStatsBefore, err := iometrics.GetContainerIOStats("uuid-bench-postgres")
	if err != nil {
		fmt.Printf("Warning:Failed to capture I/O stats before updates: %v\n", err)
	}

	// Use pgbench for update workload
	updateDuration, err := bench.UpdateRecordsPgbench(keyType, numRecords, numUpdates, batchSize)
	if err != nil {
		return nil, fmt.Errorf("update records: %w", err)
	}

	// Capture I/O stats after updates
	ioStatsAfter, err := iometrics.GetContainerIOStats("uuid-bench-postgres")
	if err != nil {
		fmt.Printf("Warning:Failed to capture I/O stats after updates: %v\n", err)
	}

	// Calculate I/O metrics
	if ioStatsBefore != nil && ioStatsAfter != nil {
		ioMetrics := iometrics.CalculateIOMetrics(ioStatsBefore, ioStatsAfter)
		result.ReadIOPS = ioMetrics.ReadIOPS
		result.WriteIOPS = ioMetrics.WriteIOPS
		result.ReadThroughputMB = ioMetrics.ReadThroughputMB
		result.WriteThroughputMB = ioMetrics.WriteThroughputMB
	}

	result.UpdateDuration = updateDuration
	result.UpdateThroughput = float64(numUpdates) / updateDuration.Seconds()
	// Note: Sequential pgbench doesn't provide latency percentiles

	fmt.Printf("Completed %d updates in %s\n", numUpdates, updateDuration)
	fmt.Printf("Update throughput: %.2f ops/sec\n", result.UpdateThroughput)

	// Step 3: Measure fragmentation after updates
	fmt.Println("Measuring fragmentation...")
	metrics, err := bench.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}
	result.Fragmentation = metrics.Fragmentation

	return result, nil
}

// MixedWorkloadInsertHeavy evaluates performance under insert-heavy workload (90% insert, 10% read)
//
// This scenario measures:
//   - Overall throughput under mixed load
//   - Per-operation throughput (inserts vs reads)
//   - Write amplification (page splits during concurrent reads)
//   - Buffer pool efficiency under write-heavy load
//
// Thesis relevance: Demonstrates how random UUIDs (UUIDv4) cause more page splits
// and fragment the index even during concurrent read operations, compared to
// time-ordered UUIDs (UUIDv7) which maintain better locality.
func MixedWorkloadInsertHeavy(keyType string, totalOps, connections, batchSize int) (*benchmark.MixedWorkloadResult, error) {
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

	// Configure mixed workload: 90% insert, 10% read
	initialDataset := 100000 // 100k initial dataset

	fmt.Printf("\n=== Mixed Workload: Insert-Heavy (90%% insert, 10%% read) - %s ===\n", keyType)

	// Use pgbench for mixed workload (90% insert, 10% read)
	result, err := bench.RunMixedWorkloadPgbench(keyType, initialDataset, totalOps, connections, 90, 10, 0)
	if err != nil {
		return nil, fmt.Errorf("run mixed workload: %w", err)
	}

	fmt.Printf("Overall throughput: %.2f ops/sec\n", result.OverallThroughput)
	fmt.Printf("Insert throughput: %.2f rec/sec\n", result.InsertThroughput)
	fmt.Printf("Read throughput: %.2f rec/sec\n", result.ReadThroughput)
	fmt.Printf("Buffer hit ratio: %.2f%%\n", result.BufferHitRatio*100)

	return result, nil
}

// MixedWorkloadReadHeavy evaluates performance under read-heavy workload (10% insert, 90% read)
//
// This scenario measures:
//   - Read performance degradation from ongoing inserts
//   - Cache efficiency under read-heavy load
//   - Impact of fragmentation on read operations
//   - Buffer pool hit ratios
//
// Thesis relevance: Demonstrates how index fragmentation (caused by random UUIDs)
// degrades read performance by reducing cache hit ratios and requiring more
// random I/O compared to sequential or time-ordered keys.
func MixedWorkloadReadHeavy(keyType string, totalOps, connections int) (*benchmark.MixedWorkloadResult, error) {
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

	// Configure mixed workload: 10% insert, 90% read
	initialDataset := 1000000 // 1M initial dataset for realistic read testing

	fmt.Printf("\n=== Mixed Workload: Read-Heavy (10%% insert, 90%% read) - %s ===\n", keyType)

	// Use pgbench for mixed workload (10% insert, 90% read)
	result, err := bench.RunMixedWorkloadPgbench(keyType, initialDataset, totalOps, connections, 10, 90, 0)
	if err != nil {
		return nil, fmt.Errorf("run mixed workload: %w", err)
	}

	fmt.Printf("Overall throughput: %.2f ops/sec\n", result.OverallThroughput)
	fmt.Printf("Insert throughput: %.2f rec/sec\n", result.InsertThroughput)
	fmt.Printf("Read throughput: %.2f rec/sec\n", result.ReadThroughput)
	fmt.Printf("Buffer hit ratio: %.2f%%\n", result.BufferHitRatio*100)

	return result, nil
}

// MixedWorkloadBalanced evaluates performance under balanced OLTP workload (50% insert, 30% read, 20% update)
//
// This scenario measures:
//   - OLTP workload simulation
//   - Combined impact of inserts, reads, and updates
//   - Overall system throughput
//   - Contention effects from mixed operations
//
// Thesis relevance: Demonstrates realistic production workload performance.
// Shows how different UUID types handle concurrent inserts, reads, and updates,
// revealing trade-offs between write performance, read performance, and index maintenance.
func MixedWorkloadBalanced(keyType string, totalOps, connections int) (*benchmark.MixedWorkloadResult, error) {
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

	// Configure mixed workload: 50% insert, 30% read, 20% update
	initialDataset := 500000 // 500k initial dataset for balanced workload

	fmt.Printf("\n=== Mixed Workload: Balanced (50%% insert, 30%% read, 20%% update) - %s ===\n", keyType)

	// Use pgbench for mixed workload (50% insert, 30% read, 20% update)
	result, err := bench.RunMixedWorkloadPgbench(keyType, initialDataset, totalOps, connections, 50, 30, 20)
	if err != nil {
		return nil, fmt.Errorf("run mixed workload: %w", err)
	}

	fmt.Printf("Overall throughput: %.2f ops/sec\n", result.OverallThroughput)
	fmt.Printf("Insert throughput: %.2f rec/sec\n", result.InsertThroughput)
	fmt.Printf("Read throughput: %.2f rec/sec\n", result.ReadThroughput)
	fmt.Printf("Update throughput: %.2f rec/sec\n", result.UpdateThroughput)
	fmt.Printf("Buffer hit ratio: %.2f%%\n", result.BufferHitRatio*100)

	return result, nil
}
