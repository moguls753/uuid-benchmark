package scenarios

import (
	"fmt"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres"
)

// AllKeyTypes defines all UUID types to benchmark
var AllKeyTypes = []string{"bigserial", "uuidv4"} // uuidv7, ulid, uuidv1 pending

// InsertPerformance evaluates disk efficiency during insert operations
//
// This scenario measures:
//   - Insert throughput
//   - Page splits (B-tree index maintenance cost)
//   - Index fragmentation
//   - Disk usage (table and index size)
//   - Latency percentiles (p50, p95, p99)
//
// Thesis relevance: Demonstrates that UUIDv4 causes more page splits and
// fragmentation compared to sequential keys (BIGSERIAL) or time-ordered UUIDs (UUIDv7).
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

	fmt.Printf("→ Inserting %d records (connections=%d, batch=%d)...\n", numRecords, connections, batchSize)

	result := &benchmark.InsertPerformanceResult{
		KeyType:     keyType,
		NumRecords:  numRecords,
		BatchSize:   batchSize,
		Connections: connections,
	}

	// Execute insert operation (sequential or concurrent)
	if connections == 1 {
		// Sequential insert
		duration, err := bench.InsertRecords(keyType, numRecords, batchSize)
		if err != nil {
			return nil, fmt.Errorf("insert records: %w", err)
		}
		result.Duration = duration
		result.Throughput = float64(numRecords) / duration.Seconds()
	} else {
		// Concurrent insert
		concResult, err := bench.InsertRecordsConcurrent(keyType, numRecords, connections, batchSize)
		if err != nil {
			return nil, fmt.Errorf("insert records concurrent: %w", err)
		}
		result.Duration = concResult.Duration
		result.Throughput = concResult.Throughput
		result.LatencyP50 = concResult.LatencyP50
		result.LatencyP95 = concResult.LatencyP95
		result.LatencyP99 = concResult.LatencyP99
	}

	fmt.Printf("✓ Inserted %d records in %s\n", numRecords, result.Duration)
	fmt.Printf("✓ Throughput: %.2f records/sec\n", result.Throughput)

	// Measure metrics (page splits, fragmentation, disk usage)
	fmt.Println("→ Measuring metrics...")
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
