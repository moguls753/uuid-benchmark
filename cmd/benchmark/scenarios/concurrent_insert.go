package scenarios

import (
	"fmt"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres"
)

// ConcurrentInsert evaluates insert performance under concurrent load
//
// This scenario measures:
//   - Concurrent insert throughput
//   - Latency percentiles under load
//   - Page splits with concurrent writers
//   - Disk usage and fragmentation
//
// Thesis relevance: Tests whether UUIDv4's random distribution causes contention
// or performance degradation under concurrent writes compared to sequential keys.
// Multiple writers may cause lock contention on B-tree pages with random UUIDs.
func ConcurrentInsert(keyType string, numRecords, batchSize, connections int) (*benchmark.ConcurrentInsertResult, error) {
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

	fmt.Printf("→ Inserting %d records with %d concurrent workers (batch=%d)...\n",
		numRecords, connections, batchSize)

	result := &benchmark.ConcurrentInsertResult{
		KeyType:     keyType,
		NumRecords:  numRecords,
		BatchSize:   batchSize,
		Connections: connections,
	}

	// Execute concurrent insert
	concResult, err := bench.InsertRecordsConcurrent(keyType, numRecords, connections, batchSize)
	if err != nil {
		return nil, fmt.Errorf("insert records concurrent: %w", err)
	}

	result.Duration = concResult.Duration
	result.Throughput = concResult.Throughput
	result.LatencyP50 = concResult.LatencyP50
	result.LatencyP95 = concResult.LatencyP95
	result.LatencyP99 = concResult.LatencyP99

	fmt.Printf("✓ Inserted %d records in %s\n", numRecords, result.Duration)
	fmt.Printf("✓ Throughput: %.2f records/sec\n", result.Throughput)
	fmt.Printf("✓ Latency p50/p95/p99: %s / %s / %s\n",
		result.LatencyP50, result.LatencyP95, result.LatencyP99)

	// Measure metrics
	fmt.Println("→ Measuring metrics...")
	metrics, err := bench.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}

	result.PageSplits = metrics.PageSplits
	result.TableSize = metrics.TableSize
	result.IndexSize = metrics.IndexSize
	result.Fragmentation = metrics.Fragmentation

	return result, nil
}
