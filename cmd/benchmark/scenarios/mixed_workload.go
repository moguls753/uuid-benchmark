package scenarios

import (
	"fmt"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres"
)

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
	insertOps := int(float64(totalOps) * 0.90)
	readOps := int(float64(totalOps) * 0.10)
	updateOps := 0

	config := postgres.MixedWorkloadConfig{
		KeyType:        keyType,
		TotalOps:       totalOps,
		Connections:    connections,
		InsertOps:      insertOps,
		ReadOps:        readOps,
		UpdateOps:      updateOps,
		InitialDataset: 100000, // 100k initial dataset
		BatchSize:      batchSize,
	}

	fmt.Printf("\n=== Mixed Workload: Insert-Heavy (90%% insert, 10%% read) - %s ===\n", keyType)

	result, err := bench.RunMixedWorkload(config)
	if err != nil {
		return nil, fmt.Errorf("run mixed workload: %w", err)
	}

	fmt.Printf("✓ Overall throughput: %.2f ops/sec\n", result.OverallThroughput)
	fmt.Printf("✓ Insert throughput: %.2f rec/sec\n", result.InsertThroughput)
	fmt.Printf("✓ Read throughput: %.2f rec/sec\n", result.ReadThroughput)
	fmt.Printf("✓ Buffer hit ratio: %.2f%%\n", result.BufferHitRatio*100)

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
	insertOps := int(float64(totalOps) * 0.10)
	readOps := int(float64(totalOps) * 0.90)
	updateOps := 0

	config := postgres.MixedWorkloadConfig{
		KeyType:        keyType,
		TotalOps:       totalOps,
		Connections:    connections,
		InsertOps:      insertOps,
		ReadOps:        readOps,
		UpdateOps:      updateOps,
		InitialDataset: 1000000, // 1M initial dataset for realistic read testing
		BatchSize:      100,
	}

	fmt.Printf("\n=== Mixed Workload: Read-Heavy (10%% insert, 90%% read) - %s ===\n", keyType)

	result, err := bench.RunMixedWorkload(config)
	if err != nil {
		return nil, fmt.Errorf("run mixed workload: %w", err)
	}

	fmt.Printf("✓ Overall throughput: %.2f ops/sec\n", result.OverallThroughput)
	fmt.Printf("✓ Insert throughput: %.2f rec/sec\n", result.InsertThroughput)
	fmt.Printf("✓ Read throughput: %.2f rec/sec\n", result.ReadThroughput)
	fmt.Printf("✓ Buffer hit ratio: %.2f%%\n", result.BufferHitRatio*100)

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
	insertOps := int(float64(totalOps) * 0.50)
	readOps := int(float64(totalOps) * 0.30)
	updateOps := int(float64(totalOps) * 0.20)

	config := postgres.MixedWorkloadConfig{
		KeyType:        keyType,
		TotalOps:       totalOps,
		Connections:    connections,
		InsertOps:      insertOps,
		ReadOps:        readOps,
		UpdateOps:      updateOps,
		InitialDataset: 500000, // 500k initial dataset for balanced workload
		BatchSize:      100,
	}

	fmt.Printf("\n=== Mixed Workload: Balanced (50%% insert, 30%% read, 20%% update) - %s ===\n", keyType)

	result, err := bench.RunMixedWorkload(config)
	if err != nil {
		return nil, fmt.Errorf("run mixed workload: %w", err)
	}

	fmt.Printf("✓ Overall throughput: %.2f ops/sec\n", result.OverallThroughput)
	fmt.Printf("✓ Insert throughput: %.2f rec/sec\n", result.InsertThroughput)
	fmt.Printf("✓ Read throughput: %.2f rec/sec\n", result.ReadThroughput)
	fmt.Printf("✓ Update throughput: %.2f rec/sec\n", result.UpdateThroughput)
	fmt.Printf("✓ Buffer hit ratio: %.2f%%\n", result.BufferHitRatio*100)

	return result, nil
}
