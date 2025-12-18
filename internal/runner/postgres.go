package runner

import (
	"fmt"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	iometrics "github.com/moguls753/uuid-benchmark/internal/benchmark/io"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres"
)

func InsertPerformance(keyType string, numRecords, batchSize, connections int) (*benchmark.InsertPerformanceResult, error) {
	bench := postgres.New()

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

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

	ioStatsBefore, err := iometrics.GetContainerIOStats("uuid-bench-postgres")
	if err != nil {
		fmt.Printf("Warning:Failed to capture I/O stats before insert: %v\n", err)
	}

	if connections == 1 {
		duration, err := bench.InsertRecordsPgbench(keyType, numRecords, batchSize)
		if err != nil {
			return nil, fmt.Errorf("insert records: %w", err)
		}
		result.Duration = duration
		result.Throughput = float64(numRecords) / duration.Seconds()
	} else {
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

	ioStatsAfter, err := iometrics.GetContainerIOStats("uuid-bench-postgres")
	if err != nil {
		fmt.Printf("Warning:Failed to capture I/O stats after insert: %v\n", err)
	}

	if ioStatsBefore != nil && ioStatsAfter != nil {
		ioMetrics := iometrics.CalculateIOMetrics(ioStatsBefore, ioStatsAfter)
		result.ReadIOPS = ioMetrics.ReadIOPS
		result.WriteIOPS = ioMetrics.WriteIOPS
		result.ReadThroughputMB = ioMetrics.ReadThroughputMB
		result.WriteThroughputMB = ioMetrics.WriteThroughputMB
	}

	fmt.Printf("Inserted %d records in %s\n", numRecords, result.Duration)
	fmt.Printf("Throughput: %.2f records/sec\n", result.Throughput)

	fmt.Println("Measuring metrics...")
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

func ReadAfterFragmentation(keyType string, numRecords, numReads int) (*benchmark.ReadAfterFragmentationResult, error) {
	bench := postgres.New()

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateTable(keyType); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	result := &benchmark.ReadAfterFragmentationResult{
		KeyType:    keyType,
		NumRecords: numRecords,
		NumReads:   numReads,
	}

	fmt.Printf("Inserting %d records to create index...\n", numRecords)
	insertDuration, err := bench.InsertRecordsPgbench(keyType, numRecords, 100)
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}
	result.InsertDuration = insertDuration
	fmt.Printf("Inserted %d records in %s\n", numRecords, insertDuration)

	fmt.Println("Measuring fragmentation...")
	metrics, err := bench.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}
	result.Fragmentation = metrics.Fragmentation
	fmt.Printf("Index fragmentation: %.2f%%\n", metrics.Fragmentation.FragmentationPercent)

	fmt.Println("Resetting PostgreSQL statistics...")
	if err := bench.ResetStats(); err != nil {
		return nil, fmt.Errorf("reset stats: %w", err)
	}

	fmt.Printf("Running %d point lookups...\n", numReads)

	ioStatsBefore, err := iometrics.GetContainerIOStats("uuid-bench-postgres")
	if err != nil {
		fmt.Printf("Warning:Failed to capture I/O stats before reads: %v\n", err)
	}

	readDuration, err := bench.ReadRecordsPgbench(keyType, numRecords, numReads)
	if err != nil {
		return nil, fmt.Errorf("read records: %w", err)
	}
	result.ReadDuration = readDuration
	result.ReadThroughput = float64(numReads) / readDuration.Seconds()

	ioStatsAfter, err := iometrics.GetContainerIOStats("uuid-bench-postgres")
	if err != nil {
		fmt.Printf("Warning:Failed to capture I/O stats after reads: %v\n", err)
	}

	if ioStatsBefore != nil && ioStatsAfter != nil {
		ioMetrics := iometrics.CalculateIOMetrics(ioStatsBefore, ioStatsAfter)
		result.ReadIOPS = ioMetrics.ReadIOPS
		result.WriteIOPS = ioMetrics.WriteIOPS
		result.ReadThroughputMB = ioMetrics.ReadThroughputMB
		result.WriteThroughputMB = ioMetrics.WriteThroughputMB
	}

	fmt.Printf("Completed %d reads in %s\n", numReads, readDuration)
	fmt.Printf("Read throughput: %.2f ops/sec\n", result.ReadThroughput)

	fmt.Println("Measuring buffer pool hit ratios...")
	finalMetrics, err := bench.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure final metrics: %w", err)
	}
	result.BufferHitRatio = finalMetrics.BufferHitRatio
	result.IndexBufferHitRatio = finalMetrics.IndexBufferHitRatio

	return result, nil
}

func UpdatePerformance(keyType string, numRecords, numUpdates, batchSize int) (*benchmark.UpdatePerformanceResult, error) {
	bench := postgres.New()

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateTable(keyType); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	result := &benchmark.UpdatePerformanceResult{
		KeyType:    keyType,
		NumRecords: numRecords,
		NumUpdates: numUpdates,
		BatchSize:  batchSize,
	}

	fmt.Printf("Inserting %d records...\n", numRecords)
	_, err := bench.InsertRecordsPgbench(keyType, numRecords, 100)
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}
	fmt.Printf("Inserted %d records\n", numRecords)

	fmt.Printf("Running %d updates (batch size=%d)...\n", numUpdates, batchSize)

	ioStatsBefore, err := iometrics.GetContainerIOStats("uuid-bench-postgres")
	if err != nil {
		fmt.Printf("Warning:Failed to capture I/O stats before updates: %v\n", err)
	}

	updateDuration, err := bench.UpdateRecordsPgbench(keyType, numRecords, numUpdates, batchSize)
	if err != nil {
		return nil, fmt.Errorf("update records: %w", err)
	}

	ioStatsAfter, err := iometrics.GetContainerIOStats("uuid-bench-postgres")
	if err != nil {
		fmt.Printf("Warning:Failed to capture I/O stats after updates: %v\n", err)
	}

	if ioStatsBefore != nil && ioStatsAfter != nil {
		ioMetrics := iometrics.CalculateIOMetrics(ioStatsBefore, ioStatsAfter)
		result.ReadIOPS = ioMetrics.ReadIOPS
		result.WriteIOPS = ioMetrics.WriteIOPS
		result.ReadThroughputMB = ioMetrics.ReadThroughputMB
		result.WriteThroughputMB = ioMetrics.WriteThroughputMB
	}

	result.UpdateDuration = updateDuration
	result.UpdateThroughput = float64(numUpdates) / updateDuration.Seconds()

	fmt.Printf("Completed %d updates in %s\n", numUpdates, updateDuration)
	fmt.Printf("Update throughput: %.2f ops/sec\n", result.UpdateThroughput)

	fmt.Println("Measuring fragmentation...")
	metrics, err := bench.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}
	result.Fragmentation = metrics.Fragmentation

	return result, nil
}

func MixedWorkloadInsertHeavy(keyType string, totalOps, connections, batchSize int) (*benchmark.MixedWorkloadResult, error) {
	bench := postgres.New()

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateTable(keyType); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	initialDataset := 100000

	fmt.Printf("\n=== Mixed Workload: Insert-Heavy (90%% insert, 10%% read) - %s ===\n", keyType)

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

func MixedWorkloadReadHeavy(keyType string, totalOps, connections int) (*benchmark.MixedWorkloadResult, error) {
	bench := postgres.New()

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateTable(keyType); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	initialDataset := 1000000

	fmt.Printf("\n=== Mixed Workload: Read-Heavy (10%% insert, 90%% read) - %s ===\n", keyType)

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

func MixedWorkloadBalanced(keyType string, totalOps, connections int) (*benchmark.MixedWorkloadResult, error) {
	bench := postgres.New()

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateTable(keyType); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	initialDataset := 500000

	fmt.Printf("\n=== Mixed Workload: Balanced (50%% insert, 30%% read, 20%% update) - %s ===\n", keyType)

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
