package runner

import (
	"fmt"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	iometrics "github.com/moguls753/uuid-benchmark/internal/benchmark/io"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/mysql"
)

func MySQLInsertPerformance(keyType string, numRecords, batchSize, connections int) (*benchmark.InsertPerformanceResult, error) {
	bench := mysql.New()

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

	ioStatsBefore, err := iometrics.GetContainerIOStats("uuid-bench-mysql")
	if err != nil {
		fmt.Printf("Warning: Failed to capture I/O stats before insert: %v\n", err)
	}

	concResult, err := bench.InsertRecordsSysbenchConcurrent(keyType, numRecords, connections, batchSize)
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}
	result.Duration = concResult.Duration
	result.Throughput = concResult.Throughput
	result.LatencyP50 = concResult.LatencyP50
	result.LatencyP95 = concResult.LatencyP95
	result.LatencyP99 = concResult.LatencyP99

	ioStatsAfter, err := iometrics.GetContainerIOStats("uuid-bench-mysql")
	if err != nil {
		fmt.Printf("Warning: Failed to capture I/O stats after insert: %v\n", err)
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

func MySQLReadPerformance(keyType string, numRecords, numReads int) (*benchmark.ReadPerformanceResult, error) {
	bench := mysql.New()

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateTable(keyType); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	result := &benchmark.ReadPerformanceResult{
		KeyType:    keyType,
		NumRecords: numRecords,
		NumReads:   numReads,
	}

	fmt.Printf("Inserting %d records to create index...\n", numRecords)
	insertDuration, err := bench.InsertRecordsSysbench(keyType, numRecords, 100)
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}
	result.InsertDuration = insertDuration
	fmt.Printf("Inserted %d records in %s\n", numRecords, insertDuration)

	fmt.Println("Creating lookup table for random key selection...")
	if err := bench.CreateLookupTable(); err != nil {
		return nil, fmt.Errorf("create lookup table: %w", err)
	}

	fmt.Println("Measuring fragmentation...")
	metrics, err := bench.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}
	result.Fragmentation = metrics.Fragmentation
	fmt.Printf("Index fragmentation: %.2f%%\n", metrics.Fragmentation.FragmentationPercent)

	fmt.Println("Resetting MySQL statistics...")
	if err := bench.ResetStats(); err != nil {
		fmt.Printf("Warning: Could not reset stats: %v\n", err)
	}

	fmt.Printf("Running %d point lookups...\n", numReads)

	ioStatsBefore, err := iometrics.GetContainerIOStats("uuid-bench-mysql")
	if err != nil {
		fmt.Printf("Warning: Failed to capture I/O stats before reads: %v\n", err)
	}

	readResult, err := bench.ReadRecordsSysbenchConcurrent(keyType, numRecords, numReads, 1)
	if err != nil {
		return nil, fmt.Errorf("read records: %w", err)
	}
	result.ReadDuration = readResult.Duration
	result.ReadThroughput = readResult.Throughput
	result.LatencyP50 = readResult.LatencyP50
	result.LatencyP95 = readResult.LatencyP95
	result.LatencyP99 = readResult.LatencyP99

	ioStatsAfter, err := iometrics.GetContainerIOStats("uuid-bench-mysql")
	if err != nil {
		fmt.Printf("Warning: Failed to capture I/O stats after reads: %v\n", err)
	}

	if ioStatsBefore != nil && ioStatsAfter != nil {
		ioMetrics := iometrics.CalculateIOMetrics(ioStatsBefore, ioStatsAfter)
		result.ReadIOPS = ioMetrics.ReadIOPS
		result.WriteIOPS = ioMetrics.WriteIOPS
		result.ReadThroughputMB = ioMetrics.ReadThroughputMB
		result.WriteThroughputMB = ioMetrics.WriteThroughputMB
	}

	fmt.Printf("Completed %d reads in %s\n", numReads, readResult.Duration)
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

func MySQLUpdatePerformance(keyType string, numRecords, numUpdates, batchSize int) (*benchmark.UpdatePerformanceResult, error) {
	bench := mysql.New()

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
	_, err := bench.InsertRecordsSysbench(keyType, numRecords, 100)
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}
	fmt.Printf("Inserted %d records\n", numRecords)

	fmt.Println("Creating lookup table for random key selection...")
	if err := bench.CreateLookupTable(); err != nil {
		return nil, fmt.Errorf("create lookup table: %w", err)
	}

	fmt.Printf("Running %d updates (batch size=%d)...\n", numUpdates, batchSize)

	ioStatsBefore, err := iometrics.GetContainerIOStats("uuid-bench-mysql")
	if err != nil {
		fmt.Printf("Warning: Failed to capture I/O stats before updates: %v\n", err)
	}

	updateResult, err := bench.UpdateRecordsSysbenchConcurrent(keyType, numRecords, numUpdates, 1, batchSize)
	if err != nil {
		return nil, fmt.Errorf("update records: %w", err)
	}

	ioStatsAfter, err := iometrics.GetContainerIOStats("uuid-bench-mysql")
	if err != nil {
		fmt.Printf("Warning: Failed to capture I/O stats after updates: %v\n", err)
	}

	if ioStatsBefore != nil && ioStatsAfter != nil {
		ioMetrics := iometrics.CalculateIOMetrics(ioStatsBefore, ioStatsAfter)
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

	fmt.Printf("Completed %d updates in %s\n", numUpdates, updateResult.Duration)
	fmt.Printf("Update throughput: %.2f ops/sec\n", result.UpdateThroughput)

	fmt.Println("Measuring fragmentation...")
	metrics, err := bench.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}
	result.Fragmentation = metrics.Fragmentation

	return result, nil
}

func MySQLMixedWorkloadInsertHeavy(keyType string, totalOps, connections, batchSize int) (*benchmark.MixedWorkloadResult, error) {
	bench := mysql.New()

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateTable(keyType); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	initialDataset := 100000

	fmt.Printf("\n=== Mixed Workload: Insert-Heavy (70%% insert, 30%% read) - %s ===\n", keyType)

	result, err := bench.RunMixedWorkloadSysbench(keyType, initialDataset, totalOps, connections, 70, 30, 0)
	if err != nil {
		return nil, fmt.Errorf("run mixed workload: %w", err)
	}

	fmt.Printf("Overall throughput: %.2f ops/sec\n", result.OverallThroughput)
	fmt.Printf("Buffer hit ratio: %.2f%%\n", result.BufferHitRatio*100)

	return result, nil
}

func MySQLMixedWorkloadReadUpdate(keyType string, totalOps, connections int) (*benchmark.MixedWorkloadResult, error) {
	bench := mysql.New()

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateTable(keyType); err != nil {
		return nil, fmt.Errorf("create table: %w", err)
	}

	initialDataset := 500000

	fmt.Printf("\n=== Mixed Workload: YCSB-A (50%% read, 50%% update) - %s ===\n", keyType)

	result, err := bench.RunMixedWorkloadSysbench(keyType, initialDataset, totalOps, connections, 0, 50, 50)
	if err != nil {
		return nil, fmt.Errorf("run mixed workload: %w", err)
	}

	fmt.Printf("Overall throughput: %.2f ops/sec\n", result.OverallThroughput)
	fmt.Printf("Buffer hit ratio: %.2f%%\n", result.BufferHitRatio*100)

	return result, nil
}
