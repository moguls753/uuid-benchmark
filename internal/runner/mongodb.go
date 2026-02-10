package runner

import (
	"fmt"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	iometrics "github.com/moguls753/uuid-benchmark/internal/benchmark/io"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/mongodb"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
)

func MongoDBInsertPerformance(keyType string, numRecords, batchSize, connections int) (*benchmark.InsertPerformanceResult, error) {
	bench := mongodb.New()

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateCollection(keyType); err != nil {
		return nil, fmt.Errorf("create collection: %w", err)
	}

	fmt.Printf("Inserting %d records (connections=%d, batch=%d)...\n", numRecords, connections, batchSize)

	result := &benchmark.InsertPerformanceResult{
		KeyType:     keyType,
		NumRecords:  numRecords,
		BatchSize:   batchSize,
		Connections: connections,
	}

	ioStatsBefore, err := iometrics.GetContainerIOStats(mongodb.ContainerName)
	if err != nil {
		fmt.Printf("Warning: Failed to capture I/O stats before insert: %v\n", err)
	}

	wlResult, err := bench.InsertRecords(keyType, numRecords, batchSize, connections)
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}

	result.Duration = wlResult.Duration
	result.Throughput = wlResult.Throughput
	result.LatencyP50 = wlResult.LatencyP50
	result.LatencyP95 = wlResult.LatencyP95
	result.LatencyP99 = wlResult.LatencyP99

	ioStatsAfter, err := iometrics.GetContainerIOStats(mongodb.ContainerName)
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

func MongoDBReadAfterFragmentation(keyType string, numRecords, numReads int) (*benchmark.ReadAfterFragmentationResult, error) {
	bench := mongodb.New()

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateCollection(keyType); err != nil {
		return nil, fmt.Errorf("create collection: %w", err)
	}

	result := &benchmark.ReadAfterFragmentationResult{
		KeyType:    keyType,
		NumRecords: numRecords,
		NumReads:   numReads,
	}

	fmt.Printf("Inserting %d records to create index...\n", numRecords)
	insertResult, err := bench.InsertRecords(keyType, numRecords, 100, 1)
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}
	result.InsertDuration = insertResult.Duration
	fmt.Printf("Inserted %d records in %s\n", numRecords, insertResult.Duration)

	fmt.Println("Measuring fragmentation...")
	metrics, err := bench.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}
	result.Fragmentation = metrics.Fragmentation
	fmt.Printf("Index fragmentation: %.2f%%\n", metrics.Fragmentation.FragmentationPercent)

	// Capture metrics before read phase
	if err := bench.CaptureMetricsBefore(); err != nil {
		fmt.Printf("Warning: Could not capture metrics before reads: %v\n", err)
	}

	fmt.Printf("Running %d point lookups...\n", numReads)

	ioStatsBefore, err := iometrics.GetContainerIOStats(mongodb.ContainerName)
	if err != nil {
		fmt.Printf("Warning: Failed to capture I/O stats before reads: %v\n", err)
	}

	readResult, err := bench.ReadRecords(keyType, numReads, 1)
	if err != nil {
		return nil, fmt.Errorf("read records: %w", err)
	}
	result.ReadDuration = readResult.Duration
	result.ReadThroughput = readResult.Throughput
	result.LatencyP50 = readResult.LatencyP50
	result.LatencyP95 = readResult.LatencyP95
	result.LatencyP99 = readResult.LatencyP99

	ioStatsAfter, err := iometrics.GetContainerIOStats(mongodb.ContainerName)
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

func MongoDBUpdatePerformance(keyType string, numRecords, numUpdates, batchSize int) (*benchmark.UpdatePerformanceResult, error) {
	bench := mongodb.New()

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateCollection(keyType); err != nil {
		return nil, fmt.Errorf("create collection: %w", err)
	}

	result := &benchmark.UpdatePerformanceResult{
		KeyType:    keyType,
		NumRecords: numRecords,
		NumUpdates: numUpdates,
		BatchSize:  batchSize,
	}

	fmt.Printf("Inserting %d records...\n", numRecords)
	_, err := bench.InsertRecords(keyType, numRecords, 100, 1)
	if err != nil {
		return nil, fmt.Errorf("insert records: %w", err)
	}
	fmt.Printf("Inserted %d records\n", numRecords)

	if err := bench.CaptureMetricsBefore(); err != nil {
		fmt.Printf("Warning: Could not capture metrics before updates: %v\n", err)
	}

	fmt.Printf("Running %d updates...\n", numUpdates)

	ioStatsBefore, err := iometrics.GetContainerIOStats(mongodb.ContainerName)
	if err != nil {
		fmt.Printf("Warning: Failed to capture I/O stats before updates: %v\n", err)
	}

	updateResult, err := bench.UpdateRecords(keyType, numUpdates, 1)
	if err != nil {
		return nil, fmt.Errorf("update records: %w", err)
	}

	ioStatsAfter, err := iometrics.GetContainerIOStats(mongodb.ContainerName)
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

func MongoDBMixedWorkloadInsertHeavy(keyType string, totalOps, connections, batchSize int) (*benchmark.MixedWorkloadResult, error) {
	bench := mongodb.New()

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateCollection(keyType); err != nil {
		return nil, fmt.Errorf("create collection: %w", err)
	}

	initialDataset := 100000

	fmt.Printf("\n=== Mixed Workload: Insert-Heavy (90%% insert, 10%% read) - %s ===\n", keyType)

	wlResult, err := bench.RunMixedWorkload(keyType, initialDataset, totalOps, connections, 90, 10, 0)
	if err != nil {
		return nil, fmt.Errorf("run mixed workload: %w", err)
	}

	metrics, err := bench.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}

	result := mixedResultFromWorkload(keyType, initialDataset, totalOps, wlResult, metrics, 90, 10, 0)

	fmt.Printf("Overall throughput: %.2f ops/sec\n", result.OverallThroughput)
	fmt.Printf("Buffer hit ratio: %.2f%%\n", result.BufferHitRatio*100)

	return result, nil
}

func MongoDBMixedWorkloadReadHeavy(keyType string, totalOps, connections int) (*benchmark.MixedWorkloadResult, error) {
	bench := mongodb.New()

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateCollection(keyType); err != nil {
		return nil, fmt.Errorf("create collection: %w", err)
	}

	initialDataset := 1000000

	fmt.Printf("\n=== Mixed Workload: Read-Heavy (10%% insert, 90%% read) - %s ===\n", keyType)

	wlResult, err := bench.RunMixedWorkload(keyType, initialDataset, totalOps, connections, 10, 90, 0)
	if err != nil {
		return nil, fmt.Errorf("run mixed workload: %w", err)
	}

	metrics, err := bench.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}

	result := mixedResultFromWorkload(keyType, initialDataset, totalOps, wlResult, metrics, 10, 90, 0)

	fmt.Printf("Overall throughput: %.2f ops/sec\n", result.OverallThroughput)
	fmt.Printf("Buffer hit ratio: %.2f%%\n", result.BufferHitRatio*100)

	return result, nil
}

func MongoDBMixedWorkloadBalanced(keyType string, totalOps, connections int) (*benchmark.MixedWorkloadResult, error) {
	bench := mongodb.New()

	if err := bench.Connect(); err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	defer bench.Close()

	if err := bench.CreateCollection(keyType); err != nil {
		return nil, fmt.Errorf("create collection: %w", err)
	}

	initialDataset := 500000

	fmt.Printf("\n=== Mixed Workload: Balanced (50%% insert, 30%% read, 20%% update) - %s ===\n", keyType)

	wlResult, err := bench.RunMixedWorkload(keyType, initialDataset, totalOps, connections, 50, 30, 20)
	if err != nil {
		return nil, fmt.Errorf("run mixed workload: %w", err)
	}

	metrics, err := bench.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}

	result := mixedResultFromWorkload(keyType, initialDataset, totalOps, wlResult, metrics, 50, 30, 20)

	fmt.Printf("Overall throughput: %.2f ops/sec\n", result.OverallThroughput)
	fmt.Printf("Buffer hit ratio: %.2f%%\n", result.BufferHitRatio*100)

	return result, nil
}

func mixedResultFromWorkload(keyType string, initialDataset, totalOps int, wl *workload.WorkloadResult, metrics *benchmark.BenchmarkResult, insertPct, readPct, updatePct int) *benchmark.MixedWorkloadResult {
	insertOps := wl.InsertOps
	readOps := wl.ReadOps
	updateOps := wl.UpdateOps
	if insertOps == 0 && readOps == 0 && updateOps == 0 {
		insertOps = (totalOps * insertPct) / 100
		readOps = (totalOps * readPct) / 100
		updateOps = (totalOps * updatePct) / 100
	}

	return &benchmark.MixedWorkloadResult{
		KeyType:             keyType,
		NumRecords:          initialDataset,
		TotalOps:            totalOps,
		InsertOps:           insertOps,
		ReadOps:             readOps,
		UpdateOps:           updateOps,
		Duration:            wl.Duration,
		OverallThroughput:   wl.Throughput,
		InsertThroughput:    0,
		ReadThroughput:      0,
		UpdateThroughput:    0,
		LatencyP50:          wl.LatencyP50,
		LatencyP95:          wl.LatencyP95,
		LatencyP99:          wl.LatencyP99,
		BufferHitRatio:      metrics.BufferHitRatio,
		IndexBufferHitRatio: metrics.IndexBufferHitRatio,
		Fragmentation:       metrics.Fragmentation,
		TableSize:           metrics.TableSize,
		IndexSize:           metrics.IndexSize,
	}
}
