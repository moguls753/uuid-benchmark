package postgres

import (
	"fmt"
	"sync"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
)

// MixedWorkloadConfig defines the configuration for a mixed workload
type MixedWorkloadConfig struct {
	KeyType       string
	TotalOps      int
	Connections   int
	InsertOps     int
	ReadOps       int
	UpdateOps     int
	InitialDataset int
	BatchSize     int
}

// OperationLatencies tracks latencies per operation type
type OperationLatencies struct {
	InsertLatencies []time.Duration
	ReadLatencies   []time.Duration
	UpdateLatencies []time.Duration
}

// RunMixedWorkload executes a mixed workload with concurrent insert/read/update operations
func (p *PostgresBenchmarker) RunMixedWorkload(config MixedWorkloadConfig) (*benchmark.MixedWorkloadResult, error) {
	// Phase 1: Create initial dataset
	fmt.Printf("→ Creating initial dataset (%d records)...\n", config.InitialDataset)
	_, err := p.InsertRecords(config.KeyType, config.InitialDataset, config.BatchSize)
	if err != nil {
		return nil, fmt.Errorf("create initial dataset: %w", err)
	}

	// Phase 2: Reset PostgreSQL statistics to measure only mixed workload
	fmt.Println("→ Resetting statistics...")
	err = p.ResetStats()
	if err != nil {
		return nil, fmt.Errorf("reset stats: %w", err)
	}

	// Phase 3: Calculate worker distribution
	insertWorkers, readWorkers, updateWorkers := calculateWorkerDistribution(
		config.Connections,
		config.InsertOps,
		config.ReadOps,
		config.UpdateOps,
	)

	fmt.Printf("→ Running mixed workload (%d inserts, %d reads, %d updates)...\n",
		config.InsertOps, config.ReadOps, config.UpdateOps)
	fmt.Printf("  Workers: %d insert, %d read, %d update\n",
		insertWorkers, readWorkers, updateWorkers)

	// Capture start LSN before mixed workload
	startLSN, err := p.getCurrentLSN()
	if err != nil {
		return nil, fmt.Errorf("capture start LSN: %w", err)
	}
	p.startLSN = startLSN

	// Phase 4: Run mixed workload with concurrent workers
	var wg sync.WaitGroup
	var mu sync.Mutex
	allLatencies := OperationLatencies{
		InsertLatencies: make([]time.Duration, 0, config.InsertOps),
		ReadLatencies:   make([]time.Duration, 0, config.ReadOps),
		UpdateLatencies: make([]time.Duration, 0, config.UpdateOps),
	}

	startTime := time.Now()

	// Launch insert workers
	if config.InsertOps > 0 && insertWorkers > 0 {
		opsPerWorker := config.InsertOps / insertWorkers
		remainder := config.InsertOps % insertWorkers

		for workerID := 0; workerID < insertWorkers; workerID++ {
			wg.Add(1)
			workerOps := opsPerWorker
			if workerID < remainder {
				workerOps++
			}

			go func(id, ops int) {
				defer wg.Done()
				latencies := p.runInsertMixedWorker(config.KeyType, ops, config.InitialDataset+id*10000, config.BatchSize)

				mu.Lock()
				allLatencies.InsertLatencies = append(allLatencies.InsertLatencies, latencies...)
				mu.Unlock()
			}(workerID, workerOps)
		}
	}

	// Launch read workers
	if config.ReadOps > 0 && readWorkers > 0 {
		opsPerWorker := config.ReadOps / readWorkers
		remainder := config.ReadOps % readWorkers

		for workerID := 0; workerID < readWorkers; workerID++ {
			wg.Add(1)
			workerOps := opsPerWorker
			if workerID < remainder {
				workerOps++
			}

			go func(id, ops int) {
				defer wg.Done()
				latencies := p.runReadMixedWorker(config.KeyType, ops, config.InitialDataset)

				mu.Lock()
				allLatencies.ReadLatencies = append(allLatencies.ReadLatencies, latencies...)
				mu.Unlock()
			}(workerID, workerOps)
		}
	}

	// Launch update workers
	if config.UpdateOps > 0 && updateWorkers > 0 {
		opsPerWorker := config.UpdateOps / updateWorkers
		remainder := config.UpdateOps % updateWorkers

		for workerID := 0; workerID < updateWorkers; workerID++ {
			wg.Add(1)
			workerOps := opsPerWorker
			if workerID < remainder {
				workerOps++
			}

			go func(id, ops int) {
				defer wg.Done()
				latencies := p.runUpdateMixedWorker(config.KeyType, ops, config.InitialDataset)

				mu.Lock()
				allLatencies.UpdateLatencies = append(allLatencies.UpdateLatencies, latencies...)
				mu.Unlock()
			}(workerID, workerOps)
		}
	}

	wg.Wait()
	duration := time.Since(startTime)

	// Capture end LSN after mixed workload
	endLSN, err := p.getCurrentLSN()
	if err != nil {
		return nil, fmt.Errorf("capture end LSN: %w", err)
	}
	p.endLSN = endLSN

	fmt.Printf("✓ Mixed workload completed in %s\n", duration)

	// Phase 5: Measure final metrics
	fmt.Println("→ Measuring final metrics...")
	metrics, err := p.MeasureMetrics()
	if err != nil {
		return nil, fmt.Errorf("measure metrics: %w", err)
	}

	// Calculate throughput per operation type
	var insertThroughput, readThroughput, updateThroughput float64
	if config.InsertOps > 0 && len(allLatencies.InsertLatencies) > 0 {
		insertDuration := time.Duration(0)
		for _, lat := range allLatencies.InsertLatencies {
			insertDuration += lat
		}
		insertThroughput = float64(config.InsertOps) / insertDuration.Seconds()
	}

	if config.ReadOps > 0 && len(allLatencies.ReadLatencies) > 0 {
		readDuration := time.Duration(0)
		for _, lat := range allLatencies.ReadLatencies {
			readDuration += lat
		}
		readThroughput = float64(config.ReadOps) / readDuration.Seconds()
	}

	if config.UpdateOps > 0 && len(allLatencies.UpdateLatencies) > 0 {
		updateDuration := time.Duration(0)
		for _, lat := range allLatencies.UpdateLatencies {
			updateDuration += lat
		}
		updateThroughput = float64(config.UpdateOps) / updateDuration.Seconds()
	}

	overallThroughput := float64(config.TotalOps) / duration.Seconds()

	// Build result
	result := &benchmark.MixedWorkloadResult{
		KeyType:             config.KeyType,
		NumRecords:          config.InitialDataset,
		Duration:            duration,
		TotalOps:            config.TotalOps,
		InsertOps:           config.InsertOps,
		ReadOps:             config.ReadOps,
		UpdateOps:           config.UpdateOps,
		OverallThroughput:   overallThroughput,
		InsertThroughput:    insertThroughput,
		ReadThroughput:      readThroughput,
		UpdateThroughput:    updateThroughput,
		BufferHitRatio:      metrics.BufferHitRatio,
		IndexBufferHitRatio: metrics.IndexBufferHitRatio,
		Fragmentation:       metrics.Fragmentation,
		TableSize:           metrics.TableSize,
		IndexSize:           metrics.IndexSize,
	}

	return result, nil
}

// calculateWorkerDistribution determines how many workers for each operation type
func calculateWorkerDistribution(totalConnections, insertOps, readOps, updateOps int) (int, int, int) {
	totalOps := insertOps + readOps + updateOps
	if totalOps == 0 {
		return 0, 0, 0
	}

	// Calculate proportional worker counts
	insertWorkers := max(0, int(float64(totalConnections)*float64(insertOps)/float64(totalOps)))
	readWorkers := max(0, int(float64(totalConnections)*float64(readOps)/float64(totalOps)))
	updateWorkers := max(0, int(float64(totalConnections)*float64(updateOps)/float64(totalOps)))

	// Ensure at least 1 worker for each operation type that has ops
	if insertOps > 0 && insertWorkers == 0 {
		insertWorkers = 1
	}
	if readOps > 0 && readWorkers == 0 {
		readWorkers = 1
	}
	if updateOps > 0 && updateWorkers == 0 {
		updateWorkers = 1
	}

	// Adjust if total exceeds connections (due to rounding and minimums)
	total := insertWorkers + readWorkers + updateWorkers
	if total > totalConnections {
		// Reduce from largest group
		if insertWorkers >= readWorkers && insertWorkers >= updateWorkers && insertWorkers > 1 {
			insertWorkers -= (total - totalConnections)
		} else if readWorkers >= updateWorkers && readWorkers > 1 {
			readWorkers -= (total - totalConnections)
		} else if updateWorkers > 1 {
			updateWorkers -= (total - totalConnections)
		}
	}

	return insertWorkers, readWorkers, updateWorkers
}

// runInsertMixedWorker executes insert operations for a mixed workload worker
func (p *PostgresBenchmarker) runInsertMixedWorker(keyType string, numOps, startIdx, batchSize int) []time.Duration {
	latencies := make([]time.Duration, 0, numOps)

	if batchSize == 1 {
		// Single insert mode
		for i := 0; i < numOps; i++ {
			start := time.Now()
			_ = p.executeSingleInsert(keyType, startIdx+i)
			latencies = append(latencies, time.Since(start))
		}
	} else {
		// Batch insert mode
		for i := 0; i < numOps; i += batchSize {
			actualBatchSize := min(batchSize, numOps-i)

			start := time.Now()
			_ = p.executeBatchInsert(keyType, startIdx+i, actualBatchSize, -1)
			latencies = append(latencies, time.Since(start))
		}
	}

	return latencies
}

// runReadMixedWorker executes read operations for a mixed workload worker
func (p *PostgresBenchmarker) runReadMixedWorker(keyType string, numOps, totalRecords int) []time.Duration {
	latencies := make([]time.Duration, 0, numOps)

	for i := 0; i < numOps; i++ {
		start := time.Now()
		_, _ = p.executeRandomRead(keyType, totalRecords)
		latencies = append(latencies, time.Since(start))
	}

	return latencies
}

// runUpdateMixedWorker executes update operations for a mixed workload worker
func (p *PostgresBenchmarker) runUpdateMixedWorker(keyType string, numOps, totalRecords int) []time.Duration {
	latencies := make([]time.Duration, 0, numOps)

	for i := 0; i < numOps; i++ {
		start := time.Now()
		_ = p.executeRandomUpdate(keyType, i, totalRecords)
		latencies = append(latencies, time.Since(start))
	}

	return latencies
}
