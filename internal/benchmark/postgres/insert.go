package postgres

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/moguls753/uuid-benchmark/internal/benchmark"
)

const noWorkerID = -1

func (p *PostgresBenchmarker) InsertRecords(keyType string, numRecords, batchSize int) (time.Duration, error) {
	startTime := time.Now()

	if batchSize == 1 {
		for i := 0; i < numRecords; i++ {
			if err := p.executeSingleInsert(keyType, i); err != nil {
				return 0, fmt.Errorf("insert record %d: %w", i, err)
			}

			if (i+1)%1000 == 0 {
				fmt.Printf("  ... %d/%d\n", i+1, numRecords)
			}
		}
	} else {
		for i := 0; i < numRecords; i += batchSize {
			batchEnd := min(i+batchSize, numRecords)
			actualBatchSize := batchEnd - i

			if err := p.executeBatchInsert(keyType, i, actualBatchSize, noWorkerID); err != nil {
				return 0, fmt.Errorf("insert batch at %d: %w", i, err)
			}

			if batchEnd%1000 < batchSize || batchEnd == numRecords {
				fmt.Printf("  ... %d/%d\n", batchEnd, numRecords)
			}
		}
	}

	return time.Since(startTime), nil
}

func (p *PostgresBenchmarker) InsertRecordsConcurrent(keyType string, numRecords, connections, batchSize int) (*benchmark.ConcurrentBenchmarkResult, error) {
	var wg sync.WaitGroup
	var mu sync.Mutex
	allLatencies := make([]time.Duration, 0, numRecords)
	totalSuccess := 0
	totalErrors := 0

	recordsPerConnection := numRecords / connections
	remainder := numRecords % connections
	startTime := time.Now()

	for workerID := 0; workerID < connections; workerID++ {
		wg.Add(1)

		workerRecords := recordsPerConnection
		if workerID < remainder {
			workerRecords++
		}

		go func(id, records int) {
			defer wg.Done()
			latencies, success, errors := p.runInsertWorker(id, records, keyType, batchSize)

			mu.Lock()
			allLatencies = append(allLatencies, latencies...)
			totalSuccess += success
			totalErrors += errors
			mu.Unlock()
		}(workerID, workerRecords)
	}

	wg.Wait()
	duration := time.Since(startTime)

	p50, p95, p99 := benchmark.CalculatePercentiles(allLatencies)

	return &benchmark.ConcurrentBenchmarkResult{
		Duration:     duration,
		TotalOps:     numRecords,
		Throughput:   float64(numRecords) / duration.Seconds(),
		LatencyP50:   p50,
		LatencyP95:   p95,
		LatencyP99:   p99,
		SuccessCount: totalSuccess,
		ErrorCount:   totalErrors,
	}, nil
}

func (p *PostgresBenchmarker) runInsertWorker(workerID, records int, keyType string, batchSize int) ([]time.Duration, int, int) {
	latencies := make([]time.Duration, 0, records)
	successCount := 0
	errorCount := 0

	if batchSize == 1 {
		for i := 0; i < records; i++ {
			start := time.Now()
			err := p.executeSingleInsert(keyType, workerID*10000+i)
			latencies = append(latencies, time.Since(start))

			if err != nil {
				errorCount++
			} else {
				successCount++
			}
		}
	} else {
		for i := 0; i < records; i += batchSize {
			actualBatchSize := min(batchSize, records-i)

			start := time.Now()
			err := p.executeBatchInsert(keyType, i, actualBatchSize, workerID)
			latencies = append(latencies, time.Since(start))

			if err != nil {
				errorCount += actualBatchSize
			} else {
				successCount += actualBatchSize
			}
		}
	}

	return latencies, successCount, errorCount
}

func (p *PostgresBenchmarker) executeSingleInsert(keyType string, index int) error {
	data := fmt.Sprintf("test_data_%d", index)

	switch keyType {
	case "bigserial":
		_, err := p.db.Exec(
			fmt.Sprintf("INSERT INTO %s (data) VALUES ($1)", p.tableName),
			data,
		)
		return err

	case "uuidv4":
		_, err := p.db.Exec(
			fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1, $2)", p.tableName),
			uuid.New(), data,
		)
		return err

	default:
		return fmt.Errorf("unknown key type: %s", keyType)
	}
}

func (p *PostgresBenchmarker) executeBatchInsert(keyType string, startIdx, batchSize, workerID int) error {
	switch keyType {
	case "bigserial":
		return p.executeBatchBigserial(startIdx, batchSize, workerID)
	case "uuidv4":
		return p.executeBatchUUIDv4(startIdx, batchSize, workerID)
	default:
		return fmt.Errorf("unknown key type: %s", keyType)
	}
}

func formatDataString(index, workerID int) string {
	if workerID >= 0 {
		return fmt.Sprintf("test_data_worker%d_%d", workerID, index)
	}
	return fmt.Sprintf("test_data_%d", index)
}

func (p *PostgresBenchmarker) executeBatchBigserial(startIdx, batchSize, workerID int) error {
	placeholders := make([]string, batchSize)
	args := make([]interface{}, batchSize)

	for i := 0; i < batchSize; i++ {
		placeholders[i] = fmt.Sprintf("($%d)", i+1)
		args[i] = formatDataString(startIdx+i, workerID)
	}

	sql := fmt.Sprintf("INSERT INTO %s (data) VALUES %s",
		p.tableName, strings.Join(placeholders, ", "))

	_, err := p.db.Exec(sql, args...)
	return err
}

func (p *PostgresBenchmarker) executeBatchUUIDv4(startIdx, batchSize, workerID int) error {
	placeholders := make([]string, batchSize)
	args := make([]interface{}, batchSize*2)

	for i := 0; i < batchSize; i++ {
		placeholders[i] = fmt.Sprintf("($%d, $%d)", i*2+1, i*2+2)
		args[i*2] = uuid.New()
		args[i*2+1] = formatDataString(startIdx+i, workerID)
	}

	sql := fmt.Sprintf("INSERT INTO %s (id, data) VALUES %s",
		p.tableName, strings.Join(placeholders, ", "))

	_, err := p.db.Exec(sql, args...)
	return err
}
