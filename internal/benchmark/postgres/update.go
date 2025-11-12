package postgres

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/moguls753/uuid-benchmark/internal/benchmark"
)

// UpdateRandomRecords performs random point updates on the table
func (p *PostgresBenchmarker) UpdateRandomRecords(keyType string, numUpdates, numTotalRecords int) (*benchmark.UpdateBenchmarkResult, error) {
	// Set table name
	p.tableName = fmt.Sprintf("bench_%s", keyType)
	p.indexName = fmt.Sprintf("%s_pkey", p.tableName)

	latencies := make([]time.Duration, 0, numUpdates)
	successCount := 0
	errorCount := 0

	for i := 0; i < numUpdates; i++ {
		start := time.Now()
		err := p.executeRandomUpdate(keyType, i, numTotalRecords)
		latencies = append(latencies, time.Since(start))

		if err != nil {
			errorCount++
		} else {
			successCount++
		}

		if (i+1)%1000 == 0 {
			fmt.Printf("  ... %d/%d updates\n", i+1, numUpdates)
		}
	}

	duration := time.Duration(0)
	for _, lat := range latencies {
		duration += lat
	}

	p50, p95, p99 := benchmark.CalculatePercentiles(latencies)

	return &benchmark.UpdateBenchmarkResult{
		Duration:     duration,
		TotalUpdates: numUpdates,
		Throughput:   float64(numUpdates) / duration.Seconds(),
		LatencyP50:   p50,
		LatencyP95:   p95,
		LatencyP99:   p99,
		SuccessCount: successCount,
		ErrorCount:   errorCount,
	}, nil
}

// UpdateRandomRecordsConcurrent performs random updates using multiple concurrent connections
func (p *PostgresBenchmarker) UpdateRandomRecordsConcurrent(keyType string, numUpdates, connections, numTotalRecords int) (*benchmark.ConcurrentBenchmarkResult, error) {
	// Set table name
	p.tableName = fmt.Sprintf("bench_%s", keyType)
	p.indexName = fmt.Sprintf("%s_pkey", p.tableName)

	var wg sync.WaitGroup
	var mu sync.Mutex
	allLatencies := make([]time.Duration, 0, numUpdates)
	totalSuccess := 0
	totalErrors := 0

	updatesPerConnection := numUpdates / connections
	remainder := numUpdates % connections
	startTime := time.Now()

	for workerID := 0; workerID < connections; workerID++ {
		wg.Add(1)

		workerUpdates := updatesPerConnection
		if workerID < remainder {
			workerUpdates++
		}

		go func(id, updates int) {
			defer wg.Done()
			latencies, success, errors := p.runUpdateWorker(id, updates, keyType, numTotalRecords)

			mu.Lock()
			allLatencies = append(allLatencies, latencies...)
			totalSuccess += success
			totalErrors += errors
			mu.Unlock()
		}(workerID, workerUpdates)
	}

	wg.Wait()
	duration := time.Since(startTime)

	p50, p95, p99 := benchmark.CalculatePercentiles(allLatencies)

	return &benchmark.ConcurrentBenchmarkResult{
		Duration:     duration,
		TotalOps:     numUpdates,
		Throughput:   float64(numUpdates) / duration.Seconds(),
		LatencyP50:   p50,
		LatencyP95:   p95,
		LatencyP99:   p99,
		SuccessCount: totalSuccess,
		ErrorCount:   totalErrors,
	}, nil
}

// UpdateBatchRecords performs updates in batches (multiple updates per transaction)
func (p *PostgresBenchmarker) UpdateBatchRecords(keyType string, numUpdates, batchSize, numTotalRecords int) (*benchmark.UpdateBenchmarkResult, error) {
	// Set table name
	p.tableName = fmt.Sprintf("bench_%s", keyType)
	p.indexName = fmt.Sprintf("%s_pkey", p.tableName)

	latencies := make([]time.Duration, 0, numUpdates/batchSize+1)
	successCount := 0
	errorCount := 0

	for i := 0; i < numUpdates; i += batchSize {
		batchEnd := min(i+batchSize, numUpdates)
		actualBatchSize := batchEnd - i

		start := time.Now()
		err := p.executeBatchUpdate(keyType, i, actualBatchSize, numTotalRecords)
		latencies = append(latencies, time.Since(start))

		if err != nil {
			errorCount += actualBatchSize
		} else {
			successCount += actualBatchSize
		}

		if batchEnd%1000 < batchSize || batchEnd == numUpdates {
			fmt.Printf("  ... %d/%d updates\n", batchEnd, numUpdates)
		}
	}

	duration := time.Duration(0)
	for _, lat := range latencies {
		duration += lat
	}

	p50, p95, p99 := benchmark.CalculatePercentiles(latencies)

	return &benchmark.UpdateBenchmarkResult{
		Duration:     duration,
		TotalUpdates: numUpdates,
		Throughput:   float64(numUpdates) / duration.Seconds(),
		LatencyP50:   p50,
		LatencyP95:   p95,
		LatencyP99:   p99,
		SuccessCount: successCount,
		ErrorCount:   errorCount,
	}, nil
}

// runUpdateWorker executes updates for a single concurrent worker
func (p *PostgresBenchmarker) runUpdateWorker(workerID, updates int, keyType string, numTotalRecords int) ([]time.Duration, int, int) {
	latencies := make([]time.Duration, 0, updates)
	successCount := 0
	errorCount := 0

	for i := 0; i < updates; i++ {
		start := time.Now()
		err := p.executeRandomUpdate(keyType, workerID*10000+i, numTotalRecords)
		latencies = append(latencies, time.Since(start))

		if err != nil {
			errorCount++
		} else {
			successCount++
		}
	}

	return latencies, successCount, errorCount
}

// executeRandomUpdate performs a single random point update
func (p *PostgresBenchmarker) executeRandomUpdate(keyType string, index, numTotalRecords int) error {
	newData := fmt.Sprintf("updated_data_%d", index)

	switch keyType {
	case "bigserial":
		return p.executeUpdateBigserial(newData, numTotalRecords)
	case "uuidv4", "uuidv7", "uuidv1":
		return p.executeUpdateUUIDv4(newData)
	case "ulid":
		return p.executeUpdateULID(newData)
	default:
		return fmt.Errorf("unknown key type: %s", keyType)
	}
}

// executeUpdateBigserial performs a point update for BIGSERIAL
func (p *PostgresBenchmarker) executeUpdateBigserial(newData string, numTotalRecords int) error {
	// Generate random ID in range [1, numTotalRecords]
	randomID := rand.Intn(numTotalRecords) + 1

	query := fmt.Sprintf("UPDATE %s SET data = $1 WHERE id = $2", p.tableName)
	_, err := p.db.Exec(query, newData, randomID)
	return err
}

// executeUpdateUUIDv4 performs a point update for UUIDv4
func (p *PostgresBenchmarker) executeUpdateUUIDv4(newData string) error {
	// For random UUIDs, we need to first fetch a random existing ID
	query := fmt.Sprintf("SELECT id FROM %s ORDER BY RANDOM() LIMIT 1", p.tableName)

	var randomUUID uuid.UUID
	err := p.db.QueryRow(query).Scan(&randomUUID)
	if err != nil {
		return err
	}

	// Now perform the actual update
	updateQuery := fmt.Sprintf("UPDATE %s SET data = $1 WHERE id = $2", p.tableName)
	_, err = p.db.Exec(updateQuery, newData, randomUUID)
	return err
}

// executeUpdateULID performs a point update for ULID (stored as TEXT)
func (p *PostgresBenchmarker) executeUpdateULID(newData string) error {
	// For ULID, we need to first fetch a random existing ID
	query := fmt.Sprintf("SELECT id FROM %s ORDER BY RANDOM() LIMIT 1", p.tableName)

	var randomULID string
	err := p.db.QueryRow(query).Scan(&randomULID)
	if err != nil {
		return err
	}

	// Now perform the actual update
	updateQuery := fmt.Sprintf("UPDATE %s SET data = $1 WHERE id = $2", p.tableName)
	_, err = p.db.Exec(updateQuery, newData, randomULID)
	return err
}

// executeBatchUpdate performs a batch of updates in a single transaction
func (p *PostgresBenchmarker) executeBatchUpdate(keyType string, startIdx, batchSize, numTotalRecords int) error {
	// Begin transaction
	tx, err := p.db.Begin()
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback()

	// Execute batch of updates
	for i := 0; i < batchSize; i++ {
		newData := fmt.Sprintf("updated_data_%d", startIdx+i)

		switch keyType {
		case "bigserial":
			randomID := rand.Intn(numTotalRecords) + 1
			query := fmt.Sprintf("UPDATE %s SET data = $1 WHERE id = $2", p.tableName)
			_, err := tx.Exec(query, newData, randomID)
			if err != nil {
				return err
			}

		case "uuidv4", "uuidv7", "uuidv1":
			// For UUID, fetch random ID first
			query := fmt.Sprintf("SELECT id FROM %s ORDER BY RANDOM() LIMIT 1", p.tableName)
			var randomUUID uuid.UUID
			err := tx.QueryRow(query).Scan(&randomUUID)
			if err != nil {
				return err
			}

			updateQuery := fmt.Sprintf("UPDATE %s SET data = $1 WHERE id = $2", p.tableName)
			_, err = tx.Exec(updateQuery, newData, randomUUID)
			if err != nil {
				return err
			}

		case "ulid":
			// For ULID (TEXT), fetch random ID first
			query := fmt.Sprintf("SELECT id FROM %s ORDER BY RANDOM() LIMIT 1", p.tableName)
			var randomULID string
			err := tx.QueryRow(query).Scan(&randomULID)
			if err != nil {
				return err
			}

			updateQuery := fmt.Sprintf("UPDATE %s SET data = $1 WHERE id = $2", p.tableName)
			_, err = tx.Exec(updateQuery, newData, randomULID)
			if err != nil {
				return err
			}

		default:
			return fmt.Errorf("unknown key type: %s", keyType)
		}
	}

	// Commit transaction
	return tx.Commit()
}
