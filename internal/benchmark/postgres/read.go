package postgres

import (
	"database/sql"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/moguls753/uuid-benchmark/internal/benchmark"
)

// ReadRandomRecords performs random point lookups on the table
func (p *PostgresBenchmarker) ReadRandomRecords(keyType string, numReads, numTotalRecords int) (*benchmark.ReadBenchmarkResult, error) {
	// Set table name
	p.tableName = fmt.Sprintf("bench_%s", keyType)
	p.indexName = fmt.Sprintf("%s_pkey", p.tableName)

	latencies := make([]time.Duration, 0, numReads)
	var totalRowsReturned int64

	for i := 0; i < numReads; i++ {
		start := time.Now()
		rows, err := p.executeRandomRead(keyType, numTotalRecords)
		latencies = append(latencies, time.Since(start))

		if err != nil {
			return nil, fmt.Errorf("read %d: %w", i, err)
		}
		totalRowsReturned += rows

		if (i+1)%1000 == 0 {
			fmt.Printf("  ... %d/%d reads\n", i+1, numReads)
		}
	}

	duration := time.Duration(0)
	for _, lat := range latencies {
		duration += lat
	}

	p50, p95, p99 := benchmark.CalculatePercentiles(latencies)

	return &benchmark.ReadBenchmarkResult{
		Duration:     duration,
		TotalReads:   numReads,
		Throughput:   float64(numReads) / duration.Seconds(),
		LatencyP50:   p50,
		LatencyP95:   p95,
		LatencyP99:   p99,
		RowsReturned: totalRowsReturned,
	}, nil
}

// ReadRandomRecordsConcurrent performs random reads using multiple concurrent connections
func (p *PostgresBenchmarker) ReadRandomRecordsConcurrent(keyType string, numReads, connections, numTotalRecords int) (*benchmark.ConcurrentBenchmarkResult, error) {
	// Set table name
	p.tableName = fmt.Sprintf("bench_%s", keyType)
	p.indexName = fmt.Sprintf("%s_pkey", p.tableName)

	var wg sync.WaitGroup
	var mu sync.Mutex
	allLatencies := make([]time.Duration, 0, numReads)
	totalSuccess := 0
	totalErrors := 0

	readsPerConnection := numReads / connections
	remainder := numReads % connections
	startTime := time.Now()

	for workerID := 0; workerID < connections; workerID++ {
		wg.Add(1)

		workerReads := readsPerConnection
		if workerID < remainder {
			workerReads++
		}

		go func(id, reads int) {
			defer wg.Done()
			latencies, success, errors := p.runReadWorker(id, reads, keyType, numTotalRecords)

			mu.Lock()
			allLatencies = append(allLatencies, latencies...)
			totalSuccess += success
			totalErrors += errors
			mu.Unlock()
		}(workerID, workerReads)
	}

	wg.Wait()
	duration := time.Since(startTime)

	p50, p95, p99 := benchmark.CalculatePercentiles(allLatencies)

	return &benchmark.ConcurrentBenchmarkResult{
		Duration:     duration,
		TotalOps:     numReads,
		Throughput:   float64(numReads) / duration.Seconds(),
		LatencyP50:   p50,
		LatencyP95:   p95,
		LatencyP99:   p99,
		SuccessCount: totalSuccess,
		ErrorCount:   totalErrors,
	}, nil
}

// ReadRangeScans performs range scans on the table
func (p *PostgresBenchmarker) ReadRangeScans(keyType string, numScans, rangeSize, numTotalRecords int) (*benchmark.ReadBenchmarkResult, error) {
	// Set table name
	p.tableName = fmt.Sprintf("bench_%s", keyType)
	p.indexName = fmt.Sprintf("%s_pkey", p.tableName)

	latencies := make([]time.Duration, 0, numScans)
	var totalRowsReturned int64

	for i := 0; i < numScans; i++ {
		start := time.Now()
		rows, err := p.executeRangeScan(keyType, rangeSize, numTotalRecords)
		latencies = append(latencies, time.Since(start))

		if err != nil {
			return nil, fmt.Errorf("range scan %d: %w", i, err)
		}
		totalRowsReturned += rows

		if (i+1)%100 == 0 {
			fmt.Printf("  ... %d/%d scans\n", i+1, numScans)
		}
	}

	duration := time.Duration(0)
	for _, lat := range latencies {
		duration += lat
	}

	p50, p95, p99 := benchmark.CalculatePercentiles(latencies)

	return &benchmark.ReadBenchmarkResult{
		Duration:     duration,
		TotalReads:   numScans,
		Throughput:   float64(numScans) / duration.Seconds(),
		LatencyP50:   p50,
		LatencyP95:   p95,
		LatencyP99:   p99,
		RowsReturned: totalRowsReturned,
	}, nil
}

// ReadSequentialScan performs a full table scan
func (p *PostgresBenchmarker) ReadSequentialScan(keyType string) (time.Duration, int64, error) {
	// Set table name
	p.tableName = fmt.Sprintf("bench_%s", keyType)
	p.indexName = fmt.Sprintf("%s_pkey", p.tableName)

	startTime := time.Now()

	query := fmt.Sprintf("SELECT * FROM %s", p.tableName)
	rows, err := p.db.Query(query)
	if err != nil {
		return 0, 0, fmt.Errorf("sequential scan: %w", err)
	}
	defer rows.Close()

	var count int64
	for rows.Next() {
		count++
	}

	if err := rows.Err(); err != nil {
		return 0, 0, fmt.Errorf("scan rows: %w", err)
	}

	return time.Since(startTime), count, nil
}

// runReadWorker executes reads for a single concurrent worker
func (p *PostgresBenchmarker) runReadWorker(workerID, reads int, keyType string, numTotalRecords int) ([]time.Duration, int, int) {
	latencies := make([]time.Duration, 0, reads)
	successCount := 0
	errorCount := 0

	for i := 0; i < reads; i++ {
		start := time.Now()
		_, err := p.executeRandomRead(keyType, numTotalRecords)
		latencies = append(latencies, time.Since(start))

		if err != nil {
			errorCount++
		} else {
			successCount++
		}
	}

	return latencies, successCount, errorCount
}

// executeRandomRead performs a single random point lookup
func (p *PostgresBenchmarker) executeRandomRead(keyType string, numTotalRecords int) (int64, error) {
	switch keyType {
	case "bigserial":
		return p.executeReadBigserial(numTotalRecords)
	case "uuidv4", "uuidv7", "uuidv1":
		return p.executeReadUUIDv4()
	case "ulid":
		return p.executeReadULID()
	default:
		return 0, fmt.Errorf("unknown key type: %s", keyType)
	}
}

// executeReadBigserial performs a point lookup for BIGSERIAL
func (p *PostgresBenchmarker) executeReadBigserial(numTotalRecords int) (int64, error) {
	// Generate random ID in range [1, numTotalRecords]
	randomID := rand.Intn(numTotalRecords) + 1

	query := fmt.Sprintf("SELECT id, data, created_at FROM %s WHERE id = $1", p.tableName)

	var id int64
	var data string
	var createdAt time.Time

	err := p.db.QueryRow(query, randomID).Scan(&id, &data, &createdAt)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	return 1, nil
}

// executeReadUUIDv4 performs a point lookup for UUIDv4
func (p *PostgresBenchmarker) executeReadUUIDv4() (int64, error) {
	// For random UUIDs, we need to first fetch a random existing ID
	query := fmt.Sprintf("SELECT id FROM %s ORDER BY RANDOM() LIMIT 1", p.tableName)

	var randomUUID uuid.UUID
	err := p.db.QueryRow(query).Scan(&randomUUID)
	if err != nil {
		return 0, err
	}

	// Now perform the actual point lookup
	selectQuery := fmt.Sprintf("SELECT id, data, created_at FROM %s WHERE id = $1", p.tableName)

	var id uuid.UUID
	var data string
	var createdAt time.Time

	err = p.db.QueryRow(selectQuery, randomUUID).Scan(&id, &data, &createdAt)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	return 1, nil
}

// executeReadULID performs a point lookup for ULID (stored as TEXT)
func (p *PostgresBenchmarker) executeReadULID() (int64, error) {
	// For ULID, we need to first fetch a random existing ID
	query := fmt.Sprintf("SELECT id FROM %s ORDER BY RANDOM() LIMIT 1", p.tableName)

	var randomULID string
	err := p.db.QueryRow(query).Scan(&randomULID)
	if err != nil {
		return 0, err
	}

	// Now perform the actual point lookup
	selectQuery := fmt.Sprintf("SELECT id, data, created_at FROM %s WHERE id = $1", p.tableName)

	var id string
	var data string
	var createdAt time.Time

	err = p.db.QueryRow(selectQuery, randomULID).Scan(&id, &data, &createdAt)
	if err == sql.ErrNoRows {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}

	return 1, nil
}

// executeRangeScan performs a range scan
func (p *PostgresBenchmarker) executeRangeScan(keyType string, rangeSize, numTotalRecords int) (int64, error) {
	switch keyType {
	case "bigserial":
		return p.executeRangeScanBigserial(rangeSize, numTotalRecords)
	case "uuidv4", "uuidv7", "uuidv1":
		return p.executeRangeScanUUIDv4(rangeSize)
	case "ulid":
		return p.executeRangeScanULID(rangeSize)
	default:
		return 0, fmt.Errorf("unknown key type: %s", keyType)
	}
}

// executeRangeScanBigserial performs a range scan for BIGSERIAL
func (p *PostgresBenchmarker) executeRangeScanBigserial(rangeSize, numTotalRecords int) (int64, error) {
	// Generate random starting point
	maxStart := numTotalRecords - rangeSize
	if maxStart < 1 {
		maxStart = 1
	}
	startID := rand.Intn(maxStart) + 1
	endID := startID + rangeSize

	query := fmt.Sprintf("SELECT id, data, created_at FROM %s WHERE id >= $1 AND id < $2", p.tableName)
	rows, err := p.db.Query(query, startID, endID)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var count int64
	for rows.Next() {
		count++
	}

	return count, rows.Err()
}

// executeRangeScanUUIDv4 performs a range scan for UUIDv4
func (p *PostgresBenchmarker) executeRangeScanUUIDv4(rangeSize int) (int64, error) {
	// For UUIDs, we'll fetch N consecutive rows after a random starting point
	// This simulates a range scan on the index order
	query := fmt.Sprintf("SELECT id, data, created_at FROM %s ORDER BY id OFFSET (SELECT FLOOR(RANDOM() * COUNT(*)) FROM %s) LIMIT $1",
		p.tableName, p.tableName)

	rows, err := p.db.Query(query, rangeSize)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var count int64
	for rows.Next() {
		count++
	}

	return count, rows.Err()
}

// executeRangeScanULID performs a range scan for ULID (stored as TEXT)
func (p *PostgresBenchmarker) executeRangeScanULID(rangeSize int) (int64, error) {
	// For ULID (TEXT), we'll fetch N consecutive rows after a random starting point
	// This simulates a range scan on the index order
	query := fmt.Sprintf("SELECT id, data, created_at FROM %s ORDER BY id OFFSET (SELECT FLOOR(RANDOM() * COUNT(*)) FROM %s) LIMIT $1",
		p.tableName, p.tableName)

	rows, err := p.db.Query(query, rangeSize)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	var count int64
	for rows.Next() {
		count++
	}

	return count, rows.Err()
}
