package postgres

import (
	"bytes"
	"fmt"
	"time"

	"github.com/oklog/ulid/v2"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/postgres/pgbench"
)

// ULIDLoaderConfig holds configuration for ULID pre-loading
type ULIDLoaderConfig struct {
	ContainerName string
	TableName     string
	NumRecords    int
	BatchSize     int // Number of INSERTs per transaction
}

// ULIDLoadResult contains the result of ULID loading
type ULIDLoadResult struct {
	Duration      time.Duration
	RecordsLoaded int
}

// LoadULIDData pre-generates ULIDs and bulk-loads them into the container
// This eliminates network overhead during benchmark execution
func LoadULIDData(cfg ULIDLoaderConfig) (*ULIDLoadResult, error) {
	startTime := time.Now()

	// Generate SQL script with all ULIDs
	script := generateULIDInsertScript(cfg.TableName, cfg.NumRecords, cfg.BatchSize)

	// Copy script to container
	scriptName := fmt.Sprintf("ulid_insert_%d.sql", time.Now().Unix())
	containerPath, err := pgbench.CopyScriptToContainer(cfg.ContainerName, script, scriptName)
	if err != nil {
		return nil, fmt.Errorf("failed to copy ULID script to container: %w", err)
	}

	// Execute script via psql
	result, err := pgbench.ExecuteSQLFile(cfg.ContainerName, containerPath)
	if err != nil {
		return nil, fmt.Errorf("failed to execute ULID load script: %w", err)
	}

	if result.ExitCode != 0 {
		return nil, fmt.Errorf("ULID load script failed with exit code %d: %s", result.ExitCode, result.Stderr)
	}

	duration := time.Since(startTime)

	return &ULIDLoadResult{
		Duration:      duration,
		RecordsLoaded: cfg.NumRecords,
	}, nil
}

// generateULIDInsertScript creates a SQL script with batched ULID inserts
// Example output:
// BEGIN;
// INSERT INTO bench_ulid (id, data) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAV', 'test_data_1');
// INSERT INTO bench_ulid (id, data) VALUES ('01ARZ3NDEKTSV4RRFFQ69G5FAW', 'test_data_2');
// ...
// COMMIT;
func generateULIDInsertScript(tableName string, numRecords, batchSize int) string {
	var buf bytes.Buffer

	recordsInCurrentBatch := 0
	for i := 0; i < numRecords; i++ {
		// Start new transaction
		if recordsInCurrentBatch == 0 {
			buf.WriteString("BEGIN;\n")
		}

		// Generate ULID
		id := ulid.Make()
		data := fmt.Sprintf("test_data_%d", i)

		// Write INSERT statement
		buf.WriteString(fmt.Sprintf("INSERT INTO %s (id, data) VALUES ('%s', '%s');\n",
			tableName, id.String(), data))

		recordsInCurrentBatch++

		// Commit transaction if batch is full or this is the last record
		if recordsInCurrentBatch >= batchSize || i == numRecords-1 {
			buf.WriteString("COMMIT;\n\n")
			recordsInCurrentBatch = 0
		}
	}

	return buf.String()
}

// GenerateULIDs generates a slice of ULID strings for use in bulk operations
// This is useful when you need the ULIDs in memory for other purposes
func GenerateULIDs(count int) []string {
	ulids := make([]string, count)
	for i := 0; i < count; i++ {
		ulids[i] = ulid.Make().String()
	}
	return ulids
}

// CreateULIDInsertScriptFromList creates a SQL script from a pre-generated list of ULIDs
func CreateULIDInsertScriptFromList(tableName string, ulids []string, batchSize int) string {
	var buf bytes.Buffer

	recordsInCurrentBatch := 0
	for i, id := range ulids {
		// Start new transaction
		if recordsInCurrentBatch == 0 {
			buf.WriteString("BEGIN;\n")
		}

		data := fmt.Sprintf("test_data_%d", i)

		// Write INSERT statement
		buf.WriteString(fmt.Sprintf("INSERT INTO %s (id, data) VALUES ('%s', '%s');\n",
			tableName, id, data))

		recordsInCurrentBatch++

		// Commit transaction if batch is full or this is the last record
		if recordsInCurrentBatch >= batchSize || i == len(ulids)-1 {
			buf.WriteString("COMMIT;\n\n")
			recordsInCurrentBatch = 0
		}
	}

	return buf.String()
}
