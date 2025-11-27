package postgres

import (
	"bytes"
	"crypto/rand"
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
		// NOTE: Using ulid.Make() generates ULIDs with current timestamp.
		// When generating many ULIDs rapidly (>10k/sec), most fall into the same
		// millisecond bucket, causing the 80-bit random suffix to dominate sorting.
		// This leads to poor B-tree performance similar to UUIDv4:
		//   - Standard ULID: 606 page splits (+122% vs BIGSERIAL, 100k records)
		//   - UUIDv7 (12-bit random): 384 page splits (+41% vs BIGSERIAL)
		//
		// THESIS FINDING: Monotonic ULID mode does NOT help for pre-generated batches!
		// When ULIDs are generated in a tight loop before insertion (as done here),
		// even monotonic mode produces ULIDs with identical timestamps, defeating
		// the purpose. Monotonic mode only helps when ULIDs are generated during
		// insertion (e.g., as database function), not for client-side batch generation.
		//
		// Conclusion: For batch operations, UUIDv7 is superior due to smaller random
		// component (12 bits vs ULID's 80 bits).
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

// generateMonotonicULIDInsertScript creates a SQL script with monotonic ULIDs
// Monotonic ULIDs increment the random part when timestamp is the same,
// ensuring sortability even during high-frequency batch generation.
func generateMonotonicULIDInsertScript(tableName string, numRecords, batchSize int) string {
	var buf bytes.Buffer

	// Create monotonic entropy source
	entropy := ulid.Monotonic(rand.Reader, 0)

	recordsInCurrentBatch := 0
	for i := 0; i < numRecords; i++ {
		// Start new transaction
		if recordsInCurrentBatch == 0 {
			buf.WriteString("BEGIN;\n")
		}

		// Generate monotonic ULID
		// When timestamp is the same, entropy increments instead of being random
		id := ulid.MustNew(ulid.Timestamp(time.Now()), entropy)
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

// LoadMonotonicULIDData pre-generates monotonic ULIDs and bulk-loads them
// This variant uses monotonic mode to ensure sortability during batch operations
func LoadMonotonicULIDData(cfg ULIDLoaderConfig) (*ULIDLoadResult, error) {
	startTime := time.Now()

	// Generate SQL script with monotonic ULIDs
	script := generateMonotonicULIDInsertScript(cfg.TableName, cfg.NumRecords, cfg.BatchSize)

	// Copy script to container
	scriptName := fmt.Sprintf("ulid_monotonic_insert_%d.sql", time.Now().Unix())
	containerPath, err := pgbench.CopyScriptToContainer(cfg.ContainerName, script, scriptName)
	if err != nil {
		return nil, fmt.Errorf("failed to copy monotonic ULID script to container: %w", err)
	}

	// Execute script via psql
	result, err := pgbench.ExecuteSQLFile(cfg.ContainerName, containerPath)
	if err != nil {
		return nil, fmt.Errorf("failed to execute monotonic ULID load script: %w", err)
	}

	if result.ExitCode != 0 {
		return nil, fmt.Errorf("monotonic ULID load script failed with exit code %d: %s", result.ExitCode, result.Stderr)
	}

	duration := time.Since(startTime)

	return &ULIDLoadResult{
		Duration:      duration,
		RecordsLoaded: cfg.NumRecords,
	}, nil
}
