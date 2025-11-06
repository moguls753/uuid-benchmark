package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
)

func main() {
	// Command-line flags
	keyType := flag.String("key-type", "bigserial", "Key type to benchmark (bigserial, uuidv4)")
	numRecords := flag.Int("num-records", 10000, "Number of records to insert")
	flag.Parse()

	fmt.Println("UUID Benchmark - PostgreSQL")
	fmt.Println(strings.Repeat("=", 50))
	fmt.Printf("Key Type: %s\n", *keyType)
	fmt.Printf("Records:  %d\n", *numRecords)
	fmt.Println()

	// Validate key type
	if *keyType != "bigserial" && *keyType != "uuidv4" {
		log.Fatalf("Invalid key-type: %s (must be 'bigserial' or 'uuidv4')", *keyType)
	}

	// Connect to database
	connStr := "host=localhost port=5432 user=benchmark password=benchmark123 dbname=uuid_benchmark sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Test connection
	err = db.Ping()
	if err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	fmt.Println("✓ Connected to PostgreSQL")

	// Enable pgstattuple extension for index statistics
	_, err = db.Exec("CREATE EXTENSION IF NOT EXISTS pgstattuple")
	if err != nil {
		log.Fatalf("Failed to enable pgstattuple extension: %v", err)
	}

	// Create table
	tableName := fmt.Sprintf("bench_%s", *keyType)
	err = createTable(db, tableName, *keyType)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	fmt.Printf("✓ Created table: %s\n", tableName)

	// Insert records and measure time
	fmt.Printf("→ Inserting %d records...\n", *numRecords)
	duration, err := insertRecords(db, tableName, *keyType, *numRecords)
	if err != nil {
		log.Fatalf("Failed to insert records: %v", err)
	}

	throughput := float64(*numRecords) / duration.Seconds()
	fmt.Printf("✓ Inserted %d records in %s\n", *numRecords, duration.Round(time.Millisecond))
	fmt.Printf("✓ Throughput: %.2f records/sec\n", throughput)

	// Count page splits from WAL
	pageSplits, err := countPageSplits()
	if err != nil {
		log.Printf("Warning: Could not count page splits: %v", err)
	} else {
		fmt.Printf("✓ Page splits: %d\n", pageSplits)
	}

	// Measure disk usage
	tableSize, indexSize, err := measureDiskUsage(db, tableName)
	if err != nil {
		log.Fatalf("Failed to measure disk usage: %v", err)
	}
	fmt.Printf("✓ Table size: %s\n", formatBytes(tableSize))
	fmt.Printf("✓ Index size: %s\n", formatBytes(indexSize))
	fmt.Printf("✓ Total size: %s\n", formatBytes(tableSize+indexSize))

	// Measure index fragmentation
	indexName := fmt.Sprintf("%s_pkey", tableName)
	fragStats, err := measureIndexFragmentation(db, indexName)
	if err != nil {
		log.Fatalf("Failed to measure index fragmentation: %v", err)
	}
	fmt.Println()
	fmt.Println("Index Statistics:")
	fmt.Printf("  Fragmentation:    %.2f%%\n", fragStats.FragmentationPercent)
	fmt.Printf("  Avg Leaf Density: %.2f%%\n", fragStats.AvgLeafDensity)
	fmt.Printf("  Leaf Pages:       %d\n", fragStats.LeafPages)
	fmt.Printf("  Empty Pages:      %d\n", fragStats.EmptyPages)

	fmt.Println()
	fmt.Println("Benchmark completed successfully!")
}

// createTable creates a benchmark table with the specified key type
func createTable(db *sql.DB, tableName, keyType string) error {
	// Drop table if exists
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)
	_, err := db.Exec(dropSQL)
	if err != nil {
		return fmt.Errorf("drop table: %w", err)
	}

	// Create table based on key type
	var createSQL string
	switch keyType {
	case "bigserial":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id BIGSERIAL PRIMARY KEY,
				data TEXT,
				created_at TIMESTAMP DEFAULT NOW()
			)
		`, tableName)
	case "uuidv4":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id UUID PRIMARY KEY,
				data TEXT,
				created_at TIMESTAMP DEFAULT NOW()
			)
		`, tableName)
	default:
		return fmt.Errorf("unknown key type: %s", keyType)
	}

	_, err = db.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	return nil
}

// insertRecords inserts the specified number of records and returns the duration
func insertRecords(db *sql.DB, tableName, keyType string, numRecords int) (time.Duration, error) {
	startTime := time.Now()

	for i := 0; i < numRecords; i++ {
		var err error

		switch keyType {
		case "bigserial":
			// BIGSERIAL auto-generates the ID
			data := fmt.Sprintf("test_data_%d", i)
			_, err = db.Exec(
				fmt.Sprintf("INSERT INTO %s (data) VALUES ($1)", tableName),
				data,
			)

		case "uuidv4":
			// Generate random UUID
			id := uuid.New()
			data := fmt.Sprintf("test_data_%d", i)
			_, err = db.Exec(
				fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1, $2)", tableName),
				id, data,
			)

		default:
			return 0, fmt.Errorf("unknown key type: %s", keyType)
		}

		if err != nil {
			return 0, fmt.Errorf("insert record %d: %w", i, err)
		}

		// Progress indicator every 1000 records
		if (i+1)%1000 == 0 {
			fmt.Printf("  ... %d/%d\n", i+1, numRecords)
		}
	}

	duration := time.Since(startTime)
	return duration, nil
}

// measureDiskUsage queries PostgreSQL for table and index sizes
func measureDiskUsage(db *sql.DB, tableName string) (tableSize, indexSize int64, err error) {
	// Get table size
	err = db.QueryRow("SELECT pg_table_size($1)", tableName).Scan(&tableSize)
	if err != nil {
		return 0, 0, fmt.Errorf("query table size: %w", err)
	}

	// Get index size
	err = db.QueryRow("SELECT pg_indexes_size($1)", tableName).Scan(&indexSize)
	if err != nil {
		return 0, 0, fmt.Errorf("query index size: %w", err)
	}

	return tableSize, indexSize, nil
}

// IndexFragmentationStats holds index fragmentation metrics
type IndexFragmentationStats struct {
	FragmentationPercent float64
	AvgLeafDensity       float64
	LeafPages            int64
	EmptyPages           int64
}

// measureIndexFragmentation queries PostgreSQL for index fragmentation statistics
func measureIndexFragmentation(db *sql.DB, indexName string) (IndexFragmentationStats, error) {
	var stats IndexFragmentationStats

	query := `
		SELECT
			leaf_fragmentation,
			avg_leaf_density,
			leaf_pages,
			empty_pages
		FROM pgstatindex($1)
	`

	err := db.QueryRow(query, indexName).Scan(
		&stats.FragmentationPercent,
		&stats.AvgLeafDensity,
		&stats.LeafPages,
		&stats.EmptyPages,
	)

	if err != nil {
		return stats, fmt.Errorf("query index statistics: %w", err)
	}

	return stats, nil
}

// formatBytes formats bytes into human-readable format
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// countPageSplits counts B-tree page splits from PostgreSQL WAL using pg_waldump
func countPageSplits() (int, error) {
	// Run pg_waldump inside the PostgreSQL container to count SPLIT operations
	// Use [0-9]* pattern to match only WAL segment files, not subdirectories like 'summaries'
	cmd := exec.Command("docker", "exec", "uuid-bench-postgres",
		"sh", "-c",
		"pg_waldump /var/lib/postgresql/data/pg_wal/[0-9]* 2>/dev/null | grep -c SPLIT || true")

	output, err := cmd.Output()
	if err != nil {
		return 0, fmt.Errorf("execute pg_waldump: %w", err)
	}

	// Parse the count from output
	countStr := strings.TrimSpace(string(output))
	count, err := strconv.Atoi(countStr)
	if err != nil {
		return 0, fmt.Errorf("parse count '%s': %w", countStr, err)
	}

	return count, nil
}
