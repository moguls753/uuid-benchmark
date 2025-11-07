package postgres

import (
	"database/sql"
	"fmt"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/lib/pq"
	"github.com/moguls753/uuid-benchmark/internal/benchmark"
)

// PostgresBenchmarker implements the Benchmarker interface for PostgreSQL
type PostgresBenchmarker struct {
	db        *sql.DB
	keyType   string
	tableName string
	indexName string
}

// New creates a new PostgreSQL benchmarker instance
func New() *PostgresBenchmarker {
	return &PostgresBenchmarker{}
}

// Connect establishes a connection to the PostgreSQL database
func (p *PostgresBenchmarker) Connect() error {
	connStr := "host=localhost port=5432 user=benchmark password=benchmark123 dbname=uuid_benchmark sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}

	// Test connection
	err = db.Ping()
	if err != nil {
		return fmt.Errorf("ping database: %w", err)
	}

	p.db = db

	// Enable pgstattuple extension for index statistics
	_, err = p.db.Exec("CREATE EXTENSION IF NOT EXISTS pgstattuple")
	if err != nil {
		return fmt.Errorf("enable pgstattuple extension: %w", err)
	}

	return nil
}

// CreateTable creates the benchmark table with the specified key type
func (p *PostgresBenchmarker) CreateTable(keyType string) error {
	p.keyType = keyType
	p.tableName = fmt.Sprintf("bench_%s", keyType)
	p.indexName = fmt.Sprintf("%s_pkey", p.tableName)

	// Drop table if exists
	dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", p.tableName)
	_, err := p.db.Exec(dropSQL)
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
		`, p.tableName)
	case "uuidv4":
		createSQL = fmt.Sprintf(`
			CREATE TABLE %s (
				id UUID PRIMARY KEY,
				data TEXT,
				created_at TIMESTAMP DEFAULT NOW()
			)
		`, p.tableName)
	default:
		return fmt.Errorf("unknown key type: %s", keyType)
	}

	_, err = p.db.Exec(createSQL)
	if err != nil {
		return fmt.Errorf("create table: %w", err)
	}

	return nil
}

// InsertRecords inserts the specified number of records and returns the duration
func (p *PostgresBenchmarker) InsertRecords(keyType string, numRecords int) (time.Duration, error) {
	startTime := time.Now()

	for i := 0; i < numRecords; i++ {
		var err error

		switch keyType {
		case "bigserial":
			// BIGSERIAL auto-generates the ID
			data := fmt.Sprintf("test_data_%d", i)
			_, err = p.db.Exec(
				fmt.Sprintf("INSERT INTO %s (data) VALUES ($1)", p.tableName),
				data,
			)

		case "uuidv4":
			// Generate random UUID
			id := uuid.New()
			data := fmt.Sprintf("test_data_%d", i)
			_, err = p.db.Exec(
				fmt.Sprintf("INSERT INTO %s (id, data) VALUES ($1, $2)", p.tableName),
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

// MeasureMetrics collects all benchmark metrics
func (p *PostgresBenchmarker) MeasureMetrics() (*benchmark.BenchmarkResult, error) {
	result := &benchmark.BenchmarkResult{}

	// Measure disk usage
	tableSize, indexSize, err := p.measureDiskUsage()
	if err != nil {
		return nil, fmt.Errorf("measure disk usage: %w", err)
	}
	result.TableSize = tableSize
	result.IndexSize = indexSize

	// Measure index fragmentation
	fragStats, err := p.measureIndexFragmentation()
	if err != nil {
		return nil, fmt.Errorf("measure fragmentation: %w", err)
	}
	result.Fragmentation = fragStats

	// Count page splits from WAL
	pageSplits, err := p.countPageSplits()
	if err != nil {
		// Page splits are optional, just log warning
		fmt.Printf("Warning: Could not count page splits: %v\n", err)
		result.PageSplits = 0
	} else {
		result.PageSplits = pageSplits
	}

	return result, nil
}

// Close closes the database connection
func (p *PostgresBenchmarker) Close() error {
	if p.db != nil {
		return p.db.Close()
	}
	return nil
}

// measureDiskUsage queries PostgreSQL for table and index sizes
func (p *PostgresBenchmarker) measureDiskUsage() (tableSize, indexSize int64, err error) {
	// Get table size
	err = p.db.QueryRow("SELECT pg_table_size($1)", p.tableName).Scan(&tableSize)
	if err != nil {
		return 0, 0, fmt.Errorf("query table size: %w", err)
	}

	// Get index size
	err = p.db.QueryRow("SELECT pg_indexes_size($1)", p.tableName).Scan(&indexSize)
	if err != nil {
		return 0, 0, fmt.Errorf("query index size: %w", err)
	}

	return tableSize, indexSize, nil
}

// measureIndexFragmentation queries PostgreSQL for index fragmentation statistics
func (p *PostgresBenchmarker) measureIndexFragmentation() (benchmark.IndexFragmentationStats, error) {
	var stats benchmark.IndexFragmentationStats

	query := `
		SELECT
			leaf_fragmentation,
			avg_leaf_density,
			leaf_pages,
			empty_pages
		FROM pgstatindex($1)
	`

	err := p.db.QueryRow(query, p.indexName).Scan(
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

// countPageSplits counts B-tree page splits from PostgreSQL WAL using pg_waldump
func (p *PostgresBenchmarker) countPageSplits() (int, error) {
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
