package postgres

import (
	"fmt"
	"os/exec"
	"strconv"
	"strings"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
)

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
