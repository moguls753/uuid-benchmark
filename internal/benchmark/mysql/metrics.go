package mysql

import (
	"fmt"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
)

func (m *MySQLBenchmarker) MeasureMetrics() (*benchmark.BenchmarkResult, error) {
	result := &benchmark.BenchmarkResult{}

	// Force MySQL to update table statistics before measuring
	_, _ = m.db.Exec(fmt.Sprintf("ANALYZE TABLE %s", m.tableName))

	tableSize, indexSize, err := m.measureDiskUsage()
	if err != nil {
		return nil, fmt.Errorf("measure disk usage: %w", err)
	}
	result.TableSize = tableSize
	result.IndexSize = indexSize

	fragStats, err := m.measureIndexFragmentation()
	if err != nil {
		fmt.Printf("Warning: Could not measure fragmentation: %v\n", err)
		result.Fragmentation = benchmark.IndexFragmentationStats{
			AvgLeafDensity: -1,
			EmptyPages:     -1,
		}
	} else {
		result.Fragmentation = fragStats
	}

	pageSplits, err := m.countPageSplits()
	if err != nil {
		fmt.Printf("Warning: Could not count page splits: %v\n", err)
		result.PageSplits = 0
	} else {
		result.PageSplits = pageSplits
	}

	bufferHitRatio, indexHitRatio, err := m.measureBufferHitRatios()
	if err != nil {
		fmt.Printf("Warning: Could not measure buffer hit ratios: %v\n", err)
		result.BufferHitRatio = 0
		result.IndexBufferHitRatio = 0
	} else {
		result.BufferHitRatio = bufferHitRatio
		result.IndexBufferHitRatio = indexHitRatio
	}

	return result, nil
}

func (m *MySQLBenchmarker) measureDiskUsage() (tableSize, indexSize int64, err error) {
	// MySQL uses information_schema.tables for size information
	// Note: For InnoDB with clustered primary key, data_length includes the PK index
	// index_length only shows secondary indexes. We report data_length as index_size
	// since the B-tree IS the data in InnoDB.
	query := `
		SELECT
			COALESCE(data_length, 0) as data_size,
			COALESCE(index_length, 0) as secondary_index_size
		FROM information_schema.tables
		WHERE table_schema = ? AND table_name = ?
	`

	var dataSize, secondaryIndexSize int64
	err = m.db.QueryRow(query, dbName, m.tableName).Scan(&dataSize, &secondaryIndexSize)
	if err != nil {
		return 0, 0, fmt.Errorf("query table size: %w", err)
	}

	// For InnoDB: data_length is the clustered index (PK + row data)
	// We report total size as tableSize, and data_length as indexSize
	// since the primary key B-tree structure is what we're measuring
	tableSize = dataSize + secondaryIndexSize
	indexSize = dataSize // The clustered index IS the data

	return tableSize, indexSize, nil
}

func (m *MySQLBenchmarker) measureIndexFragmentation() (benchmark.IndexFragmentationStats, error) {
	var stats benchmark.IndexFragmentationStats

	// MySQL provides B-tree statistics via mysql.innodb_index_stats
	// This is not exactly the same as PostgreSQL's pgstatindex (which measures physical
	// page ordering), but gives us useful B-tree structure information.
	query := `
		SELECT
			COALESCE(MAX(CASE WHEN stat_name = 'n_leaf_pages' THEN stat_value END), 0) as leaf_pages,
			COALESCE(MAX(CASE WHEN stat_name = 'size' THEN stat_value END), 0) as total_pages
		FROM mysql.innodb_index_stats
		WHERE database_name = ? AND table_name = ? AND index_name = 'PRIMARY'
	`

	var leafPages, totalPages int64
	err := m.db.QueryRow(query, dbName, m.tableName).Scan(&leafPages, &totalPages)
	if err != nil {
		return stats, fmt.Errorf("query innodb_index_stats: %w", err)
	}

	stats.LeafPages = leafPages

	// InnoDB does not expose actual leaf page fill factor.
	// Only PostgreSQL's pgstatindex() provides this metric.
	stats.AvgLeafDensity = -1 // N/A — not exposed by InnoDB

	// Calculate internal (non-leaf) pages as a measure of B-tree depth/overhead
	// More internal pages relative to leaf pages indicates deeper tree / more overhead
	if totalPages > 0 && leafPages > 0 {
		internalPages := totalPages - leafPages
		// "Fragmentation" here means B-tree overhead: internal pages as % of total
		// A perfectly flat tree would have ~0%, deeper trees have more overhead
		stats.FragmentationPercent = float64(internalPages) / float64(totalPages) * 100
	}

	stats.EmptyPages = -1 // N/A — InnoDB doesn't expose this

	return stats, nil
}

func (m *MySQLBenchmarker) capturePageSplitCount() (int64, error) {
	// MySQL tracks page splits globally via innodb_metrics or global status
	// SHOW GLOBAL STATUS LIKE 'Innodb_pages_split' doesn't exist in all versions
	// Use information_schema.innodb_metrics instead
	var count int64

	// Try innodb_metrics first (MySQL 5.6+)
	query := `
		SELECT COALESCE(count, 0)
		FROM information_schema.innodb_metrics
		WHERE name = 'index_page_splits'
	`
	err := m.db.QueryRow(query).Scan(&count)
	if err != nil {
		// Fallback: some MySQL versions might not have this
		// Return 0 and let the caller handle it
		return 0, nil
	}

	return count, nil
}

func (m *MySQLBenchmarker) countPageSplits() (int, error) {
	if !m.pageSplitCountsCaptured {
		return 0, fmt.Errorf("page split counts not captured")
	}

	delta := m.endPageSplitCount - m.startPageSplitCount
	if delta < 0 {
		delta = 0
	}

	return int(delta), nil
}

func (m *MySQLBenchmarker) measureBufferHitRatios() (float64, float64, error) {
	// MySQL buffer pool hit ratio from global status
	var bufferHitRatio float64

	// Buffer pool read requests (hits + misses) and reads from disk (misses)
	query := `
		SELECT
			COALESCE(
				(SELECT variable_value FROM performance_schema.global_status WHERE variable_name = 'Innodb_buffer_pool_read_requests'),
				'0'
			) as read_requests,
			COALESCE(
				(SELECT variable_value FROM performance_schema.global_status WHERE variable_name = 'Innodb_buffer_pool_reads'),
				'0'
			) as disk_reads
	`

	var readRequestsStr, diskReadsStr string
	err := m.db.QueryRow(query).Scan(&readRequestsStr, &diskReadsStr)
	if err != nil {
		// Fallback to information_schema for older MySQL versions
		query = `
			SELECT
				COALESCE(
					(SELECT variable_value FROM information_schema.global_status WHERE variable_name = 'Innodb_buffer_pool_read_requests'),
					'0'
				) as read_requests,
				COALESCE(
					(SELECT variable_value FROM information_schema.global_status WHERE variable_name = 'Innodb_buffer_pool_reads'),
					'0'
				) as disk_reads
		`
		err = m.db.QueryRow(query).Scan(&readRequestsStr, &diskReadsStr)
		if err != nil {
			return 0, 0, fmt.Errorf("query buffer stats: %w", err)
		}
	}

	var readRequests, diskReads int64
	fmt.Sscanf(readRequestsStr, "%d", &readRequests)
	fmt.Sscanf(diskReadsStr, "%d", &diskReads)

	if readRequests > 0 {
		hits := readRequests - diskReads
		bufferHitRatio = float64(hits) / float64(readRequests)
	}

	// MySQL doesn't have per-index buffer hit ratio like PostgreSQL
	// Return the same value for both
	return bufferHitRatio, bufferHitRatio, nil
}

func (m *MySQLBenchmarker) ResetStats() error {
	// MySQL doesn't have a direct equivalent to pg_stat_reset()
	// We can flush status, but it requires RELOAD privilege
	// For benchmarking purposes, we track deltas instead
	return nil
}
