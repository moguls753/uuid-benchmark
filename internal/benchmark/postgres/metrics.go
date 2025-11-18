package postgres

import (
	"fmt"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
)

func (p *PostgresBenchmarker) MeasureMetrics() (*benchmark.BenchmarkResult, error) {
	result := &benchmark.BenchmarkResult{}

	tableSize, indexSize, err := p.measureDiskUsage()
	if err != nil {
		return nil, fmt.Errorf("measure disk usage: %w", err)
	}
	result.TableSize = tableSize
	result.IndexSize = indexSize

	fragStats, err := p.measureIndexFragmentation()
	if err != nil {
		return nil, fmt.Errorf("measure fragmentation: %w", err)
	}
	result.Fragmentation = fragStats

	pageSplits, err := p.countPageSplits()
	if err != nil {
		fmt.Printf("Warning: Could not count page splits: %v\n", err)
		result.PageSplits = 0
	} else {
		result.PageSplits = pageSplits
	}

	bufferHitRatio, indexHitRatio, err := p.measureBufferHitRatios()
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

func (p *PostgresBenchmarker) measureDiskUsage() (tableSize, indexSize int64, err error) {
	err = p.db.QueryRow("SELECT pg_table_size($1)", p.tableName).Scan(&tableSize)
	if err != nil {
		return 0, 0, fmt.Errorf("query table size: %w", err)
	}

	err = p.db.QueryRow("SELECT pg_indexes_size($1)", p.tableName).Scan(&indexSize)
	if err != nil {
		return 0, 0, fmt.Errorf("query index size: %w", err)
	}

	return tableSize, indexSize, nil
}

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

func (p *PostgresBenchmarker) getCurrentLSN() (string, error) {
	var lsn string
	err := p.db.QueryRow("SELECT pg_current_wal_lsn()::text").Scan(&lsn)
	if err != nil {
		return "", fmt.Errorf("query current LSN: %w", err)
	}
	return lsn, nil
}

func (p *PostgresBenchmarker) countPageSplits() (int, error) {
	if p.startLSN == "" || p.endLSN == "" {
		return 0, fmt.Errorf("LSN range not captured (startLSN=%q, endLSN=%q)", p.startLSN, p.endLSN)
	}

	query := `
		SELECT COALESCE(SUM(count), 0)::int
		FROM pg_get_wal_stats($1::pg_lsn, $2::pg_lsn, per_record := true)
		WHERE "resource_manager/record_type" IN ('Btree/SPLIT_L', 'Btree/SPLIT_R')
	`

	var count int
	err := p.db.QueryRow(query, p.startLSN, p.endLSN).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("query page splits (LSN %s to %s): %w", p.startLSN, p.endLSN, err)
	}

	return count, nil
}

func (p *PostgresBenchmarker) measureBufferHitRatios() (float64, float64, error) {
	var bufferHitRatio float64
	bufferQuery := `
		SELECT
			COALESCE(blks_hit::float / NULLIF(blks_hit + blks_read, 0), 0) AS cache_hit_ratio
		FROM pg_stat_database
		WHERE datname = 'uuid_benchmark'
	`
	err := p.db.QueryRow(bufferQuery).Scan(&bufferHitRatio)
	if err != nil {
		return 0, 0, fmt.Errorf("query buffer hit ratio: %w", err)
	}

	var indexHitRatio float64
	indexQuery := `
		SELECT
			COALESCE(idx_blks_hit::float / NULLIF(idx_blks_hit + idx_blks_read, 0), 0) AS index_hit_ratio
		FROM pg_statio_user_tables
		WHERE relname = $1
	`
	err = p.db.QueryRow(indexQuery, p.tableName).Scan(&indexHitRatio)
	if err != nil {
		indexHitRatio = 0
	}

	return bufferHitRatio, indexHitRatio, nil
}

func (p *PostgresBenchmarker) ResetStats() error {
	_, err := p.db.Exec("SELECT pg_stat_reset()")
	if err != nil {
		return fmt.Errorf("reset stats: %w", err)
	}
	return nil
}
