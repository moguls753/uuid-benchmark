package benchmark

import (
	"fmt"
	"time"
)

// Benchmarker defines the interface that all database benchmark implementations must follow
type Benchmarker interface {
	// Connect establishes a connection to the database
	Connect() error

	// CreateTable creates the benchmark table with the specified key type
	CreateTable(keyType string) error

	// InsertRecords inserts the specified number of records and returns the duration
	InsertRecords(keyType string, numRecords int) (time.Duration, error)

	// MeasureMetrics collects all benchmark metrics (disk usage, fragmentation, page splits)
	MeasureMetrics() (*BenchmarkResult, error)

	// Close closes the database connection
	Close() error
}

// BenchmarkResult holds all metrics collected during a benchmark run
type BenchmarkResult struct {
	InsertDuration   time.Duration
	Throughput       float64
	PageSplits       int
	TableSize        int64
	IndexSize        int64
	Fragmentation    IndexFragmentationStats
}

// IndexFragmentationStats holds index fragmentation metrics
type IndexFragmentationStats struct {
	FragmentationPercent float64
	AvgLeafDensity       float64
	LeafPages            int64
	EmptyPages           int64
}

// FormatBytes formats bytes into human-readable format (KB, MB, GB, etc.)
func FormatBytes(bytes int64) string {
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
