package benchmark

import (
	"fmt"
	"sort"
	"time"
)

// Note: The Benchmarker interface was removed as it was not enforced and contained
// only legacy method signatures from the pre-pgbench architecture.
// PostgresBenchmarker implements pgbench-based methods directly without interface constraints.

type BenchmarkResult struct {
	InsertDuration      time.Duration
	Throughput          float64
	PageSplits          int
	TableSize           int64
	IndexSize           int64
	Fragmentation       IndexFragmentationStats
	BufferHitRatio      float64 // Cache hit ratio (0.0 to 1.0)
	IndexBufferHitRatio float64 // Index-specific cache hit ratio
}

type IndexFragmentationStats struct {
	FragmentationPercent float64
	AvgLeafDensity       float64
	LeafPages            int64
	EmptyPages           int64
}

// ConcurrentBenchmarkResult holds results from concurrent pgbench operations
type ConcurrentBenchmarkResult struct {
	Duration     time.Duration
	TotalOps     int
	Throughput   float64
	LatencyP50   time.Duration
	LatencyP95   time.Duration
	LatencyP99   time.Duration
	SuccessCount int
	ErrorCount   int
}

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

func CalculatePercentiles(latencies []time.Duration) (p50, p95, p99 time.Duration) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}

	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})

	n := len(latencies)
	p50 = latencies[n*50/100]
	p95 = latencies[n*95/100]
	p99 = latencies[n*99/100]

	return p50, p95, p99
}
