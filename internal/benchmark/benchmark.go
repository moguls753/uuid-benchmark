package benchmark

import (
	"fmt"
	"sort"
	"time"
)

type Benchmarker interface {
	Connect() error
	CreateTable(keyType string) error
	InsertRecords(keyType string, numRecords, batchSize int) (time.Duration, error)
	InsertRecordsConcurrent(keyType string, numRecords, connections, batchSize int) (*ConcurrentBenchmarkResult, error)
	ReadRandomRecords(keyType string, numReads, numTotalRecords int) (*ReadBenchmarkResult, error)
	ReadRandomRecordsConcurrent(keyType string, numReads, connections, numTotalRecords int) (*ConcurrentBenchmarkResult, error)
	ReadRangeScans(keyType string, numScans, rangeSize, numTotalRecords int) (*ReadBenchmarkResult, error)
	ReadSequentialScan(keyType string) (time.Duration, int64, error)
	UpdateRandomRecords(keyType string, numUpdates, numTotalRecords int) (*UpdateBenchmarkResult, error)
	UpdateRandomRecordsConcurrent(keyType string, numUpdates, connections, numTotalRecords int) (*ConcurrentBenchmarkResult, error)
	UpdateBatchRecords(keyType string, numUpdates, batchSize, numTotalRecords int) (*UpdateBenchmarkResult, error)
	MeasureMetrics() (*BenchmarkResult, error)
	Close() error
}

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

type ReadBenchmarkResult struct {
	Duration     time.Duration
	TotalReads   int
	Throughput   float64
	LatencyP50   time.Duration
	LatencyP95   time.Duration
	LatencyP99   time.Duration
	RowsReturned int64
}

type UpdateBenchmarkResult struct {
	Duration     time.Duration
	TotalUpdates int
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
