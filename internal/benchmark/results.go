package benchmark

import "time"

// InsertPerformanceResult contains results for the insert-performance scenario
type InsertPerformanceResult struct {
	KeyType            string
	NumRecords         int
	BatchSize          int
	Connections        int
	Duration           time.Duration
	Throughput         float64
	PageSplits         int
	TableSize          int64
	IndexSize          int64
	Fragmentation      IndexFragmentationStats
	LatencyP50         time.Duration
	LatencyP95         time.Duration
	LatencyP99         time.Duration
	ReadIOPS           float64
	WriteIOPS          float64
	ReadThroughputMB   float64
	WriteThroughputMB  float64
}

// ReadAfterFragmentationResult contains results for the read-after-fragmentation scenario
type ReadAfterFragmentationResult struct {
	KeyType             string
	NumRecords          int
	NumReads            int
	InsertDuration      time.Duration
	ReadDuration        time.Duration
	ReadThroughput      float64
	Fragmentation       IndexFragmentationStats
	BufferHitRatio      float64
	IndexBufferHitRatio float64
	LatencyP50          time.Duration
	LatencyP95          time.Duration
	LatencyP99          time.Duration
	ReadIOPS            float64
	WriteIOPS           float64
	ReadThroughputMB    float64
	WriteThroughputMB   float64
}

// UpdatePerformanceResult contains results for the update-performance scenario
type UpdatePerformanceResult struct {
	KeyType            string
	NumRecords         int
	NumUpdates         int
	BatchSize          int
	UpdateDuration     time.Duration
	UpdateThroughput   float64
	Fragmentation      IndexFragmentationStats
	LatencyP50         time.Duration
	LatencyP95         time.Duration
	LatencyP99         time.Duration
	ReadIOPS           float64
	WriteIOPS          float64
	ReadThroughputMB   float64
	WriteThroughputMB  float64
}

// ConcurrentInsertResult contains results for the concurrent-insert scenario
type ConcurrentInsertResult struct {
	KeyType       string
	NumRecords    int
	BatchSize     int
	Connections   int
	Duration      time.Duration
	Throughput    float64
	PageSplits    int
	TableSize     int64
	IndexSize     int64
	Fragmentation IndexFragmentationStats
	LatencyP50    time.Duration
	LatencyP95    time.Duration
	LatencyP99    time.Duration
}

// MixedWorkloadResult contains results for the mixed-workload scenario
type MixedWorkloadResult struct {
	KeyType             string
	NumRecords          int
	Duration            time.Duration
	TotalOps            int
	InsertOps           int
	ReadOps             int
	UpdateOps           int
	OverallThroughput   float64
	InsertThroughput    float64
	ReadThroughput      float64
	UpdateThroughput    float64
	BufferHitRatio      float64
	IndexBufferHitRatio float64
	Fragmentation       IndexFragmentationStats
	TableSize           int64
	IndexSize           int64
	ReadIOPS            float64
	WriteIOPS           float64
	ReadThroughputMB    float64
	WriteThroughputMB   float64
}
