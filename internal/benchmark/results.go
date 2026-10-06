package benchmark

import "time"

type InsertPerformanceResult struct {
	KeyType           string
	NumRecords        int
	BatchSize         int
	Connections       int
	Counts            OperationCounts
	Duration          time.Duration
	Throughput        float64
	PageSplits        int
	TableSize         int64
	IndexSize         int64
	Fragmentation     IndexFragmentationStats
	LatencyP50        time.Duration
	LatencyP95        time.Duration
	LatencyP99        time.Duration
	ReadIOPS          float64
	WriteIOPS         float64
	ReadThroughputMB  float64
	WriteThroughputMB float64
	// IOValid reports whether the cgroup window was captured at all. A failed
	// capture leaves the four fields above at zero, and zero is the most
	// favourable value read_iops can take: without this flag a missing
	// measurement enters a rank test as the strongest possible result.
	IOValid bool
}

// OperationCounts records how a measured phase actually ended. Throughput
// counts attempts, so without these the success rate of a run is unknowable
// after the fact — the gap that blocks every result claim from the June 2026
// datasets. NotFound is tracked apart from Failed because the read targets are
// drawn during the insert: a row that never landed returns fast and would
// otherwise pass for a quick, healthy operation.
type OperationCounts struct {
	Attempted int
	Succeeded int
	Failed    int
	NotFound  int
}

type ReadPerformanceResult struct {
	KeyType        string
	NumRecords     int
	NumReads       int
	InsertDuration time.Duration
	FetchDuration  time.Duration
	Counts         OperationCounts
	// InsertFailed is the dataset bootstrap's failed row count. Kept apart
	// from Counts, which describes the measured phase: a bootstrap that lost
	// rows is otherwise only visible as a 0.2 %-diluted shortfall in the
	// target set, which is far coarser than the number the insert already had.
	InsertFailed int
	// IDFileSHA256 fingerprints the target set this run measured against.
	IDFileSHA256        string
	TableSize           int64
	ReadDuration        time.Duration
	ReadThroughput      float64
	Fragmentation       IndexFragmentationStats
	BufferHitRatio      float64
	IndexBufferHitRatio float64
	BloomFilterFP       int64
	LatencyP50          time.Duration
	LatencyP95          time.Duration
	LatencyP99          time.Duration
	ReadIOPS            float64
	WriteIOPS           float64
	ReadThroughputMB    float64
	WriteThroughputMB   float64
	// IOValid reports whether the cgroup window was captured at all. See the
	// note on InsertPerformanceResult.IOValid.
	IOValid bool
}

type UpdatePerformanceResult struct {
	KeyType           string
	NumRecords        int
	NumUpdates        int
	BatchSize         int
	InsertDuration    time.Duration
	FetchDuration     time.Duration
	Counts            OperationCounts
	InsertFailed      int
	IDFileSHA256      string
	UpdateDuration    time.Duration
	UpdateThroughput  float64
	Fragmentation     IndexFragmentationStats
	LatencyP50        time.Duration
	LatencyP95        time.Duration
	LatencyP99        time.Duration
	ReadIOPS          float64
	WriteIOPS         float64
	ReadThroughputMB  float64
	WriteThroughputMB float64
	// IOValid reports whether the cgroup window was captured at all. A failed
	// capture leaves the four fields above at zero, and zero is the most
	// favourable value read_iops can take: without this flag a missing
	// measurement enters a rank test as the strongest possible result.
	IOValid bool
}

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
	LatencyP50          time.Duration
	LatencyP95          time.Duration
	LatencyP99          time.Duration
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
