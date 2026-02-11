package cassandra

import (
	"bytes"
	"fmt"
	"os/exec"
	"regexp"
	"strconv"
	"strings"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
)

// CassandraMetricsSnapshot holds parsed nodetool tablestats output.
type CassandraMetricsSnapshot struct {
	SSTableCount         int
	SpaceUsedLive        int64
	SpaceUsedTotal       int64
	MemtableSwitchCount  int64
	BloomFilterFP        int64
	BloomFilterFPRatio   float64
	KeyCacheHitRate      float64
	CompactionBytesTotal int64
}

// CaptureMetricsBefore takes a snapshot of tablestats before the workload.
func (c *CassandraBenchmarker) CaptureMetricsBefore() error {
	snapshot, err := c.captureTableStats()
	if err != nil {
		return err
	}
	c.metricsBefore = snapshot
	return nil
}

func (c *CassandraBenchmarker) MeasureMetrics() (*benchmark.BenchmarkResult, error) {
	result := &benchmark.BenchmarkResult{}

	stats, err := c.captureTableStats()
	if err != nil {
		return nil, fmt.Errorf("capture table stats: %w", err)
	}

	// Disk usage: space used live as table size
	result.TableSize = stats.SpaceUsedLive
	result.IndexSize = 0 // Cassandra doesn't separate index from data in tablestats

	// Fragmentation: SSTable count and space amplification
	var fragStats benchmark.IndexFragmentationStats
	if stats.SpaceUsedLive > 0 {
		// Space amplification ratio as "fragmentation" proxy
		fragStats.FragmentationPercent = float64(stats.SpaceUsedTotal-stats.SpaceUsedLive) / float64(stats.SpaceUsedTotal) * 100
	}
	fragStats.LeafPages = int64(stats.SSTableCount) // Repurpose LeafPages for SSTable count
	fragStats.AvgLeafDensity = -1 // N/A — LSM-tree has no B-tree leaf pages
	fragStats.EmptyPages = -1 // N/A — LSM-tree concept doesn't apply
	result.Fragmentation = fragStats

	// Page splits equivalent: compaction delta (if before snapshot available)
	if c.metricsBefore != nil {
		delta := stats.SSTableCount - c.metricsBefore.SSTableCount
		if delta < 0 {
			delta = 0
		}
		result.PageSplits = delta // Repurpose for SSTable count change
	}

	// Cache hit ratio from nodetool info
	hitRatio, err := c.measureKeyCacheHitRate()
	if err != nil {
		fmt.Printf("Warning: Could not measure key cache hit rate: %v\n", err)
		result.BufferHitRatio = 0
	} else {
		result.BufferHitRatio = hitRatio
	}
	result.IndexBufferHitRatio = result.BufferHitRatio

	return result, nil
}

func (c *CassandraBenchmarker) captureTableStats() (*CassandraMetricsSnapshot, error) {
	cmd := exec.Command("docker", "exec", ContainerName,
		"nodetool", "tablestats", fmt.Sprintf("%s.%s", keyspace, c.tableName))

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("nodetool tablestats: %w (stderr: %s)", err, stderr.String())
	}

	return parseTableStats(stdout.String())
}

func parseTableStats(output string) (*CassandraMetricsSnapshot, error) {
	snapshot := &CassandraMetricsSnapshot{}

	lines := strings.Split(output, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)

		if val, ok := parseTableStatsInt(line, "SSTable count:"); ok {
			snapshot.SSTableCount = int(val)
		}
		if val, ok := parseTableStatsInt(line, "Space used (live):"); ok {
			snapshot.SpaceUsedLive = val
		}
		if val, ok := parseTableStatsInt(line, "Space used (total):"); ok {
			snapshot.SpaceUsedTotal = val
		}
		if val, ok := parseTableStatsInt(line, "Memtable switch count:"); ok {
			snapshot.MemtableSwitchCount = val
		}
		if val, ok := parseTableStatsInt(line, "Bloom filter false positives:"); ok {
			snapshot.BloomFilterFP = val
		}
		if val, ok := parseTableStatsFloat(line, "Bloom filter false ratio:"); ok {
			snapshot.BloomFilterFPRatio = val
		}
	}

	return snapshot, nil
}

func parseTableStatsInt(line, prefix string) (int64, bool) {
	if !strings.HasPrefix(line, prefix) {
		return 0, false
	}
	valStr := strings.TrimSpace(strings.TrimPrefix(line, prefix))
	val, err := strconv.ParseInt(valStr, 10, 64)
	if err != nil {
		return 0, false
	}
	return val, true
}

func parseTableStatsFloat(line, prefix string) (float64, bool) {
	if !strings.HasPrefix(line, prefix) {
		return 0, false
	}
	valStr := strings.TrimSpace(strings.TrimPrefix(line, prefix))
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return 0, false
	}
	return val, true
}

func (c *CassandraBenchmarker) measureKeyCacheHitRate() (float64, error) {
	cmd := exec.Command("docker", "exec", ContainerName, "nodetool", "info")

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return 0, fmt.Errorf("nodetool info: %w (stderr: %s)", err, stderr.String())
	}

	// Parse "Key Cache : entries X, size Y, capacity Z, X hits, Y requests, A recent hit rate"
	// Or "Key Cache        : entries 0, size 0 bytes, capacity 25 MB, 0 hits, 0 requests, NaN recent hit rate"
	re := regexp.MustCompile(`Key Cache\s*:.*?(\d+)\s+hits,\s*(\d+)\s+requests`)
	matches := re.FindStringSubmatch(stdout.String())
	if len(matches) >= 3 {
		hits, _ := strconv.ParseFloat(matches[1], 64)
		requests, _ := strconv.ParseFloat(matches[2], 64)
		if requests > 0 {
			return hits / requests, nil
		}
	}

	// Try parsing "recent hit rate" directly
	reRate := regexp.MustCompile(`Key Cache\s*:.*?([0-9.]+)\s+recent hit rate`)
	matchesRate := reRate.FindStringSubmatch(stdout.String())
	if len(matchesRate) >= 2 {
		rate, err := strconv.ParseFloat(matchesRate[1], 64)
		if err == nil {
			return rate, nil
		}
	}

	return 0, nil
}
