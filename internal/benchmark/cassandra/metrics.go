package cassandra

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/cluster"
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

// CaptureMetricsBeforeAll samples every node's tablestats + info via the
// given Backend and stores the aggregated cluster-wide snapshot as
// c.metricsBefore. Pairs with MeasureMetricsAll: the runner takes a
// before-snapshot here and then computes deltas against the after-snapshot
// returned by MeasureMetricsAll.
func (c *CassandraBenchmarker) CaptureMetricsBeforeAll(b cluster.Backend) error {
	snaps := make([]*CassandraMetricsSnapshot, b.NodeCount())
	for i := 0; i < b.NodeCount(); i++ {
		s, err := c.captureNodeSnapshot(b, i)
		if err != nil {
			return err
		}
		snaps[i] = s
	}
	c.metricsBefore = AggregateSnapshots(snaps)
	return nil
}

// MeasureMetricsAll samples every node again, aggregates, and assembles a
// BenchmarkResult relative to c.metricsBefore (set by an earlier
// CaptureMetricsBeforeAll). Returns the cluster-wide deltas and after-state.
func (c *CassandraBenchmarker) MeasureMetricsAll(b cluster.Backend) (*benchmark.BenchmarkResult, error) {
	snaps := make([]*CassandraMetricsSnapshot, b.NodeCount())
	for i := 0; i < b.NodeCount(); i++ {
		s, err := c.captureNodeSnapshot(b, i)
		if err != nil {
			return nil, err
		}
		snaps[i] = s
	}
	after := AggregateSnapshots(snaps)
	return buildBenchmarkResult(c.metricsBefore, after), nil
}

// captureNodeSnapshot pulls a full per-node snapshot from the i-th node,
// combining `nodetool tablestats` and `nodetool info`. A failure to read
// tablestats fails the whole capture (the bench-table size fields are
// load-bearing). A failure to read `nodetool info` is tolerated with a
// warning, matching the single-node behavior in MeasureMetrics — the cache
// hit rate is non-critical and zero is the existing fallback.
func (c *CassandraBenchmarker) captureNodeSnapshot(b cluster.Backend, i int) (*CassandraMetricsSnapshot, error) {
	table := fmt.Sprintf("%s.%s", c.cfg.Keyspace, c.tableName)
	out, err := b.ExecOnNode(i, "nodetool", "tablestats", table)
	if err != nil {
		return nil, fmt.Errorf("nodetool tablestats on node %d: %w", i, err)
	}
	snap, err := parseTableStats(out)
	if err != nil {
		return nil, fmt.Errorf("parse tablestats node %d: %w", i, err)
	}
	info, err := b.ExecOnNode(i, "nodetool", "info")
	if err != nil {
		fmt.Printf("Warning: Could not measure key cache hit rate on node %d: %v\n", i, err)
		// snap.KeyCacheHitRate stays at the zero value, consistent with
		// single-node fallback in MeasureMetrics.
		return snap, nil
	}
	snap.KeyCacheHitRate = parseKeyCacheHitRate(info)
	return snap, nil
}

// AggregateSnapshots sums per-node metrics into a single cluster-wide
// snapshot. Counters (SSTableCount, SpaceUsed*, MemtableSwitchCount,
// BloomFilterFP, CompactionBytesTotal) are summed. Ratios
// (BloomFilterFPRatio, KeyCacheHitRate) are averaged across non-nil
// snapshots. Nil entries in the input are skipped — callers can pass a
// partial slice when a node's capture failed and the failure was tolerated
// upstream.
func AggregateSnapshots(snaps []*CassandraMetricsSnapshot) *CassandraMetricsSnapshot {
	out := &CassandraMetricsSnapshot{}
	var fpRatioSum, keyHitSum float64
	n := 0
	for _, s := range snaps {
		if s == nil {
			continue
		}
		out.SSTableCount += s.SSTableCount
		out.SpaceUsedLive += s.SpaceUsedLive
		out.SpaceUsedTotal += s.SpaceUsedTotal
		out.MemtableSwitchCount += s.MemtableSwitchCount
		out.BloomFilterFP += s.BloomFilterFP
		out.CompactionBytesTotal += s.CompactionBytesTotal
		fpRatioSum += s.BloomFilterFPRatio
		keyHitSum += s.KeyCacheHitRate
		n++
	}
	if n > 0 {
		out.BloomFilterFPRatio = fpRatioSum / float64(n)
		out.KeyCacheHitRate = keyHitSum / float64(n)
	}
	return out
}

// buildBenchmarkResult assembles the BenchmarkResult from a before/after
// pair. Extracted from the previous inline body of MeasureMetrics so the
// single-node and multi-node paths produce identical results. before may
// be nil (no prior snapshot taken); in that case PageSplits and
// BloomFilterFP deltas are left at zero.
func buildBenchmarkResult(before, after *CassandraMetricsSnapshot) *benchmark.BenchmarkResult {
	result := &benchmark.BenchmarkResult{}

	// Disk usage: space used live as table size.
	result.TableSize = after.SpaceUsedLive
	result.IndexSize = 0 // Cassandra doesn't separate index from data in tablestats.

	// Fragmentation: SSTable count and space amplification.
	var fragStats benchmark.IndexFragmentationStats
	if after.SpaceUsedLive > 0 {
		// Space amplification ratio as "fragmentation" proxy.
		fragStats.FragmentationPercent = float64(after.SpaceUsedTotal-after.SpaceUsedLive) / float64(after.SpaceUsedTotal) * 100
	}
	fragStats.LeafPages = int64(after.SSTableCount) // Repurpose LeafPages for SSTable count.
	fragStats.AvgLeafDensity = -1                   // N/A — LSM-tree has no B-tree leaf pages.
	fragStats.EmptyPages = -1                       // N/A — LSM-tree concept doesn't apply.
	result.Fragmentation = fragStats

	// Page splits equivalent: SSTable count delta. Clamp at zero because
	// Cassandra's counters can legitimately decrease across compactions.
	if before != nil {
		delta := after.SSTableCount - before.SSTableCount
		if delta < 0 {
			delta = 0
		}
		result.PageSplits = delta

		fpDelta := after.BloomFilterFP - before.BloomFilterFP
		if fpDelta < 0 {
			fpDelta = 0
		}
		result.BloomFilterFP = fpDelta
	}

	// Cache hit ratio: post-workload value (already on the snapshot).
	result.BufferHitRatio = after.KeyCacheHitRate
	result.IndexBufferHitRatio = result.BufferHitRatio

	return result
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

// Hoisted at package scope so parseKeyCacheHitRate doesn't recompile
// these on every call. nodetool info is a low-frequency probe today
// (one shell-out per workload phase), but the per-node multi-node
// fan-out makes the savings cheap and idiomatic.
var (
	keyCacheHitsReqsRe = regexp.MustCompile(`Key Cache\s*:.*?(\d+)\s+hits,\s*(\d+)\s+requests`)
	keyCacheRateRe     = regexp.MustCompile(`Key Cache\s*:.*?([0-9.]+)\s+recent hit rate`)
)

// parseKeyCacheHitRate extracts the Key Cache hit ratio from `nodetool
// info` output. Prefers the "X hits, Y requests" form (compute directly)
// and falls back to the "<rate> recent hit rate" form. Returns 0 when
// neither form yields a valid value (NaN rate, zero requests, or no Key
// Cache line at all) — matches the pre-extraction fallback behavior.
func parseKeyCacheHitRate(out string) float64 {
	// "Key Cache : entries X, size Y, capacity Z, H hits, R requests, ..."
	if m := keyCacheHitsReqsRe.FindStringSubmatch(out); len(m) >= 3 {
		hits, _ := strconv.ParseFloat(m[1], 64)
		requests, _ := strconv.ParseFloat(m[2], 64)
		if requests > 0 {
			return hits / requests
		}
	}
	// Fallback: trailing "0.42 recent hit rate".
	if m := keyCacheRateRe.FindStringSubmatch(out); len(m) >= 2 {
		if rate, err := strconv.ParseFloat(m[1], 64); err == nil {
			return rate
		}
	}
	return 0
}
