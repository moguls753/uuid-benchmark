package cassandra

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/cluster"
)

// CassandraMetricsSnapshot holds parsed nodetool tablestats output.
type CassandraMetricsSnapshot struct {
	SSTableCount        int
	SpaceUsedLive       int64
	SpaceUsedTotal      int64
	MemtableSwitchCount int64
	BloomFilterFP       int64
	BloomFilterFPRatio  float64
	KeyCacheHitRate     float64
}

// CaptureMetricsBeforeAll samples every node's tablestats + info via the
// given Backend and stores the per-node before-snapshots on the receiver.
// Pairs with MeasureMetricsAll, which computes per-node deltas against
// these snapshots and clamps each one before summing — see
// buildBenchmarkResultPerNode for why per-node clamping matters.
//
// Per-node calls run concurrently to bound the snapshot window. With N
// remote nodes the previous serial implementation accumulated N RTTs of
// drift between the first and last node's snapshot; parallelising
// collapses that to a single RTT and makes the snapshot ~symmetric across
// nodes. Errors from any node fail the whole capture (errors.Join surfaces
// all failing nodes at once) — partial captures would silently produce
// zero deltas in MeasureMetricsAll, which is exactly the foot-gun this
// fail-loud path is designed to prevent.
func (c *CassandraBenchmarker) CaptureMetricsBeforeAll(b cluster.Backend) error {
	snaps, err := c.captureAllNodes(b)
	if err != nil {
		return err
	}
	c.metricsBeforeNodes = snaps
	return nil
}

// MeasureMetricsAll samples every node again concurrently and assembles a
// BenchmarkResult by computing per-node deltas against the before-snapshots
// stored by CaptureMetricsBeforeAll, then aggregating. Per-node deltas for
// PageSplits (SSTableCount-based) and BloomFilterFP are clamped at zero
// before summing — this prevents one node's compaction-induced decrease
// from cancelling another node's workload-induced increase in the cluster
// sum, which would silently understate real ingest activity.
//
// Fails loudly if any node's snapshot errors (consistent with
// CaptureMetricsBeforeAll's fail-loud contract).
func (c *CassandraBenchmarker) MeasureMetricsAll(b cluster.Backend) (*benchmark.BenchmarkResult, error) {
	after, err := c.captureAllNodes(b)
	if err != nil {
		return nil, err
	}
	return buildBenchmarkResultPerNode(c.metricsBeforeNodes, after), nil
}

// captureAllNodes runs captureNodeSnapshot on every node concurrently.
// On any per-node failure all errors are joined and returned — operators
// see every failing node at once instead of having to retry to discover
// the second failure. Returns a fully-populated snapshot slice on success;
// on error the slice may be partially populated and must not be used.
func (c *CassandraBenchmarker) captureAllNodes(b cluster.Backend) ([]*CassandraMetricsSnapshot, error) {
	n := b.NodeCount()
	snaps := make([]*CassandraMetricsSnapshot, n)
	errs := make([]error, n)

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s, err := c.captureNodeSnapshot(b, i)
			snaps[i] = s
			errs[i] = err
		}(i)
	}
	wg.Wait()

	if err := errors.Join(errs...); err != nil {
		return snaps, err
	}
	return snaps, nil
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
// BloomFilterFP) are summed. Ratios (BloomFilterFPRatio, KeyCacheHitRate)
// are averaged across non-nil snapshots. Nil entries in the input are
// skipped — callers can pass a partial slice when a node's capture failed
// and the failure was tolerated upstream.
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

// buildBenchmarkResultPerNode assembles the BenchmarkResult from per-node
// before/after slices. Per-node deltas for PageSplits (SSTableCount-based)
// and BloomFilterFP are clamped at zero individually before summing — this
// avoids the masking effect of clamping the cluster sum after aggregation,
// where one node's compaction-induced decrease could silently cancel
// another node's workload-induced increase.
//
// Aggregate (non-delta) fields like TableSize and BufferHitRatio come from
// summing/averaging the after-snapshots. The before slice may be nil (no
// prior snapshot taken — fail-soft fallback); in that case PageSplits and
// BloomFilterFP deltas are left at zero. If before is non-nil it must have
// the same length as after.
func buildBenchmarkResultPerNode(before, after []*CassandraMetricsSnapshot) *benchmark.BenchmarkResult {
	afterAgg := AggregateSnapshots(after)
	result := &benchmark.BenchmarkResult{}

	// Disk usage: space used live as table size.
	result.TableSize = afterAgg.SpaceUsedLive
	result.IndexSize = 0 // Cassandra doesn't separate index from data in tablestats.

	// Fragmentation: SSTable count and space amplification.
	var fragStats benchmark.IndexFragmentationStats
	if afterAgg.SpaceUsedTotal > 0 {
		// Space amplification ratio as "fragmentation" proxy.
		fragStats.FragmentationPercent = float64(afterAgg.SpaceUsedTotal-afterAgg.SpaceUsedLive) / float64(afterAgg.SpaceUsedTotal) * 100
	}
	fragStats.LeafPages = int64(afterAgg.SSTableCount) // Repurpose LeafPages for SSTable count.
	fragStats.AvgLeafDensity = -1                      // N/A — LSM-tree has no B-tree leaf pages.
	fragStats.EmptyPages = -1                          // N/A — LSM-tree concept doesn't apply.
	result.Fragmentation = fragStats

	// Per-node delta with clamp-then-sum. Per-node clamping is load-bearing:
	// if node A compacted (SSTableCount went down) and node B took a workload
	// hit (SSTableCount went up), summing first and clamping last would
	// understate B's activity. Clamping per-node first and summing reports
	// the actual ingest pressure.
	if before != nil {
		result.PageSplits = int(sumClampedDelta(before, after, func(s *CassandraMetricsSnapshot) int64 {
			return int64(s.SSTableCount)
		}))
		result.BloomFilterFP = sumClampedDelta(before, after, func(s *CassandraMetricsSnapshot) int64 {
			return s.BloomFilterFP
		})
	}

	// Cache hit ratio: cluster-wide average of after-snapshot values.
	result.BufferHitRatio = afterAgg.KeyCacheHitRate
	result.IndexBufferHitRatio = result.BufferHitRatio

	return result
}

// sumClampedDelta computes Σ max(0, field(after[i]) - field(before[i])) over
// every node where both snapshots exist. Length mismatches between before
// and after are tolerated by iterating to the shorter length — defensive
// only, the orchestrator always pairs matched-length slices.
func sumClampedDelta(before, after []*CassandraMetricsSnapshot, field func(*CassandraMetricsSnapshot) int64) int64 {
	n := len(before)
	if len(after) < n {
		n = len(after)
	}
	var total int64
	for i := 0; i < n; i++ {
		if before[i] == nil || after[i] == nil {
			continue
		}
		delta := field(after[i]) - field(before[i])
		if delta > 0 {
			total += delta
		}
	}
	return total
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
