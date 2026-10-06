package cassandra

import (
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/cluster"
)

const realisticTableStatsOutput = `Total number of tables: 45
----------------
Keyspace: uuid_benchmark
	Read Count: 0
	Read Latency: NaN ms
	Write Count: 100000
	Write Latency: 0.025 ms
	Pending Flushes: 0
		Table: bench
		SSTable count: 4
		Old SSTable count: 0
		Max SSTable size: 0
		Space used (live): 52428800
		Space used (total): 62914560
		Space used by snapshots (total): 0
		Off heap memory used (total): 12345
		SSTable Compression Ratio: 0.5
		Number of partitions (estimate): 100000
		Memtable cell count: 5000
		Memtable data size: 1048576
		Memtable off heap memory used: 0
		Memtable switch count: 7
		Local read count: 0
		Local read latency: NaN ms
		Local write count: 100000
		Local write latency: 0.025 ms
		Pending flushes: 0
		Percent repaired: 100.0
		Bloom filter false positives: 42
		Bloom filter false ratio: 0.00123
		Bloom filter space used: 24000
		Bloom filter off heap memory used: 24000
		Index summary off heap memory used: 4096
		Compression metadata off heap memory used: 8192
		Compacted partition minimum bytes: 36
		Compacted partition mean bytes: 525
		Compacted partition maximum bytes: 892
		Average live cells per slice (last five minutes): 1.0
		Maximum live cells per slice (last five minutes): 1
		Average tombstones per slice (last five minutes): 0.0
		Maximum tombstones per slice (last five minutes): 0
		Dropped Mutations: 0
		Droppable tombstone ratio: 0.00000`

func TestParseTableStats(t *testing.T) {
	t.Parallel()

	t.Run("realistic output", func(t *testing.T) {
		t.Parallel()
		snapshot, err := parseTableStats(realisticTableStatsOutput)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if snapshot.SSTableCount != 4 {
			t.Errorf("SSTableCount = %d, want 4", snapshot.SSTableCount)
		}
		if snapshot.SpaceUsedLive != 52428800 {
			t.Errorf("SpaceUsedLive = %d, want 52428800", snapshot.SpaceUsedLive)
		}
		if snapshot.SpaceUsedTotal != 62914560 {
			t.Errorf("SpaceUsedTotal = %d, want 62914560", snapshot.SpaceUsedTotal)
		}
		if snapshot.MemtableSwitchCount != 7 {
			t.Errorf("MemtableSwitchCount = %d, want 7", snapshot.MemtableSwitchCount)
		}
		if snapshot.BloomFilterFP != 42 {
			t.Errorf("BloomFilterFP = %d, want 42", snapshot.BloomFilterFP)
		}
		if snapshot.BloomFilterFPRatio < 0.00122 || snapshot.BloomFilterFPRatio > 0.00124 {
			t.Errorf("BloomFilterFPRatio = %v, want ~0.00123", snapshot.BloomFilterFPRatio)
		}
	})

	t.Run("partial output missing fields default to zero", func(t *testing.T) {
		t.Parallel()
		output := `		SSTable count: 12
		Space used (live): 999999`
		snapshot, err := parseTableStats(output)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if snapshot.SSTableCount != 12 {
			t.Errorf("SSTableCount = %d, want 12", snapshot.SSTableCount)
		}
		if snapshot.SpaceUsedLive != 999999 {
			t.Errorf("SpaceUsedLive = %d, want 999999", snapshot.SpaceUsedLive)
		}
		if snapshot.MemtableSwitchCount != 0 {
			t.Errorf("MemtableSwitchCount = %d, want 0 (not in output)", snapshot.MemtableSwitchCount)
		}
	})

	t.Run("malformed values silently skipped", func(t *testing.T) {
		t.Parallel()
		output := `		SSTable count: not_a_number
		Space used (live): 12345
		Bloom filter false ratio: invalid`
		snapshot, err := parseTableStats(output)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if snapshot.SSTableCount != 0 {
			t.Errorf("SSTableCount = %d, want 0 (malformed)", snapshot.SSTableCount)
		}
		if snapshot.SpaceUsedLive != 12345 {
			t.Errorf("SpaceUsedLive = %d, want 12345", snapshot.SpaceUsedLive)
		}
		if snapshot.BloomFilterFPRatio != 0 {
			t.Errorf("BloomFilterFPRatio = %v, want 0 (malformed)", snapshot.BloomFilterFPRatio)
		}
	})
}

func TestAggregateSnapshots(t *testing.T) {
	t.Parallel()
	t.Run("two-node sum + ratio mean", func(t *testing.T) {
		t.Parallel()
		a := &CassandraMetricsSnapshot{
			SSTableCount:        4,
			SpaceUsedLive:       100 * 1024 * 1024,
			SpaceUsedTotal:      120 * 1024 * 1024,
			MemtableSwitchCount: 7,
			BloomFilterFP:       42,
			BloomFilterFPRatio:  0.001,
			KeyCacheHitRate:     0.80,
		}
		b := &CassandraMetricsSnapshot{
			SSTableCount:        6,
			SpaceUsedLive:       200 * 1024 * 1024,
			SpaceUsedTotal:      240 * 1024 * 1024,
			MemtableSwitchCount: 9,
			BloomFilterFP:       8,
			BloomFilterFPRatio:  0.003,
			KeyCacheHitRate:     0.90,
		}
		got := AggregateSnapshots([]*CassandraMetricsSnapshot{a, b})
		if got.SSTableCount != 10 {
			t.Errorf("SSTableCount: got %d want 10", got.SSTableCount)
		}
		if got.SpaceUsedLive != 300*1024*1024 {
			t.Errorf("SpaceUsedLive: got %d want %d", got.SpaceUsedLive, 300*1024*1024)
		}
		if got.SpaceUsedTotal != 360*1024*1024 {
			t.Errorf("SpaceUsedTotal: got %d want %d", got.SpaceUsedTotal, 360*1024*1024)
		}
		if got.MemtableSwitchCount != 16 {
			t.Errorf("MemtableSwitchCount: got %d want 16", got.MemtableSwitchCount)
		}
		if got.BloomFilterFP != 50 {
			t.Errorf("BloomFilterFP: got %d want 50", got.BloomFilterFP)
		}
		// Ratios are averaged.
		if got.BloomFilterFPRatio < 0.00199 || got.BloomFilterFPRatio > 0.00201 {
			t.Errorf("BloomFilterFPRatio: got %v want ~0.002", got.BloomFilterFPRatio)
		}
		if got.KeyCacheHitRate < 0.849 || got.KeyCacheHitRate > 0.851 {
			t.Errorf("KeyCacheHitRate: got %v want ~0.85", got.KeyCacheHitRate)
		}
	})
	t.Run("empty input returns zero snapshot", func(t *testing.T) {
		t.Parallel()
		got := AggregateSnapshots(nil)
		if got == nil {
			t.Fatal("expected non-nil zero snapshot, got nil")
		}
		if *got != (CassandraMetricsSnapshot{}) {
			t.Errorf("expected zero-value snapshot, got %+v", got)
		}
	})
	t.Run("nil entries are skipped", func(t *testing.T) {
		t.Parallel()
		// Defense against a partially-populated input — e.g. one node
		// failed and the caller passed nil for its slot.
		a := &CassandraMetricsSnapshot{SSTableCount: 3, KeyCacheHitRate: 0.5}
		got := AggregateSnapshots([]*CassandraMetricsSnapshot{a, nil, a})
		if got.SSTableCount != 6 {
			t.Errorf("SSTableCount: got %d want 6 (nil skipped)", got.SSTableCount)
		}
		if got.KeyCacheHitRate < 0.499 || got.KeyCacheHitRate > 0.501 {
			t.Errorf("KeyCacheHitRate: got %v want ~0.5 (avg over 2 non-nil snaps)", got.KeyCacheHitRate)
		}
	})
	t.Run("single-snapshot aggregation is identity on ratios", func(t *testing.T) {
		t.Parallel()
		// Pins n=1 against a future "off by one" refactor of the
		// average denominator. A single snap's ratios must come through
		// unchanged, not divided by 0 (NaN) or 2 (halved).
		a := &CassandraMetricsSnapshot{
			SSTableCount:       5,
			BloomFilterFPRatio: 0.0037,
			KeyCacheHitRate:    0.73,
		}
		got := AggregateSnapshots([]*CassandraMetricsSnapshot{a})
		if got.SSTableCount != 5 {
			t.Errorf("SSTableCount: got %d want 5", got.SSTableCount)
		}
		if got.BloomFilterFPRatio < 0.00369 || got.BloomFilterFPRatio > 0.00371 {
			t.Errorf("BloomFilterFPRatio: got %v want 0.0037 (identity for n=1)", got.BloomFilterFPRatio)
		}
		if got.KeyCacheHitRate < 0.729 || got.KeyCacheHitRate > 0.731 {
			t.Errorf("KeyCacheHitRate: got %v want 0.73 (identity for n=1)", got.KeyCacheHitRate)
		}
	})
}

func TestBuildBenchmarkResultPerNode(t *testing.T) {
	t.Parallel()
	t.Run("delta + ratio assembly with before snapshot (single node)", func(t *testing.T) {
		t.Parallel()
		before := []*CassandraMetricsSnapshot{{
			SSTableCount:  4,
			BloomFilterFP: 10,
		}}
		after := []*CassandraMetricsSnapshot{{
			SSTableCount:    7,
			SpaceUsedLive:   200 * 1024 * 1024,
			SpaceUsedTotal:  250 * 1024 * 1024,
			BloomFilterFP:   25,
			KeyCacheHitRate: 0.91,
		}}
		got := buildBenchmarkResultPerNode(before, after)
		if got.TableSize != after[0].SpaceUsedLive {
			t.Errorf("TableSize: got %d want %d", got.TableSize, after[0].SpaceUsedLive)
		}
		if got.IndexSize != 0 {
			t.Errorf("IndexSize: got %d want 0 (Cassandra doesn't separate index)", got.IndexSize)
		}
		// PageSplits = SSTableCount delta = 7 - 4 = 3.
		if got.PageSplits != 3 {
			t.Errorf("PageSplits: got %d want 3", got.PageSplits)
		}
		// BloomFilterFP delta = 25 - 10 = 15.
		if got.BloomFilterFP != 15 {
			t.Errorf("BloomFilterFP: got %d want 15", got.BloomFilterFP)
		}
		if got.BufferHitRatio != after[0].KeyCacheHitRate {
			t.Errorf("BufferHitRatio: got %v want %v", got.BufferHitRatio, after[0].KeyCacheHitRate)
		}
		if got.IndexBufferHitRatio != got.BufferHitRatio {
			t.Errorf("IndexBufferHitRatio: got %v want %v (equal to BufferHitRatio)", got.IndexBufferHitRatio, got.BufferHitRatio)
		}
		// Fragmentation% = (total - live) / total * 100 = 50 / 250 * 100 = 20%.
		if got.Fragmentation.FragmentationPercent < 19.9 || got.Fragmentation.FragmentationPercent > 20.1 {
			t.Errorf("FragmentationPercent: got %v want ~20", got.Fragmentation.FragmentationPercent)
		}
		if got.Fragmentation.LeafPages != int64(after[0].SSTableCount) {
			t.Errorf("LeafPages (repurposed for SSTable count): got %d want %d", got.Fragmentation.LeafPages, after[0].SSTableCount)
		}
	})
	t.Run("nil before snapshot leaves delta fields zero", func(t *testing.T) {
		t.Parallel()
		after := []*CassandraMetricsSnapshot{{
			SSTableCount:  3,
			SpaceUsedLive: 100,
			BloomFilterFP: 7,
		}}
		got := buildBenchmarkResultPerNode(nil, after)
		if got.PageSplits != 0 {
			t.Errorf("PageSplits: got %d want 0 (no before)", got.PageSplits)
		}
		if got.BloomFilterFP != 0 {
			t.Errorf("BloomFilterFP: got %d want 0 (no before)", got.BloomFilterFP)
		}
		if got.TableSize != after[0].SpaceUsedLive {
			t.Errorf("TableSize: got %d want %d", got.TableSize, after[0].SpaceUsedLive)
		}
	})
	t.Run("negative single-node delta is clamped to zero", func(t *testing.T) {
		t.Parallel()
		// Cassandra counters can legitimately decrease (e.g. compaction
		// merges SSTables, lowering the count). Per-node clamping reports
		// 0 instead of a negative delta — a "page splits" or "bloom FPs
		// added" measurement can't be negative.
		before := []*CassandraMetricsSnapshot{{SSTableCount: 10, BloomFilterFP: 100}}
		after := []*CassandraMetricsSnapshot{{SSTableCount: 4, BloomFilterFP: 50}}
		got := buildBenchmarkResultPerNode(before, after)
		if got.PageSplits != 0 {
			t.Errorf("PageSplits: got %d want 0 (negative delta clamped)", got.PageSplits)
		}
		if got.BloomFilterFP != 0 {
			t.Errorf("BloomFilterFP: got %d want 0 (negative delta clamped)", got.BloomFilterFP)
		}
	})
	t.Run("per-node clamping doesn't let compaction mask workload across nodes", func(t *testing.T) {
		t.Parallel()
		// Three-node cluster snapshot pair. Node 0 compacted during the
		// window (SSTableCount and BloomFilterFP went DOWN). Node 1 took
		// a workload hit (counters went UP). Node 2 was neutral.
		//
		// Pre-fix behavior (clamp after summing the cluster delta) would
		// silently zero out node 1's signal, because the post-aggregation
		// sum is dominated by node 0's decrease:
		//   PageSplits cluster delta = (4-10) + (15-5) + (2-2) = 4 → reported 4
		//   BloomFilterFP cluster delta = (50-100) + (200-20) + (5-5) = 130 → reported 130
		// Or worse, if node 0's drop exceeded node 1's gain entirely, the
		// cluster delta would clamp to 0 and node 1's workload would
		// vanish from the metric.
		//
		// Post-fix (per-node clamp-then-sum):
		//   PageSplits = max(0, 4-10) + max(0, 15-5) + max(0, 2-2) = 0 + 10 + 0 = 10
		//   BloomFilterFP = max(0, 50-100) + max(0, 200-20) + max(0, 5-5) = 0 + 180 + 0 = 180
		before := []*CassandraMetricsSnapshot{
			{SSTableCount: 10, BloomFilterFP: 100}, // node 0
			{SSTableCount: 5, BloomFilterFP: 20},   // node 1
			{SSTableCount: 2, BloomFilterFP: 5},    // node 2
		}
		after := []*CassandraMetricsSnapshot{
			{SSTableCount: 4, BloomFilterFP: 50},   // node 0: compacted (decrease)
			{SSTableCount: 15, BloomFilterFP: 200}, // node 1: workload (increase)
			{SSTableCount: 2, BloomFilterFP: 5},    // node 2: neutral
		}
		got := buildBenchmarkResultPerNode(before, after)
		if got.PageSplits != 10 {
			t.Errorf("PageSplits: got %d want 10 (per-node clamped sum, not 4)", got.PageSplits)
		}
		if got.BloomFilterFP != 180 {
			t.Errorf("BloomFilterFP: got %d want 180 (per-node clamped sum, not 130)", got.BloomFilterFP)
		}
	})
	t.Run("multi-node cumulative deltas all positive — straight sum", func(t *testing.T) {
		t.Parallel()
		// Sanity check: when every node went up, per-node clamping is a
		// no-op and the result equals the straight sum of deltas.
		before := []*CassandraMetricsSnapshot{
			{SSTableCount: 1, BloomFilterFP: 10},
			{SSTableCount: 2, BloomFilterFP: 20},
		}
		after := []*CassandraMetricsSnapshot{
			{SSTableCount: 5, BloomFilterFP: 50},
			{SSTableCount: 7, BloomFilterFP: 80},
		}
		got := buildBenchmarkResultPerNode(before, after)
		if got.PageSplits != (5-1)+(7-2) {
			t.Errorf("PageSplits: got %d want %d", got.PageSplits, (5-1)+(7-2))
		}
		if got.BloomFilterFP != (50-10)+(80-20) {
			t.Errorf("BloomFilterFP: got %d want %d", got.BloomFilterFP, (50-10)+(80-20))
		}
	})
}

func TestParseKeyCacheHitRate(t *testing.T) {
	cases := map[string]struct {
		in   string
		want float64
	}{
		"hits-and-requests form": {
			in:   "Key Cache              : entries 100, size 1 MB, capacity 25 MB, 800 hits, 1000 requests, 0.8 recent hit rate",
			want: 0.8,
		},
		"recent-hit-rate fallback": {
			// No "X hits, Y requests" pair — only the rate.
			in:   "Key Cache: 0.42 recent hit rate",
			want: 0.42,
		},
		"NaN recent hit rate (zero requests)": {
			// Newly-started Cassandra; 0 hits / 0 requests → no rate.
			in:   "Key Cache        : entries 0, size 0 bytes, capacity 25 MB, 0 hits, 0 requests, NaN recent hit rate",
			want: 0,
		},
		"no key cache line at all": {
			in:   "Load: 1 KiB\nGeneration No: 0",
			want: 0,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got := parseKeyCacheHitRate(tc.in)
			if got != tc.want {
				// Allow a small float tolerance for the parsed cases.
				if got < tc.want-1e-9 || got > tc.want+1e-9 {
					t.Errorf("parseKeyCacheHitRate: got %v want %v", got, tc.want)
				}
			}
		})
	}
}

// fakeMetricsBackend is a minimal cluster.Backend for unit-testing the
// metrics package's per-node snapshot collection. Each ExecOnNode call
// invokes the per-node hook so tests can synchronise on concurrent entry
// (parallelism assertion) and script per-node errors (fail-loud assertion).
type fakeMetricsBackend struct {
	nodeCount int
	hook      func(i int, argv []string) (string, error)
	calls     []atomic.Int32
}

func newFakeMetricsBackend(nodeCount int, hook func(i int, argv []string) (string, error)) *fakeMetricsBackend {
	return &fakeMetricsBackend{
		nodeCount: nodeCount,
		hook:      hook,
		calls:     make([]atomic.Int32, nodeCount),
	}
}

func (f *fakeMetricsBackend) Start() error        { return nil }
func (f *fakeMetricsBackend) Stop() error         { return nil }
func (f *fakeMetricsBackend) WaitForReady() error { return nil }
func (f *fakeMetricsBackend) ExecOnNode(i int, argv ...string) (string, error) {
	f.calls[i].Add(1)
	return f.hook(i, argv)
}
func (f *fakeMetricsBackend) CopyToNode(i int, src, dst string) error { return nil }
func (f *fakeMetricsBackend) NodeAddresses() []string                 { return nil }
func (f *fakeMetricsBackend) NodeContainerIDs() ([]string, error)     { return nil, nil }
func (f *fakeMetricsBackend) NodeCount() int                          { return f.nodeCount }
func (f *fakeMetricsBackend) Mode() cluster.Mode                      { return cluster.ModeLocalCluster }

// minimal tablestats output the parser needs — single field is enough to
// produce a non-zero snapshot; tests assert against scripted distinct values.
func miniTableStats(sst int) string {
	return "Keyspace: uuid_benchmark\n\tTable: bench\n\tSSTable count: " +
		strconv.Itoa(sst) + "\n"
}

func TestCaptureAllNodesParallel(t *testing.T) {
	t.Parallel()
	// Pin the parallelism contract: with N nodes, captureAllNodes must
	// enter every per-node ExecOnNode concurrently. We detect this by
	// counting how many goroutines reach a shared rendezvous before any
	// of them is allowed to return. A serial implementation will only
	// ever have 1 goroutine waiting → fails the >= N check below.
	const n = 3
	enter := make(chan struct{}, n) // counts arrivals
	gate := make(chan struct{})     // released after all arrive

	hook := func(i int, argv []string) (string, error) {
		// Only the tablestats call participates in the rendezvous; the
		// nodetool info call is a no-op for this test (returns empty,
		// which captureNodeSnapshot tolerates).
		if len(argv) >= 2 && argv[0] == "nodetool" && argv[1] == "tablestats" {
			enter <- struct{}{}
			<-gate
			return miniTableStats(7), nil
		}
		return "", nil // nodetool info: returns empty, hit rate stays 0
	}

	b := newFakeMetricsBackend(n, hook)
	c := &CassandraBenchmarker{
		cfg:       cluster.ClusterConfig{Keyspace: "uuid_benchmark"},
		tableName: "bench",
	}

	// Wait for all N goroutines to enter, then release the gate. If the
	// implementation is serial only 1 goroutine ever waits — the test
	// times out at the "all arrived" check.
	done := make(chan error, 1)
	go func() {
		_, err := c.captureAllNodes(b)
		done <- err
	}()

	deadline := time.After(2 * time.Second)
	for i := 0; i < n; i++ {
		select {
		case <-enter:
		case <-deadline:
			t.Fatalf("only %d/%d nodes entered concurrently — captureAllNodes is serial", i, n)
		}
	}
	close(gate)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("captureAllNodes never returned after gate release")
	}
}

func TestCaptureAllNodesJoinsErrors(t *testing.T) {
	t.Parallel()
	// Pin the all-or-the-first fail-loud contract: when multiple nodes
	// error, captureAllNodes returns a joined error that mentions every
	// failing node (so the operator doesn't have to retry to discover
	// the second failure).
	const n = 3
	hook := func(i int, argv []string) (string, error) {
		if len(argv) >= 2 && argv[0] == "nodetool" && argv[1] == "tablestats" {
			// Nodes 0 and 2 fail; node 1 succeeds.
			if i == 1 {
				return miniTableStats(4), nil
			}
			return "", errors.New("scripted tablestats failure")
		}
		return "", nil
	}

	b := newFakeMetricsBackend(n, hook)
	c := &CassandraBenchmarker{
		cfg:       cluster.ClusterConfig{Keyspace: "uuid_benchmark"},
		tableName: "bench",
	}

	_, err := c.captureAllNodes(b)
	if err == nil {
		t.Fatal("expected error from captureAllNodes, got nil")
	}
	// errors.Join wraps both failures — both per-node error messages
	// must be visible to the operator.
	msg := err.Error()
	if !strings.Contains(msg, "node 0") {
		t.Errorf("expected joined error to mention node 0; got: %v", err)
	}
	if !strings.Contains(msg, "node 2") {
		t.Errorf("expected joined error to mention node 2; got: %v", err)
	}
}

func TestCaptureMetricsBeforeAllFailLoud(t *testing.T) {
	t.Parallel()
	// CaptureMetricsBeforeAll surfaces captureAllNodes errors directly
	// (no silent zeroing of metricsBeforeNodes). The runner relies on
	// this to fail the scenario rather than silently zero the deltas.
	hook := func(i int, argv []string) (string, error) {
		return "", errors.New("nodetool down")
	}
	b := newFakeMetricsBackend(1, hook)
	c := &CassandraBenchmarker{
		cfg:       cluster.ClusterConfig{Keyspace: "uuid_benchmark"},
		tableName: "bench",
	}
	if err := c.CaptureMetricsBeforeAll(b); err == nil {
		t.Fatal("expected error, got nil")
	}
	if c.metricsBeforeNodes != nil {
		t.Errorf("expected metricsBeforeNodes to stay nil on capture error; got %+v", c.metricsBeforeNodes)
	}
}
