package cassandra

import (
	"testing"
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
			SSTableCount:         4,
			SpaceUsedLive:        100 * 1024 * 1024,
			SpaceUsedTotal:       120 * 1024 * 1024,
			MemtableSwitchCount:  7,
			BloomFilterFP:        42,
			BloomFilterFPRatio:   0.001,
			KeyCacheHitRate:      0.80,
			CompactionBytesTotal: 1_000_000,
		}
		b := &CassandraMetricsSnapshot{
			SSTableCount:         6,
			SpaceUsedLive:        200 * 1024 * 1024,
			SpaceUsedTotal:       240 * 1024 * 1024,
			MemtableSwitchCount:  9,
			BloomFilterFP:        8,
			BloomFilterFPRatio:   0.003,
			KeyCacheHitRate:      0.90,
			CompactionBytesTotal: 500_000,
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
		if got.CompactionBytesTotal != 1_500_000 {
			t.Errorf("CompactionBytesTotal: got %d want 1500000", got.CompactionBytesTotal)
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

func TestBuildBenchmarkResult(t *testing.T) {
	t.Parallel()
	t.Run("delta + ratio assembly with before snapshot", func(t *testing.T) {
		t.Parallel()
		before := &CassandraMetricsSnapshot{
			SSTableCount:  4,
			BloomFilterFP: 10,
		}
		after := &CassandraMetricsSnapshot{
			SSTableCount:    7,
			SpaceUsedLive:   200 * 1024 * 1024,
			SpaceUsedTotal:  250 * 1024 * 1024,
			BloomFilterFP:   25,
			KeyCacheHitRate: 0.91,
		}
		got := buildBenchmarkResult(before, after)
		if got.TableSize != after.SpaceUsedLive {
			t.Errorf("TableSize: got %d want %d", got.TableSize, after.SpaceUsedLive)
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
		if got.BufferHitRatio != after.KeyCacheHitRate {
			t.Errorf("BufferHitRatio: got %v want %v", got.BufferHitRatio, after.KeyCacheHitRate)
		}
		if got.IndexBufferHitRatio != got.BufferHitRatio {
			t.Errorf("IndexBufferHitRatio: got %v want %v (equal to BufferHitRatio)", got.IndexBufferHitRatio, got.BufferHitRatio)
		}
		// Fragmentation% = (total - live) / total * 100 = 50 / 250 * 100 = 20%.
		if got.Fragmentation.FragmentationPercent < 19.9 || got.Fragmentation.FragmentationPercent > 20.1 {
			t.Errorf("FragmentationPercent: got %v want ~20", got.Fragmentation.FragmentationPercent)
		}
		if got.Fragmentation.LeafPages != int64(after.SSTableCount) {
			t.Errorf("LeafPages (repurposed for SSTable count): got %d want %d", got.Fragmentation.LeafPages, after.SSTableCount)
		}
	})
	t.Run("nil before snapshot leaves delta fields zero", func(t *testing.T) {
		t.Parallel()
		after := &CassandraMetricsSnapshot{
			SSTableCount:  3,
			SpaceUsedLive: 100,
			BloomFilterFP: 7,
		}
		got := buildBenchmarkResult(nil, after)
		if got.PageSplits != 0 {
			t.Errorf("PageSplits: got %d want 0 (no before)", got.PageSplits)
		}
		if got.BloomFilterFP != 0 {
			t.Errorf("BloomFilterFP: got %d want 0 (no before)", got.BloomFilterFP)
		}
		if got.TableSize != after.SpaceUsedLive {
			t.Errorf("TableSize: got %d want %d", got.TableSize, after.SpaceUsedLive)
		}
	})
	t.Run("negative delta is clamped to zero", func(t *testing.T) {
		t.Parallel()
		// Cassandra counters can legitimately decrease (e.g. compaction
		// merges SSTables, lowering the count). buildBenchmarkResult
		// reports 0 instead of a negative delta — a benchmark "page
		// splits" or "bloom FPs added" can't be negative.
		before := &CassandraMetricsSnapshot{SSTableCount: 10, BloomFilterFP: 100}
		after := &CassandraMetricsSnapshot{SSTableCount: 4, BloomFilterFP: 50}
		got := buildBenchmarkResult(before, after)
		if got.PageSplits != 0 {
			t.Errorf("PageSplits: got %d want 0 (negative delta clamped)", got.PageSplits)
		}
		if got.BloomFilterFP != 0 {
			t.Errorf("BloomFilterFP: got %d want 0 (negative delta clamped)", got.BloomFilterFP)
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
