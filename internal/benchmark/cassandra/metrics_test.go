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
