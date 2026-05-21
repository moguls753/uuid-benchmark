package docker

import (
	"strings"
	"testing"
	"time"
)

func TestParseIOStatContent(t *testing.T) {
	t.Parallel()
	t.Run("multi-device sums correctly", func(t *testing.T) {
		t.Parallel()
		input := "8:0 rbytes=12345 wbytes=67890 rios=10 wios=20\n253:0 rbytes=100 wbytes=200 rios=1 wios=2\n"
		got, err := parseIOStatContent(input)
		if err != nil {
			t.Fatal(err)
		}
		if got.ReadBytes != 12445 {
			t.Errorf("ReadBytes: got %d want 12445", got.ReadBytes)
		}
		if got.WriteBytes != 68090 {
			t.Errorf("WriteBytes: got %d want 68090", got.WriteBytes)
		}
		if got.ReadOps != 11 {
			t.Errorf("ReadOps: got %d want 11", got.ReadOps)
		}
		if got.WriteOps != 22 {
			t.Errorf("WriteOps: got %d want 22", got.WriteOps)
		}
	})
	t.Run("malformed lines skipped", func(t *testing.T) {
		t.Parallel()
		input := "garbage line\n8:0 rbytes=100\n"
		got, err := parseIOStatContent(input)
		if err != nil {
			t.Fatal(err)
		}
		if got.ReadBytes != 100 {
			t.Errorf("ReadBytes: got %d want 100", got.ReadBytes)
		}
	})
	t.Run("empty content returns zero stats", func(t *testing.T) {
		t.Parallel()
		got, err := parseIOStatContent("")
		if err != nil {
			t.Fatal(err)
		}
		if got.ReadBytes != 0 || got.WriteBytes != 0 || got.ReadOps != 0 || got.WriteOps != 0 {
			t.Errorf("expected zero stats, got %+v", got)
		}
		if got.Timestamp.IsZero() {
			t.Error("Timestamp should be set even for empty input")
		}
	})
	t.Run("unknown keys are silently ignored", func(t *testing.T) {
		t.Parallel()
		// Future cgroup v2 versions may add fields we don't care about.
		// The parser must not error or affect the known totals.
		input := "8:0 rbytes=10 unknown=999 wbytes=20\n"
		got, err := parseIOStatContent(input)
		if err != nil {
			t.Fatal(err)
		}
		if got.ReadBytes != 10 || got.WriteBytes != 20 {
			t.Errorf("got %+v, want ReadBytes=10 WriteBytes=20", got)
		}
	})
	t.Run("malformed numeric values are skipped", func(t *testing.T) {
		t.Parallel()
		input := "8:0 rbytes=notanumber wbytes=42\n"
		got, err := parseIOStatContent(input)
		if err != nil {
			t.Fatal(err)
		}
		if got.ReadBytes != 0 || got.WriteBytes != 42 {
			t.Errorf("got %+v, want ReadBytes=0 (malformed) WriteBytes=42", got)
		}
	})
}

func TestGetClusterIOStatsEmpty(t *testing.T) {
	t.Parallel()
	got, err := GetClusterIOStats(nil, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got == nil {
		t.Fatal("expected non-nil zero stats for empty refs")
	}
	if got.ReadBytes != 0 || got.WriteBytes != 0 || got.ReadOps != 0 || got.WriteOps != 0 {
		t.Errorf("empty cluster expected zero stats, got %+v", got)
	}
	if got.Timestamp.IsZero() {
		t.Error("Timestamp should be set even for empty refs")
	}
}

func TestGetClusterIOStatsRejectsEmptyContainerID(t *testing.T) {
	t.Parallel()
	// Defense in depth against [[project-io-metric-empty-container-id-bug]].
	// NodeContainerIDs() is supposed to reject empty IDs, but if a future
	// caller bypasses it we want a clean error here — not a vague
	// "no cgroup io.stat at docker-.scope" message followed by a uint64
	// underflow downstream in CalculateIOMetrics.
	cases := []struct {
		name string
		id   string
	}{
		{"empty string", ""},
		{"whitespace only", "   \t\n  "},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			refs := []NodeRef{{Host: "remote.example", ContainerID: tc.id}}
			_, err := GetClusterIOStats(refs, "user", "")
			if err == nil {
				t.Fatal("expected error for empty container ID")
			}
			if !strings.Contains(err.Error(), "empty container ID") {
				t.Errorf("expected 'empty container ID' in error, got: %v", err)
			}
		})
	}
}

func TestCalculateIOMetricsClampsUnderflow(t *testing.T) {
	t.Parallel()
	// Regression test for [[project-io-metric-empty-container-id-bug]]:
	// previously, end<start produced a uint64 underflow that surfaced
	// as ~8e18 values in the comparison table. The clamp must zero
	// these out so the operator sees "no data" rather than nonsense.
	now := time.Now()
	start := &IOStats{
		ReadBytes:  1_000_000,
		WriteBytes: 2_000_000,
		ReadOps:    100,
		WriteOps:   200,
		Timestamp:  now,
	}
	end := &IOStats{
		// Every counter is SMALLER than start — the underflow scenario.
		ReadBytes:  10,
		WriteBytes: 20,
		ReadOps:    1,
		WriteOps:   2,
		Timestamp:  now.Add(5 * time.Second),
	}
	got := CalculateIOMetrics(start, end)
	if got.ReadIOPS != 0 || got.WriteIOPS != 0 ||
		got.ReadThroughputMB != 0 || got.WriteThroughputMB != 0 {
		t.Errorf("expected all-zero metrics on underflow, got %+v", got)
	}
}

func TestCalculateIOMetricsClampsPartialUnderflow(t *testing.T) {
	t.Parallel()
	// More realistic regression of the I/O metric bug: only SOME fields
	// underflow (e.g. a truncated SSH-cat read populated rbytes/wbytes
	// but not the ops fields, leaving them at 0 while start is non-zero).
	// The clamp must zero out only the regressed fields, leaving valid
	// ones intact.
	now := time.Now()
	start := &IOStats{
		ReadBytes:  100,
		WriteBytes: 1024 * 1024,
		ReadOps:    50, // <- start has 50, end has 0 → regresses
		WriteOps:   100,
		Timestamp:  now,
	}
	end := &IOStats{
		ReadBytes:  200,                     // delta 100 (valid)
		WriteBytes: 3 * 1024 * 1024,         // delta 2 MiB (valid)
		ReadOps:    0,                       // regression
		WriteOps:   300,                     // delta 200 (valid)
		Timestamp:  now.Add(2 * time.Second),
	}
	got := CalculateIOMetrics(start, end)
	if got.ReadIOPS != 0 {
		t.Errorf("ReadIOPS: got %v want 0 (clamped)", got.ReadIOPS)
	}
	if got.WriteIOPS != 100 {
		t.Errorf("WriteIOPS: got %v want 100", got.WriteIOPS)
	}
	if got.ReadThroughputMB == 0 {
		t.Errorf("ReadThroughputMB: got 0, want non-zero (valid delta)")
	}
	if got.WriteThroughputMB != 1 {
		t.Errorf("WriteThroughputMB: got %v want 1", got.WriteThroughputMB)
	}
}

func TestCalculateIOMetricsHappyPath(t *testing.T) {
	t.Parallel()
	now := time.Now()
	start := &IOStats{
		ReadBytes:  1024 * 1024,         // 1 MiB
		WriteBytes: 2 * 1024 * 1024,     // 2 MiB
		ReadOps:    100,
		WriteOps:   200,
		Timestamp:  now,
	}
	end := &IOStats{
		ReadBytes:  3 * 1024 * 1024,     // delta 2 MiB
		WriteBytes: 6 * 1024 * 1024,     // delta 4 MiB
		ReadOps:    300,                 // delta 200
		WriteOps:   600,                 // delta 400
		Timestamp:  now.Add(2 * time.Second),
	}
	got := CalculateIOMetrics(start, end)
	if got.ReadIOPS != 100 {
		t.Errorf("ReadIOPS: got %v want 100", got.ReadIOPS)
	}
	if got.WriteIOPS != 200 {
		t.Errorf("WriteIOPS: got %v want 200", got.WriteIOPS)
	}
	if got.ReadThroughputMB != 1 {
		t.Errorf("ReadThroughputMB: got %v want 1", got.ReadThroughputMB)
	}
	if got.WriteThroughputMB != 2 {
		t.Errorf("WriteThroughputMB: got %v want 2", got.WriteThroughputMB)
	}
}

func TestNodeRefHostClassification(t *testing.T) {
	t.Parallel()
	// Verify the documented convention via observable behavior: empty
	// and "localhost" hosts route to the local path. This test exercises
	// the classification logic without needing real cgroup files — the
	// local path will fail with a "no cgroup io.stat found" error
	// because the container ID is fake, but the SSH path would fail
	// with a different error (no host reachable). We use error message
	// substring matching to distinguish.
	refs := []NodeRef{{Host: "", ContainerID: "deadbeef"}}
	_, err := GetClusterIOStats(refs, "", "")
	if err == nil {
		t.Fatal("expected error for fake container ID")
	}
	// Asserting against the package-level errNoCgroupLocal constant
	// keeps the local-vs-remote dispatch contract grep-discoverable —
	// any future rephrasing of the error must update both sites.
	if !strings.Contains(err.Error(), errNoCgroupLocal) {
		t.Errorf("expected local-path error containing %q, got: %v", errNoCgroupLocal, err)
	}
}
