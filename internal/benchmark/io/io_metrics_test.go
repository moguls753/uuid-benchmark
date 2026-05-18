package docker

import (
	"strings"
	"testing"
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
