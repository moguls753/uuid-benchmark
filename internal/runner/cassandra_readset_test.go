package runner

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/workload"
	"github.com/moguls753/uuid-benchmark/internal/cluster"
)

func TestReadSetForDefaultDrawsDuringInsert(t *testing.T) {
	path, sampleSize, err := readSetFor(cluster.ClusterConfig{}, workload.ExecutionModeNative, 1000, 100000)
	if err != nil {
		t.Fatalf("readSetFor: %v", err)
	}
	if path == "" {
		t.Fatal("expected an id file so the measured phase issues no fetch")
	}
	if sampleSize != 1000 {
		t.Fatalf("sample size %d, want 1000 (one target per operation)", sampleSize)
	}
}

// The bridge arm isolates the sampler: no id file, so the workload binary falls
// back to the partition-head fetch. It is not a replication of the June runs,
// which also used a single-writer bootstrap; both arms of this campaign share
// the eight-writer one, which is what makes the sampler the only difference
// between them.
func TestReadSetForHeadSamplingSkipsIDFile(t *testing.T) {
	path, sampleSize, err := readSetFor(cluster.ClusterConfig{HeadSampling: true}, workload.ExecutionModeNative, 1000, 100000)
	if err != nil {
		t.Fatalf("readSetFor: %v", err)
	}
	if path != "" || sampleSize != 0 {
		t.Fatalf("expected no read set, got path %q size %d", path, sampleSize)
	}
}

// In container mode the file lives inside the Cassandra container, which is
// recreated per run; the orchestrator must not look for it on its own disk.
func TestReadSetPathContainerModeIsContainerLocal(t *testing.T) {
	path, err := readSetPath(workload.ExecutionModeContainer)
	if err != nil {
		t.Fatalf("readSetPath: %v", err)
	}
	if path != "/tmp/"+readSetFile {
		t.Fatalf("got %q, want /tmp/%s", path, readSetFile)
	}
}

// A leftover file from an aborted run of the same key type would pass
// loadIDFile's key-type check, so the orchestrator clears it up front.
func TestReadSetPathNativeModeClearsStaleFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("TMPDIR", dir)

	stale := filepath.Join(dir, readSetFile)
	if err := os.WriteFile(stale, []byte("sequential\n1\n"), 0o644); err != nil {
		t.Fatalf("seed stale file: %v", err)
	}

	path, err := readSetPath(workload.ExecutionModeNative)
	if err != nil {
		t.Fatalf("readSetPath: %v", err)
	}
	if path != stale {
		t.Fatalf("got %q, want %q", path, stale)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale read set survived: %v", err)
	}
}

// One target per operation means the target set has to cover distinct rows.
// Reusing rows would measure the cache, not the storage engine, and a modulo
// over a short set would hide it.
func TestReadSetForRejectsMoreOpsThanRows(t *testing.T) {
	_, _, err := readSetFor(cluster.ClusterConfig{}, workload.ExecutionModeNative, 200000, 100000)
	if err == nil {
		t.Fatal("expected num-ops above num-records to be rejected")
	}
}

// The digest comparison is the last line of defence against a measured phase
// that ran on different rows than the ones the insert drew.
func TestSameReadSet(t *testing.T) {
	wrote := &workload.WorkloadResult{IDFileSHA256: "abc"}

	if err := sameReadSet("", wrote, &workload.WorkloadResult{}); err != nil {
		t.Fatalf("head sampling has no file and must pass: %v", err)
	}
	if err := sameReadSet("/tmp/x", wrote, &workload.WorkloadResult{IDFileSHA256: "abc"}); err != nil {
		t.Fatalf("matching digests rejected: %v", err)
	}
	if err := sameReadSet("/tmp/x", wrote, &workload.WorkloadResult{IDFileSHA256: "def"}); err == nil {
		t.Error("a changed read set must be rejected")
	}
	// An empty digest means one side never reported one, which is a plumbing
	// failure and must not pass as agreement.
	if err := sameReadSet("/tmp/x", wrote, &workload.WorkloadResult{}); err == nil {
		t.Error("a missing reader digest must be rejected")
	}
	if err := sameReadSet("/tmp/x", &workload.WorkloadResult{}, &workload.WorkloadResult{IDFileSHA256: "abc"}); err == nil {
		t.Error("a missing writer digest must be rejected")
	}
}

// Throughput counts attempts, so the exported success rate has to be derived
// from what the binary actually reported, not from what was requested.
func TestCountsFrom(t *testing.T) {
	got := countsFrom(&workload.WorkloadResult{TotalOps: 1000, Errors: 7, NotFound: 3})
	want := benchmark.OperationCounts{Attempted: 1000, Succeeded: 990, Failed: 7, NotFound: 3}
	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	// A short run must show as short rather than being padded to the request.
	short := countsFrom(&workload.WorkloadResult{TotalOps: 400, Errors: 0, NotFound: 0})
	if short.Attempted != 400 || short.Succeeded != 400 {
		t.Fatalf("short run: got %+v", short)
	}
}
