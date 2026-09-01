package main

import (
	"slices"
	"testing"

	"github.com/moguls753/uuid-benchmark/internal/benchmark"
	"github.com/moguls753/uuid-benchmark/internal/benchmark/statistics"
)

// Without a campaign seed the order stays exactly as it was, so a rerun of a
// pre-2026-09 configuration reproduces the old execution sequence.
func TestExecutionOrderUnseededKeepsFixedOrder(t *testing.T) {
	campaignSeed = 0
	for run := 0; run < 5; run++ {
		got, _ := executionOrder("read_performance", run)
		if !slices.Equal(got, allKeyTypes) {
			t.Fatalf("run %d: got %v, want %v", run, got, allKeyTypes)
		}
	}
}

func TestExecutionOrderSeededPermutesPerRepetition(t *testing.T) {
	campaignSeed = 20260901
	t.Cleanup(func() { campaignSeed = 0 })

	seen := make(map[string]bool)
	var differsFromFixed bool
	for run := 0; run < 5; run++ {
		got, _ := executionOrder("read_performance", run)

		sorted := slices.Clone(got)
		want := slices.Clone(allKeyTypes)
		slices.Sort(sorted)
		slices.Sort(want)
		if !slices.Equal(sorted, want) {
			t.Fatalf("run %d is not a permutation of the key types: %v", run, got)
		}
		if !slices.Equal(got, allKeyTypes) {
			differsFromFixed = true
		}

		again, _ := executionOrder("read_performance", run)
		if !slices.Equal(got, again) {
			t.Fatalf("run %d is not reproducible: %v vs %v", run, got, again)
		}
		seen[joined(got)] = true
	}

	if !differsFromFixed {
		t.Fatal("seeded order never differed from the fixed order")
	}
	if len(seen) < 2 {
		t.Fatalf("expected different orders across repetitions, got %d distinct", len(seen))
	}
}

// The caller mutates nothing, but a shared backing array would make one
// repetition's shuffle reorder allKeyTypes for every later one.
func TestExecutionOrderDoesNotAliasKeyTypes(t *testing.T) {
	campaignSeed = 20260901
	t.Cleanup(func() { campaignSeed = 0 })

	before := slices.Clone(allKeyTypes)
	for run := 0; run < 10; run++ {
		_, _ = executionOrder("read_performance", run)
	}
	if !slices.Equal(allKeyTypes, before) {
		t.Fatalf("allKeyTypes was mutated: %v, want %v", allKeyTypes, before)
	}
}

func TestDeriveSeedDistinguishesCoordinates(t *testing.T) {
	campaignSeed = 20260901
	t.Cleanup(func() { campaignSeed = 0 })

	base := deriveSeed("sample", "read_performance", "uuidv4", "0")
	if base < 0 {
		t.Fatalf("seed must be non-negative for rand.NewPCG, got %d", base)
	}
	if base != deriveSeed("sample", "read_performance", "uuidv4", "0") {
		t.Fatal("seed is not reproducible")
	}

	for _, other := range [][]string{
		{"sample", "read_performance", "uuidv4", "1"},
		{"sample", "read_performance", "uuidv7", "0"},
		{"sample", "update_performance", "uuidv4", "0"},
		{"order", "read_performance", "uuidv4", "0"},
	} {
		if deriveSeed(other...) == base {
			t.Errorf("seed collision between base and %v", other)
		}
	}

	campaignSeed = 1
	if deriveSeed("sample", "read_performance", "uuidv4", "0") == base {
		t.Error("campaign seed does not influence derived seeds")
	}
}

// Field separation matters: without it ("a","bc") and ("ab","c") would hash
// alike and two different runs could share a sample.
func TestDeriveSeedSeparatesFields(t *testing.T) {
	campaignSeed = 0
	if deriveSeed("a", "bc") == deriveSeed("ab", "c") {
		t.Fatal("adjacent fields are not separated")
	}
}

func joined(parts []string) string {
	out := ""
	for _, p := range parts {
		out += p + "|"
	}
	return out
}

// The order seed is recorded per run even when randomisation is off, so a
// campaign's manifest documents which permutation was in force rather than
// leaving the reader to infer it from the key-type sequence.
func TestExecutionOrderAlwaysReturnsItsSeed(t *testing.T) {
	campaignSeed = 0
	_, unseeded := executionOrder("read_performance", 0)
	if unseeded == 0 {
		t.Fatal("expected a derived order seed even with no campaign seed")
	}

	campaignSeed = 20260901
	t.Cleanup(func() { campaignSeed = 0 })
	_, seeded := executionOrder("read_performance", 0)
	if seeded == unseeded {
		t.Fatal("campaign seed does not influence the order seed")
	}
	if _, again := executionOrder("read_performance", 0); again != seeded {
		t.Fatal("order seed is not reproducible")
	}
}

// A failed cgroup capture leaves the I/O columns at zero, which for read_iops
// is the most favourable value there is. The exported flag is what separates
// that from a genuine zero.
func TestIOValidMetricEncodesCaptureFailures(t *testing.T) {
	stats := ioValidMetric([]bool{true, true, false})
	if stats.Min != 0 || stats.Max != 1 {
		t.Fatalf("expected the failed capture to show as 0 and the others as 1, got min %v max %v", stats.Min, stats.Max)
	}
	if all := ioValidMetric([]bool{true, true}); all.Min != 1 {
		t.Fatalf("expected all-valid to be 1, got %v", all.Min)
	}
}

func TestReadSetOfExtractsFingerprint(t *testing.T) {
	read := &benchmark.ReadPerformanceResult{IDFileSHA256: "abc"}
	if got := readSetOf(read); got != "abc" {
		t.Fatalf("read result: got %q, want abc", got)
	}
	update := &benchmark.UpdatePerformanceResult{IDFileSHA256: "def"}
	if got := readSetOf(update); got != "def" {
		t.Fatalf("update result: got %q, want def", got)
	}
	if got := readSetOf(&benchmark.InsertPerformanceResult{}); got != "" {
		t.Fatalf("insert result has no read set, got %q", got)
	}
}

// The gate has to agree with the analysis protocol's rules, otherwise a run
// aborted here and a run discarded later would disagree about what counts as
// broken.
func TestInvalidReasonMatchesTheProtocolRules(t *testing.T) {
	one := func(v float64) statistics.Stats { return statistics.Calculate([]float64{v}) }

	healthy := map[string]statistics.Stats{
		"attempted": one(100000), "failed": one(0), "not_found": one(0),
		"insert_failed": one(0), "io_valid": one(1),
	}
	if reason := invalidReason(healthy, 50_000_000); reason != "" {
		t.Fatalf("healthy run rejected: %s", reason)
	}

	for name, metrics := range map[string]map[string]statistics.Stats{
		"bootstrap lost rows": {"attempted": one(100000), "insert_failed": one(60000), "io_valid": one(1)},
		"error rate at 0.1 %": {"attempted": one(100000), "failed": one(60), "not_found": one(40), "insert_failed": one(0), "io_valid": one(1)},
	} {
		if invalidReason(metrics, 50_000_000) == "" {
			t.Errorf("%s: expected the run to be rejected", name)
		}
	}

	// A failed I/O capture voids only the I/O endpoint, so the run itself is
	// still valid and must not count towards the abort.
	ioFailed := map[string]statistics.Stats{
		"attempted": one(100000), "failed": one(0), "not_found": one(0),
		"insert_failed": one(0), "io_valid": one(0),
	}
	if reason := invalidReason(ioFailed, 50_000_000); reason != "" {
		t.Fatalf("a failed I/O capture must not invalidate the run: %s", reason)
	}

	// Just under the threshold stays valid: the rule is >= 0.1 %.
	borderline := map[string]statistics.Stats{
		"attempted": one(100000), "failed": one(99), "not_found": one(0),
		"insert_failed": one(0), "io_valid": one(1),
	}
	if reason := invalidReason(borderline, 50_000_000); reason != "" {
		t.Fatalf("99 of 100000 is below the threshold, got: %s", reason)
	}

	// Scenarios that export no counters at all must not be judged.
	if reason := invalidReason(map[string]statistics.Stats{"throughput": one(42)}, 50_000_000); reason != "" {
		t.Fatalf("uncounted scenario rejected: %s", reason)
	}
}

// The documented A3 invocation passes -single-node without repeating the
// replication factor. A one-host cluster cannot replicate three ways, so the
// default has to follow the acknowledgement.
func TestBuildClusterConfigSingleNodeDefaultsToRFOne(t *testing.T) {
	cfg, err := buildClusterConfig("remote-cluster", "taurus2", "someone", "", "local_one", 0, 1000, "2G", "512M", "8", "4g", true, "")
	if err != nil {
		t.Fatalf("documented single-node invocation rejected: %v", err)
	}
	if cfg.ReplicationFactor != 1 {
		t.Errorf("ReplicationFactor = %d, want 1", cfg.ReplicationFactor)
	}
	if !cfg.SingleNode {
		t.Error("SingleNode not carried into the config")
	}

	// Three hosts keep the cluster default even with the flag set.
	three, err := buildClusterConfig("remote-cluster", "a,b,c", "someone", "", "local_one", 0, 1000, "2G", "512M", "8", "4g", true, "")
	if err != nil {
		t.Fatalf("three-host config rejected: %v", err)
	}
	if three.ReplicationFactor != 3 {
		t.Errorf("three hosts: ReplicationFactor = %d, want 3", three.ReplicationFactor)
	}

	// An explicit value always wins.
	explicit, err := buildClusterConfig("remote-cluster", "taurus2", "someone", "", "local_one", 1, 1000, "2G", "512M", "8", "4g", true, "")
	if err != nil {
		t.Fatalf("explicit RF rejected: %v", err)
	}
	if explicit.ReplicationFactor != 1 {
		t.Errorf("explicit RF = %d, want 1", explicit.ReplicationFactor)
	}
}

// The dataset is 50M rows. A few thousand missing changes nothing a read
// measurement can see, and treating any loss as fatal would invalidate
// physically sound runs and abort the campaign after two of them.
func TestInvalidReasonToleratesSmallBootstrapLosses(t *testing.T) {
	one := func(v float64) statistics.Stats { return statistics.Calculate([]float64{v}) }
	with := func(lost float64) map[string]statistics.Stats {
		return map[string]statistics.Stats{
			"attempted": one(100000), "failed": one(0), "not_found": one(0),
			"insert_failed": one(lost), "io_valid": one(1),
		}
	}

	// What the first campaign run actually lost: 3000 of 50M, 0.006 %.
	if reason := invalidReason(with(3000), 50_000_000); reason != "" {
		t.Fatalf("3000 of 50M must stay valid, got: %s", reason)
	}
	// Just under the 0.1 % limit.
	if reason := invalidReason(with(49_999), 50_000_000); reason != "" {
		t.Fatalf("49999 of 50M is below the limit, got: %s", reason)
	}
	// At the limit.
	if invalidReason(with(50_000), 50_000_000) == "" {
		t.Fatal("50000 of 50M reaches 0.1 % and must be rejected")
	}
	// Without a record count the rule cannot fire; the operation-level rules
	// still apply.
	if reason := invalidReason(with(3000), 0); reason != "" {
		t.Fatalf("no record count means no rate rule, got: %s", reason)
	}
}
