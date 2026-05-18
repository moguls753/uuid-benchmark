package workload

import (
	"strings"
	"testing"
)

func TestBuildExecArgs(t *testing.T) {
	t.Run("insert with numeric flags", func(t *testing.T) {
		cfg := ExecutorConfig{
			DBType:           "cassandra",
			Op:               "insert",
			KeyType:          "uuidv7",
			NumRecords:       1000,
			BatchSize:        100,
			Threads:          4,
			ConnectionString: "taurus5,taurus6,taurus7",
		}
		got := buildExecArgs(cfg)
		joined := strings.Join(got, " ")
		for _, want := range []string{
			"--db-type cassandra",
			"--op insert",
			"--key-type uuidv7",
			"--num-records 1000",
			"--batch-size 100",
			"--threads 4",
			"--connection-string taurus5,taurus6,taurus7",
		} {
			if !strings.Contains(joined, want) {
				t.Errorf("missing %q in args: %s", want, joined)
			}
		}
	})

	t.Run("mixed op emits all conditional flags", func(t *testing.T) {
		// Pins the conditional branches in buildExecArgs that the simple
		// insert case doesn't exercise: NumOps, NumBuckets, TableName, and
		// the mixed-op pct triplet.
		cfg := ExecutorConfig{
			DBType:           "cassandra",
			Op:               "mixed",
			KeyType:          "uuidv4",
			NumOps:           5000,
			Threads:          8,
			ConnectionString: "127.0.0.1",
			InsertPct:        70,
			ReadPct:          20,
			UpdatePct:        10,
			TableName:        "bench",
			NumBuckets:       1000,
		}
		got := buildExecArgs(cfg)
		joined := strings.Join(got, " ")
		for _, want := range []string{
			"--num-ops 5000",
			"--table-name bench",
			"--num-buckets 1000",
			"--insert-pct 70",
			"--read-pct 20",
			"--update-pct 10",
		} {
			if !strings.Contains(joined, want) {
				t.Errorf("missing %q in args: %s", want, joined)
			}
		}
	})

	t.Run("emits consistency when set", func(t *testing.T) {
		// IMP-1: the workload binary's Cassandra session honors --consistency;
		// the orchestrator must forward cfg.Consistency through buildExecArgs
		// or 3-node LOCAL_QUORUM runs silently degrade to LOCAL_ONE.
		cfg := ExecutorConfig{
			DBType:           "cassandra",
			Op:               "insert",
			KeyType:          "uuidv7",
			ConnectionString: "127.0.0.1",
			Consistency:      "local_quorum",
		}
		got := buildExecArgs(cfg)
		joined := strings.Join(got, " ")
		if !strings.Contains(joined, "--consistency local_quorum") {
			t.Errorf("missing --consistency local_quorum in args: %s", joined)
		}
	})

	t.Run("omits consistency when empty", func(t *testing.T) {
		// Empty Consistency means "fall back to the workload binary's own
		// --consistency default", so the orchestrator should not emit the flag.
		cfg := ExecutorConfig{
			DBType:           "cassandra",
			Op:               "insert",
			KeyType:          "uuidv7",
			ConnectionString: "127.0.0.1",
		}
		got := buildExecArgs(cfg)
		joined := strings.Join(got, " ")
		if strings.Contains(joined, "--consistency") {
			t.Errorf("unexpected --consistency in args: %s", joined)
		}
	})

	t.Run("non-mixed op omits pct flags", func(t *testing.T) {
		// Even with non-zero percentages set, a non-"mixed" op must not
		// emit them — keeps insert/read/update args clean.
		cfg := ExecutorConfig{
			DBType:    "cassandra",
			Op:        "insert",
			KeyType:   "uuidv7",
			InsertPct: 50,
			ReadPct:   50,
		}
		got := buildExecArgs(cfg)
		joined := strings.Join(got, " ")
		for _, unwanted := range []string{"--insert-pct", "--read-pct", "--update-pct"} {
			if strings.Contains(joined, unwanted) {
				t.Errorf("unexpected %q in args: %s", unwanted, joined)
			}
		}
	})
}

func TestExecuteRejectsUnknownMode(t *testing.T) {
	_, err := Execute(ExecutorConfig{Mode: "bogus", DBType: "cassandra", Op: "insert", KeyType: "uuidv4"})
	if err == nil {
		t.Fatal("expected error for unknown mode")
	}
}

func TestExecuteContainerRequiresContainerName(t *testing.T) {
	_, err := Execute(ExecutorConfig{Mode: ExecutionModeContainer, DBType: "cassandra", Op: "insert", KeyType: "uuidv4"})
	if err == nil {
		t.Fatal("expected error when container mode has no ContainerName")
	}
}

// Pins the empty-Mode → ExecutionModeContainer default so a future refactor
// can't silently flip the implicit mode for existing callers (none of which
// set Mode today).
func TestExecuteEmptyModeDefaultsToContainer(t *testing.T) {
	_, err := Execute(ExecutorConfig{DBType: "cassandra", Op: "insert", KeyType: "uuidv4"})
	if err == nil {
		t.Fatal("expected error: empty Mode should default to container, which then rejects empty ContainerName")
	}
	if !strings.Contains(err.Error(), "container mode") {
		t.Errorf("expected container-mode error, got: %v", err)
	}
}

// Pins the native-mode behavior when no binary path can be resolved: the
// executor must error out explicitly rather than silently shell out to
// "./workload" (the prior fallback), which would surface as a confusing
// "executable file not found" from exec.Command if a future caller forgot
// to call BuildBinary() or set BinaryPath.
func TestExecuteNativeRequiresBinaryPath(t *testing.T) {
	// Save and restore the package-level binaryPath so this test is
	// independent of whether some earlier test (or future BuildBinary call)
	// populated it.
	saved := binaryPath
	binaryPath = ""
	t.Cleanup(func() { binaryPath = saved })

	_, err := Execute(ExecutorConfig{
		Mode:    ExecutionModeNative,
		DBType:  "cassandra",
		Op:      "insert",
		KeyType: "uuidv4",
	})
	if err == nil {
		t.Fatal("expected error: native mode with no BinaryPath and unbuilt package binary should fail explicitly")
	}
	if !strings.Contains(err.Error(), "native mode requires BinaryPath") {
		t.Errorf("expected explicit native-mode error, got: %v", err)
	}
}
