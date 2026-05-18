# Multi-Node Cassandra Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.
>
> **User preference override:** This project's owner handles all git operations themselves. **Do NOT run `git add`, `git commit`, or `git push`.** Steps that say "pause for review" mean: stop, summarize the change, and wait for the user to commit before continuing to the next task.

**Goal:** Extend the UUID benchmark tool to run Cassandra workloads against a real 3-node cluster on the FernUni Taurus servers, while keeping the existing single-node mode fully functional for reproducibility.

**Architecture:** Introduce a pluggable cluster backend so the runner can target either (a) the existing local single-node container, (b) a local 3-container compose cluster (laptop testing), or (c) a 3-machine remote cluster managed via SSH from `taurus4`. Distribute data across the ring by keeping the thesis's `PRIMARY KEY ((bucket), id)` schema unchanged but spreading the existing `bucket` partition key across N values (default 1000) via `bucket = FNV-1a(id_bytes) mod N`, replacing the thesis's `bucket=1` constant. This preserves the UUID-as-clustering-column dynamics measured in the thesis while enabling Cassandra's native Murmur3-based distribution across the ring. The Go workload binary runs natively on the orchestrator machine and connects to the cluster over CQL on port 9042 — no in-container execution for multi-node. See "Schema Design Methodology" below for the full rationale.

**Tech Stack:**
- Go 1.21+
- `github.com/gocql/gocql` v1.7.0 (Cassandra driver, already in go.mod)
- `golang.org/x/crypto/ssh` (new dependency, for remote node management)
- `cassandra:5` Docker image
- Standard Go `testing` package + `t.Run` subtests (project convention)

**Out of scope:**
- PostgreSQL / MySQL / MongoDB multi-node — Cassandra only
- Kubernetes / Swarm / managed cluster orchestration — SSH + `docker run`
- 100M-record dataset validation — that's a separate operational concern handled after this code lands
- New metrics — only existing metrics are extended to multi-node aggregation

---

## Schema Design Methodology

The bachelor thesis used a single-partition Cassandra schema (`PRIMARY KEY ((bucket), id)` with `bucket=1` constant) to isolate the effect of UUID byte ordering on storage engine internals. With every row in one partition and the UUID as a clustering column, MemTable/SSTable layout, compaction, and bloom-filter behavior were driven directly by the UUID's byte order — exactly the controlled experiment a single-node UUID benchmark needs.

For the multi-node extension, the schema choice is constrained by three options:

1. **Keep `bucket=1`:** does not distribute. All primary data lives on the node owning the token for `bucket=1`; other nodes hold replicas only. The cluster behaves as one node, defeating the paper's purpose.

2. **`id PRIMARY KEY` (UUID as partition key):** distributes well, but Cassandra's Murmur3 partitioner hashes the partition key before placement. The UUID's byte ordering is destroyed before any storage-engine behavior reacts to it. UUIDv4 and UUIDv7 become indistinguishable — the variable we set out to measure no longer affects what we measure.

3. **Bucketed schema with `bucket = hash(id) mod N`:** keeps the thesis's schema mechanics intact (`PRIMARY KEY ((bucket), id)`, UUID as clustering column) but spreads `bucket` across N values so the partition key — and therefore the placement — distributes via Murmur3. Within each bucket, the UUID is still the clustering column, sorted by byte order, exercising exactly the dynamic the thesis measured.

**Decision: option 3.** The thesis is option 3 with N=1; the multi-node extension is option 3 with N>1. Mechanically the same study, one knob turned.

**Bucket assignment:** `bucket = FNV-1a(id_bytes) mod N`. Deterministic from id alone — reads and updates recompute the bucket on the fly without needing to remember (bucket, id) pairs. Uniform distribution regardless of UUID type, because the hash uniformizes.

**Bucket count:** default `N=1000`, configurable via `-num-buckets` CLI flag. At 100M records this gives ~100K rows per partition (Cassandra's recommended healthy size). At smaller scales partitions shrink proportionally; the within-partition clustering effect is still exercised on whatever rows are there.

**Comparison to thesis numbers:** the bucketed schema changes two things vs the thesis simultaneously — cluster size (1 → 3 nodes) AND partition size (1 partition × full dataset → N partitions × dataset/N). Thesis numbers are not the appropriate comparison anchor for multi-node results. The single-node baseline with the new bucketed schema (Task 7.3) is the anchor. The paper acknowledges this explicitly.

---

## File Structure

**New files (created during this plan):**
- `internal/cluster/config.go` — `ClusterConfig` struct (Task 2.1)
- `internal/cluster/config_test.go` — config validation tests (Task 2.1)
- `internal/cluster/backend.go` — `Backend` interface (Task 3.3)
- `internal/cluster/local_single.go` — `LocalSingleBackend` (Task 3.4)
- `internal/cluster/local_single_test.go` — interface compliance test (Task 3.4)
- `internal/cluster/local_cluster.go` — `LocalClusterBackend` (Task 3.5)
- `internal/cluster/local_cluster_test.go` — interface compliance + name generation tests (Task 3.5)
- `internal/cluster/remote_cluster.go` — `RemoteClusterBackend` (Task 4.3)
- `internal/cluster/remote_cluster_test.go` — interface compliance test (Task 4.3)
- `internal/cluster/ring.go` — `ParseNodetoolStatus`, `AllNodesUp`, `WaitForRing` (Task 3.2)
- `internal/cluster/ring_test.go` — nodetool status parser tests (Task 3.2)
- `internal/remote/ssh.go` — SSH `Exec` and `Copy` (Tasks 4.1, 4.2)
- `internal/remote/ssh_test.go` — command-format tests (Task 4.1)
- `internal/benchmark/cassandra/connection_test.go` — schema generator tests + parseConsistency / replicationStmt tests (Tasks 1.5, 2.2)
- `internal/benchmark/workload/executor_test.go` — args-builder + mode-branch tests (Task 2.4)
- `internal/benchmark/io/io_metrics_test.go` — `parseIOStatContent` + cluster-stats tests (Task 5.2)
- `cmd/workload/main_test.go` — `bucketForID` tests (determinism, range, distribution, n=0) + contact-point parser tests (Tasks 1.1, 2.3)
- `docker/docker-compose.cassandra-cluster.yml` — 3-container local test cluster (Task 3.1)

**Modified files:**
- `internal/benchmark/cassandra/cassandra.go` — `New(cfg)` takes `ClusterConfig`; struct gets `cfg` field (Task 2.2)
- `internal/benchmark/cassandra/connection.go` — `Connect()` uses `cfg.ContactPoints`/RF/consistency; `CreateTable` delegates to `schemaForKeyType`; UUID-as-partition-key schema; no `bucket` column (Tasks 1.5, 2.2)
- `internal/benchmark/cassandra/metrics.go` — adds `AggregateSnapshots`, `CaptureMetricsBeforeAll`, `MeasureMetricsAll` (Task 5.1)
- `internal/benchmark/cassandra/insert.go`, `read.go`, `update.go`, `mixed.go` — accept `connString` and `execMode` parameters (Task 6.1 step 3)
- `internal/benchmark/io/io_metrics.go` — extracts `parseIOStatContent`, `readIOStatFile`; adds `NodeRef`, `GetClusterIOStats`, local + remote variants (Task 5.2)
- `internal/benchmark/workload/executor.go` — adds `ExecutionMode`, `BinaryPath`; branches container-vs-native (Task 2.4)
- `internal/runner/cassandra.go` — every scenario fn accepts `ClusterConfig` and `Backend`; uses cluster-aware metric capture (Task 6.1)
- `cmd/workload/main.go` — query constants hoisted with bucket placeholder; `cassandraBucket=1` replaced by hash-derived `bucketForID(idBytes, N) mod N`; `idAsBytes` helper; `--num-buckets` flag; comma-separated contact points via `parseContactPoints` (Tasks 1.1-1.5, 2.3)
- `cmd/benchmark/main.go` — new flags + `buildClusterConfig`/`buildBackend` helpers; orchestration loop branches by mode (Task 6.2)
- `CLAUDE.md` — Cluster Modes section, networking caveat, security note (Task 7.1)
- `README.md` — multi-node example invocations (Task 7.2)
- `go.mod` — adds `golang.org/x/crypto` (Task 4.1 step 1)

**Deleted/removed:**
- The `cassandraBucket=1` constant from `cmd/workload/main.go` — replaced by hash-derived bucket (Task 1.3)
- The `WHERE bucket = 1` filter from `fetchCassandraIDs` — replaced by unfiltered token-range sample (Task 1.5)
- The `bucket` column STAYS in the schema (an earlier plan revision proposed dropping it; see Schema Design Methodology section above for why that was wrong)

---

## Phase 1 — Bucket-distributed schema (UUID as clustering column, hash-derived bucket)

**Why first:** This is the methodological foundation of the paper extension. The thesis used `PRIMARY KEY ((bucket), id)` with `bucket=1` constant, putting every row in one partition and using the UUID as a clustering column whose byte order drove MemTable/SSTable layout. To extend to multi-node, we keep the same schema mechanics but spread `bucket` across N values, hashing the id to choose the bucket. This (a) distributes data across the ring via Murmur3 on the partition key, and (b) preserves within-partition UUID-clustering behavior — exactly the dynamic the thesis measured. See "Schema Design Methodology" above for why `id PRIMARY KEY` (which would destroy the UUID-ordering effect) and `bucket=1` (which doesn't distribute) are both wrong.

**Schema (unchanged from the thesis):** the DDL stays exactly as the thesis defined it — `bucket int` partition key, id as clustering column, per-key-type column choices. Only the *values* inserted into `bucket` change (from constant `1` to `bucketForID(idBytes, N) mod N`).

**Bucket assignment:** `bucket = FNV-1a(id_bytes) mod N`. Deterministic from id alone — reads/updates recompute the bucket on the fly without bookkeeping. Uniform distribution regardless of UUID type, because the hash uniformizes. See "Schema Design Methodology" for the full rationale.

**Bucket count `N`:** new `-num-buckets` CLI flag, default 1000.

### Task 1.1: Implement `bucketForID` helper with TDD

**Files:**
- Test: `cmd/workload/main_test.go` (create — does not currently exist; an earlier plan revision created and then deleted a stale version)
- Modify: `cmd/workload/main.go` (add `bucketForID` and import `hash/fnv`)

- [ ] **Step 1 (RED): Replace the contents of `cmd/workload/main_test.go` with bucket tests**

```go
package main

import (
	"crypto/rand"
	"testing"
)

func TestBucketForIDDeterministic(t *testing.T) {
	id := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}
	a := bucketForID(id, 1000)
	b := bucketForID(id, 1000)
	if a != b {
		t.Fatalf("not deterministic: %d != %d", a, b)
	}
}

func TestBucketForIDRange(t *testing.T) {
	for i := 0; i < 1000; i++ {
		id := make([]byte, 16)
		_, _ = rand.Read(id)
		got := bucketForID(id, 100)
		if got < 0 || got >= 100 {
			t.Fatalf("out of range: %d (n=100)", got)
		}
	}
}

func TestBucketForIDDistribution(t *testing.T) {
	const n = 100
	const samples = 10000
	counts := make([]int, n)
	for i := 0; i < samples; i++ {
		id := make([]byte, 16)
		_, _ = rand.Read(id)
		counts[bucketForID(id, n)]++
	}
	// Each bucket should hit ~100 times on average. Use generous tolerance
	// so the test is robust to random variance.
	for i, c := range counts {
		if c < 50 || c > 200 {
			t.Errorf("bucket %d outside [50,200] tolerance: count=%d", i, c)
		}
	}
}

func TestBucketForIDZeroN(t *testing.T) {
	// Defensive: n=0 must not panic or divide by zero.
	got := bucketForID([]byte{0x01}, 0)
	if got != 0 {
		t.Errorf("expected 0 for n=0, got %d", got)
	}
}
```

Run: `go test ./cmd/workload/ -run TestBucketForID -v`

Expected: build error — `undefined: bucketForID`. This is the RED step.

- [ ] **Step 2 (GREEN): Implement `bucketForID` in `cmd/workload/main.go`**

Add `"hash/fnv"` to the imports, and add the function (near the existing Cassandra section is fine):

```go
// bucketForID returns a stable bucket assignment for the given id bytes.
// Used to spread data across N Cassandra partitions while keeping id-as-
// clustering-column behavior from the thesis intact. Deterministic and
// uniform regardless of input distribution.
func bucketForID(id []byte, n int) int {
	if n <= 0 {
		return 0
	}
	h := fnv.New32a()
	h.Write(id)
	return int(h.Sum32() % uint32(n))
}
```

Run: `go test ./cmd/workload/ -run TestBucketForID -v`

Expected: PASS for all 4 subtests.

- [ ] **Step 3: Pause for review.**

### Task 1.2: Plumb `-num-buckets` flag through orchestrator and workload binary

**Architecture note (post-review):** `numBuckets` is stored on the `CassandraBenchmarker` struct via the constructor, **not** added as a parameter to the four `*Records`/`RunMixedWorkload` methods. This avoids signature churn in Phase 6 (which already grows those signatures with `connString` and `execMode`). Phase 2 (Task 2.2) will move `NumBuckets` from a standalone constructor argument into a `ClusterConfig` field; until then, `cassandra.New(numBuckets)` takes it directly.

**Files:**
- Modify: `cmd/workload/main.go` — add `--num-buckets` flag (default 1000), pass through to `runCassandra`
- Modify: `cmd/benchmark/main.go` — add `-num-buckets` flag (default 1000), pass through to runner scenarios
- Modify: `internal/benchmark/workload/executor.go` — add `NumBuckets int` field to `ExecutorConfig`, append `--num-buckets <N>` arg in `buildExecArgs` when N > 0
- Modify: `internal/benchmark/cassandra/cassandra.go` — add `numBuckets int` field to `CassandraBenchmarker` struct, take it via `New(numBuckets int)` constructor
- Modify: `internal/benchmark/cassandra/{insert,read,update,mixed}.go` — methods read `c.numBuckets` and populate `ExecutorConfig.NumBuckets` accordingly (no signature change)
- Modify: `internal/runner/cassandra.go` — accept `numBuckets` parameter on each scenario function and forward to `cassandra.New()`

- [ ] **Step 1: Workload binary flag**

In `cmd/workload/main.go`, add to the `flag.Parse()` section:
```go
var numBuckets int
flag.IntVar(&numBuckets, "num-buckets", 1000, "Number of Cassandra partition buckets (default 1000)")
```

Pass `numBuckets` into `runCassandra` (and any helper that needs it for read/update bucket recomputation).

- [ ] **Step 2: Orchestrator flag**

In `cmd/benchmark/main.go`, near the existing flag block:
```go
numBuckets := flag.Int("num-buckets", 1000, "Number of Cassandra partition buckets")
```

- [ ] **Step 3: Executor config**

In `internal/benchmark/workload/executor.go`:
```go
type ExecutorConfig struct {
    // ... existing fields ...
    NumBuckets int
}
```

In `buildExecArgs`:
```go
if cfg.NumBuckets > 0 {
    args = append(args, "--num-buckets", strconv.Itoa(cfg.NumBuckets))
}
```

- [ ] **Step 4: Add `numBuckets` to the CassandraBenchmarker struct and constructor**

In `internal/benchmark/cassandra/cassandra.go`:
```go
type CassandraBenchmarker struct {
    // ... existing fields ...
    numBuckets int
}

func New(numBuckets int) *CassandraBenchmarker {
    return &CassandraBenchmarker{
        // ... existing initialization ...
        numBuckets: numBuckets,
    }
}
```

In each of `insert.go`, `read.go`, `update.go`, `mixed.go`, when building the `ExecutorConfig`, set `NumBuckets: c.numBuckets` (alongside the other fields). **Do not change the public method signatures of `InsertRecords`/`ReadRecords`/`UpdateRecords`/`RunMixedWorkload`** — they already grow in Phase 6 and we don't want this field to add to that growth.

- [ ] **Step 5: Plumb through the runner**

In `internal/runner/cassandra.go`, each scenario function (`CassandraInsertPerformance`, `CassandraReadPerformance`, etc.) takes a new `numBuckets int` parameter at the end of its existing parameter list, and passes it to `cassandra.New(numBuckets)`. Update all call sites in `cmd/benchmark/main.go` to pass `*numBuckets` (the parsed flag) through.

(Phase 6 Task 6.1 will restructure these signatures further to take a `cluster.ClusterConfig` and `cluster.Backend` instead of standalone parameters — at that point `numBuckets` will be read from `cfg.NumBuckets` and the parameter goes away.)

- [ ] **Step 6: Build cleanly**

Run: `go build ./...`

Expected: clean. Public benchmarker method signatures are unchanged; only the constructor and the runner-scenario signatures grow by one int parameter.

- [ ] **Step 7: Pause for review.**

### Task 1.3: Update insert path to use hash-derived bucket

**Files:**
- Modify: `cmd/workload/main.go` `cassandraInsert`

- [ ] **Step 1: Delete `const cassandraBucket = 1`**

Verify with `grep -n cassandraBucket cmd/workload/main.go` — expect zero matches.

- [ ] **Step 2: Add an `idAsBytes` helper** in the same file, near `bucketForID`. It returns a stable byte slice for any id variant:

```go
// idAsBytes returns a stable byte representation of an id for hashing.
// Handles the id types produced by the various UUID/ULID/sequential generators
// in this workload binary.
func idAsBytes(id interface{}) []byte {
	switch v := id.(type) {
	case gocql.UUID:
		b := [16]byte(v)
		return b[:]
	case []byte:
		return v
	case int64:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, uint64(v))
		return buf
	default:
		// Defensive fallback: stringify. Stable per-type but suboptimal.
		return []byte(fmt.Sprintf("%v", v))
	}
}
```

(Add `"encoding/binary"` to imports if not already present.)

- [ ] **Step 3: Compute bucket per row in the insert loop**

In `cassandraInsert`, replace the existing `batch.Query(cassandraInsertQuery, cassandraBucket, key, payload)` call with:

```go
bucket := bucketForID(idAsBytes(key), numBuckets)
batch.Query(cassandraInsertQuery, bucket, key, payload)
```

(`numBuckets` is now in scope because it's been plumbed through in Task 1.2.)

- [ ] **Step 4: Build cleanly**

Run: `go build -o workload cmd/workload/main.go`

Expected: clean build.

- [ ] **Step 5: Pause for review.**

### Task 1.4: Update read, update, mixed paths and hoist query constants

**Files:**
- Modify: `cmd/workload/main.go` `cassandraRead`, `cassandraUpdate`, `cassandraMixed`

- [ ] **Step 1: Hoist query constants near the top of the Cassandra section**

```go
const (
	cassandraInsertQuery = "INSERT INTO bench (bucket, id, payload) VALUES (?, ?, ?)"
	cassandraReadQuery   = "SELECT payload FROM bench WHERE bucket = ? AND id = ?"
	cassandraUpdateQuery = "UPDATE bench SET payload = ? WHERE bucket = ? AND id = ?"
)
```

(These are *identical* to the thesis query shapes — just hoisted into named constants for clarity. The bucket parameter was always there; it just used to always be `1`.)

- [ ] **Step 2: Update `cassandraRead`** — for each id, compute the bucket then pass it to the query:

```go
bucket := bucketForID(idAsBytes(id), numBuckets)
err := session.Query(cassandraReadQuery, bucket, id).Scan(&payload)
```

- [ ] **Step 3: Update `cassandraUpdate`** — same pattern:

```go
bucket := bucketForID(idAsBytes(id), numBuckets)
err := session.Query(cassandraUpdateQuery, payload, bucket, id).Exec()
```

- [ ] **Step 4: Update `cassandraMixed`** — both the read and update inner paths compute the bucket. The mixed-insert path uses the same per-id bucket computation as Task 1.3.

- [ ] **Step 5: Replace any remaining inline `WHERE bucket = 1` strings** with the new constants.

Run: `grep -n 'bucket = 1' cmd/workload/main.go` — expect zero matches.

- [ ] **Step 6: Build cleanly**

Run: `go build -o workload cmd/workload/main.go`

Expected: clean.

- [ ] **Step 7: Pause for review.**

### Task 1.5: Update `fetchCassandraIDs` to sample across all buckets

**Files:**
- Modify: `cmd/workload/main.go` `fetchCassandraIDs`

- [ ] **Step 1: Drop the `WHERE bucket = 1` filter**

Old query: `SELECT id FROM bench WHERE bucket = 1 LIMIT M`
New query: `SELECT id FROM bench LIMIT M`

Cassandra handles unfiltered `SELECT ... LIMIT M` as a token-range scan that terminates as soon as M rows are collected. For sampling 10K ids out of millions, this is fine.

- [ ] **Step 2: Build cleanly**

Run: `go build -o workload cmd/workload/main.go`

Expected: clean.

- [ ] **Step 3: Pause for review.**

### Task 1.6: End-to-end smoke test against existing single-node

**Files:** (none modified — manual validation)

- [ ] **Step 1: Run a tiny insert+read benchmark**

```bash
go build -o uuid-benchmark cmd/benchmark/main.go
go build -o workload cmd/workload/main.go
./uuid-benchmark -database=cassandra -scenario=insert-performance \
    -num-records=10000 -batch-size=100 -connections=4 -num-buckets=1000
./uuid-benchmark -database=cassandra -scenario=read-performance \
    -num-records=10000 -num-ops=1000 -connections=4 -num-buckets=1000
```

Expected:
- All 6 UUID types complete inserts and reads
- No Cassandra errors in the log
- Throughput numbers differ from thesis numbers because partition shape changed (1000 partitions × 10 rows instead of 1 partition × 10000 rows); this is the methodological consequence of bucketing and is *expected*, not a regression

- [ ] **Step 2: Spot-check partition distribution**

```bash
docker exec uuid-bench-cassandra cqlsh -e \
    "SELECT bucket, count(*) FROM uuid_benchmark.bench GROUP BY bucket LIMIT 20" \
    uuid_benchmark
```

Expected: ~20 distinct bucket values with non-trivial row counts (with 10K rows / 1000 buckets, expect ~10 rows per bucket on average, with random variance).

- [ ] **Step 3: Pause for review.** Phase 1 complete — schema works on single-node with bucket-distributed partitioning, the UUID-clustering effect from the thesis is preserved within each bucket, and the implementation is ready for multi-node distribution.

---

## Phase 2 — Configurable connection layer

**Why second:** Decouples the database connection from hardcoded constants. Required before we can target multiple contact points or change RF.

### Task 2.1: Define `ClusterConfig`

**Files:**
- Create: `internal/cluster/config.go`
- Test: `internal/cluster/config_test.go`

- [ ] **Step 1: Write the failing test**

```go
package cluster

import "testing"

func TestClusterConfigDefaults(t *testing.T) {
	c := DefaultLocalSingle()
	if c.Mode != ModeLocalSingle {
		t.Errorf("default mode: got %v want %v", c.Mode, ModeLocalSingle)
	}
	if got, want := len(c.ContactPoints), 1; got != want {
		t.Errorf("contact points: got %d want %d", got, want)
	}
	if c.ContactPoints[0] != "127.0.0.1" {
		t.Errorf("contact point[0]: got %q want %q", c.ContactPoints[0], "127.0.0.1")
	}
	if c.ReplicationFactor != 1 {
		t.Errorf("RF: got %d want 1", c.ReplicationFactor)
	}
}

func TestClusterConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     ClusterConfig
		wantErr bool
	}{
		{"valid local single", DefaultLocalSingle(), false},
		{"valid local cluster (RF=3, 1 contact point)", ClusterConfig{Mode: ModeLocalCluster, ContactPoints: []string{"127.0.0.1"}, ReplicationFactor: 3, Consistency: "local_quorum", Keyspace: "uuid_benchmark"}, false},
		{"empty contact points", ClusterConfig{Mode: ModeRemoteCluster, ReplicationFactor: 3}, true},
		{"RF zero", ClusterConfig{Mode: ModeLocalSingle, ContactPoints: []string{"x"}}, true},
		{"remote RF greater than hostnames", ClusterConfig{Mode: ModeRemoteCluster, ContactPoints: []string{"a", "b"}, Hostnames: []string{"a", "b"}, SSHUser: "u", ReplicationFactor: 3}, true},
		{"remote missing SSH user", ClusterConfig{Mode: ModeRemoteCluster, ContactPoints: []string{"a", "b", "c"}, Hostnames: []string{"a", "b", "c"}, ReplicationFactor: 3}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() err=%v wantErr=%v", err, tt.wantErr)
			}
		})
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/cluster/ -v`

Expected: FAIL — package doesn't exist.

- [ ] **Step 3: Implement `config.go`**

```go
package cluster

import (
	"errors"
	"fmt"
)

type Mode string

const (
	ModeLocalSingle   Mode = "local-single"
	ModeLocalCluster  Mode = "local-cluster"
	ModeRemoteCluster Mode = "remote-cluster"
)

type ClusterConfig struct {
	Mode              Mode
	ContactPoints     []string // hostnames or IPs to pass to gocql
	Hostnames         []string // SSH hostnames (RemoteCluster only); equal to ContactPoints when on private DNS
	SSHUser           string   // RemoteCluster only
	SSHKeyPath        string   // RemoteCluster only
	ReplicationFactor int
	Consistency       string // "local_one", "local_quorum", "quorum"
	Keyspace          string
}

func DefaultLocalSingle() ClusterConfig {
	return ClusterConfig{
		Mode:              ModeLocalSingle,
		ContactPoints:     []string{"127.0.0.1"},
		ReplicationFactor: 1,
		Consistency:       "local_one",
		Keyspace:          "uuid_benchmark",
	}
}

func (c ClusterConfig) Validate() error {
	if len(c.ContactPoints) == 0 {
		return errors.New("at least one contact point required")
	}
	if c.ReplicationFactor < 1 {
		return errors.New("replication factor must be >= 1")
	}
	// RF-vs-node-count check is mode-conditional:
	// - LocalSingle / LocalCluster: only one contact point is used (the seed),
	//   but the actual node count is decoupled from ContactPoints (LocalCluster
	//   has 3 nodes behind 1 published port). Skip the per-mode RF check here.
	// - RemoteCluster: each hostname IS a node, so RF <= len(Hostnames).
	if c.Mode == ModeRemoteCluster {
		if c.SSHUser == "" {
			return errors.New("SSH user required for remote cluster")
		}
		if len(c.Hostnames) == 0 {
			return errors.New("SSH hostnames required for remote cluster")
		}
		if c.ReplicationFactor > len(c.Hostnames) {
			return fmt.Errorf("replication factor %d exceeds host count %d", c.ReplicationFactor, len(c.Hostnames))
		}
	}
	return nil
}
```

- [ ] **Step 4: Run the test to verify pass**

Run: `go test ./internal/cluster/ -v`

Expected: PASS for both tests, all subtests.

- [ ] **Step 5: Pause for review.**

### Task 2.2: CassandraBenchmarker takes a ClusterConfig

**Files:**
- Modify: `internal/benchmark/cassandra/cassandra.go` (the struct and `New()`)
- Modify: `internal/benchmark/cassandra/connection.go` (`Connect` method)

- [ ] **Step 1: Add a `ClusterConfig` field to the struct** — open `internal/benchmark/cassandra/cassandra.go`. Add field:

```go
import "github.com/moguls753/uuid-benchmark/internal/cluster"  // adjust to actual module path

type CassandraBenchmarker struct {
	session       *gocql.Session
	keyType       string
	tableName     string
	metricsBefore *CassandraMetricsSnapshot
	cfg           cluster.ClusterConfig
}

func New(cfg cluster.ClusterConfig) *CassandraBenchmarker {
	return &CassandraBenchmarker{
		cfg:       cfg,
		tableName: "bench",
	}
}
```

(Verify the actual module path with `head -1 go.mod`. Use that prefix for the cluster import.)

- [ ] **Step 2: Update `Connect()` in `connection.go`** to use the cfg:

```go
func (c *CassandraBenchmarker) Connect() error {
	cluster := gocql.NewCluster(c.cfg.ContactPoints...)
	cluster.Consistency = parseConsistency(c.cfg.Consistency)
	cluster.Timeout = 30 * time.Second
	cluster.ConnectTimeout = 30 * time.Second
	cluster.NumConns = 4

	// First session without keyspace, to create the keyspace
	bootstrap, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("bootstrap session: %w", err)
	}
	repl := replicationStmt(c.cfg)
	createKS := fmt.Sprintf(
		"CREATE KEYSPACE IF NOT EXISTS %s WITH replication = %s",
		c.cfg.Keyspace, repl,
	)
	if err := bootstrap.Query(createKS).Exec(); err != nil {
		bootstrap.Close()
		return fmt.Errorf("create keyspace: %w", err)
	}
	bootstrap.Close()

	cluster.Keyspace = c.cfg.Keyspace
	c.session, err = cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("session with keyspace: %w", err)
	}
	return nil
}

func parseConsistency(s string) gocql.Consistency {
	switch s {
	case "local_quorum":
		return gocql.LocalQuorum
	case "quorum":
		return gocql.Quorum
	case "one":
		return gocql.One
	case "local_one", "":
		return gocql.LocalOne
	default:
		return gocql.LocalOne
	}
}

func replicationStmt(c cluster.ClusterConfig) string {
	if c.Mode == ModeLocalSingle {
		return fmt.Sprintf("{'class': 'SimpleStrategy', 'replication_factor': %d}", c.ReplicationFactor)
	}
	return fmt.Sprintf("{'class': 'NetworkTopologyStrategy', 'dc1': %d}", c.ReplicationFactor)
}
```

- [ ] **Step 3: Add unit tests for `parseConsistency` and `replicationStmt`**

`internal/benchmark/cassandra/connection_test.go` (append to existing file):
```go
func TestParseConsistency(t *testing.T) {
	cases := map[string]gocql.Consistency{
		"local_one":    gocql.LocalOne,
		"local_quorum": gocql.LocalQuorum,
		"quorum":       gocql.Quorum,
		"one":          gocql.One,
		"":             gocql.LocalOne,
		"garbage":      gocql.LocalOne,
	}
	for input, want := range cases {
		t.Run(input, func(t *testing.T) {
			if got := parseConsistency(input); got != want {
				t.Errorf("parseConsistency(%q): got %v want %v", input, got, want)
			}
		})
	}
}

func TestReplicationStmt(t *testing.T) {
	t.Run("single uses SimpleStrategy", func(t *testing.T) {
		cfg := cluster.ClusterConfig{Mode: cluster.ModeLocalSingle, ReplicationFactor: 1}
		got := replicationStmt(cfg)
		if !strings.Contains(got, "SimpleStrategy") {
			t.Errorf("got %q", got)
		}
	})
	t.Run("remote uses NetworkTopologyStrategy with dc1", func(t *testing.T) {
		cfg := cluster.ClusterConfig{Mode: cluster.ModeRemoteCluster, ReplicationFactor: 3}
		got := replicationStmt(cfg)
		if !strings.Contains(got, "NetworkTopologyStrategy") {
			t.Errorf("got %q", got)
		}
		if !strings.Contains(got, "'dc1': 3") {
			t.Errorf("got %q", got)
		}
	})
}
```

- [ ] **Step 4: Update every caller of `cassandra.New()`** — find them with `grep -rn "cassandra.New(" internal/ cmd/`. Each call site needs to construct or receive a `ClusterConfig`. For now, they can pass `cluster.DefaultLocalSingle()` to preserve existing behavior. Phase 6 will wire CLI flags through.

- [ ] **Step 5: Run all tests**

Run: `go test ./...`

Expected: PASS. Existing single-node behavior preserved.

- [ ] **Step 6: Pause for review.**

### Task 2.3: Workload binary accepts comma-separated contact points

**Files:**
- Modify: `cmd/workload/main.go` (around lines 1013-1039, the `runCassandra` function)
- Test: `cmd/workload/main_test.go`

- [ ] **Step 1: Write a failing test**

```go
func TestParseContactPoints(t *testing.T) {
	cases := map[string][]string{
		"127.0.0.1":                          {"127.0.0.1"},
		"taurus5,taurus6,taurus7":            {"taurus5", "taurus6", "taurus7"},
		"taurus5:9042,taurus6:9042":          {"taurus5:9042", "taurus6:9042"},
		" taurus5 , taurus6 ":                {"taurus5", "taurus6"},
		"":                                   {},
	}
	for input, want := range cases {
		t.Run(input, func(t *testing.T) {
			got := parseContactPoints(input)
			if !equalStrings(got, want) {
				t.Errorf("parseContactPoints(%q): got %v want %v", input, got, want)
			}
		})
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./cmd/workload/ -run TestParseContactPoints -v`

Expected: FAIL — `parseContactPoints` doesn't exist.

- [ ] **Step 3: Implement `parseContactPoints`** in `cmd/workload/main.go` near the Cassandra section:

```go
func parseContactPoints(s string) []string {
	if s == "" {
		return []string{}
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}
```

- [ ] **Step 4: Update `runCassandra` to use `parseContactPoints(connString)` and pass the slice to `gocql.NewCluster(points...)`**

Replace:
```go
cluster := gocql.NewCluster(connString)
```
with:
```go
points := parseContactPoints(connString)
if len(points) == 0 {
	return nil, fmt.Errorf("no Cassandra contact points provided")
}
cluster := gocql.NewCluster(points...)
```

- [ ] **Step 5: Run the test**

Run: `go test ./cmd/workload/ -v`

Expected: PASS.

- [ ] **Step 6: Build and confirm the binary still works on single-node**

Run:
```bash
go build -o workload cmd/workload/main.go
./uuid-benchmark -database=cassandra -scenario=insert-performance \
    -num-records=1000 -batch-size=100 -connections=2
```

Expected: benchmark completes, no errors.

- [ ] **Step 7: Pause for review.**

### Task 2.4: Workload executor supports native (non-container) execution

**Why:** For multi-node modes, the workload binary runs directly on the orchestrator (taurus4) and connects over the network to the cluster. The current executor unconditionally does `docker cp` + `docker exec`, which only makes sense for the local-single mode. We add an `ExecutionMode` so callers can pick.

**Files:**
- Modify: `internal/benchmark/workload/executor.go` (around lines 95-167)
- Test: `internal/benchmark/workload/executor_test.go` (create file)

- [ ] **Step 1: Write the failing test for argument-list assembly**

```go
package workload

import (
	"strings"
	"testing"
)

func TestBuildExecArgs(t *testing.T) {
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
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/benchmark/workload/ -run "TestBuildExecArgs|TestExecuteRejectsUnknownMode|TestExecuteContainerRequiresContainerName" -v`

Expected: FAIL — `buildExecArgs` and `ExecutionModeContainer` don't exist.

- [ ] **Step 3: Refactor `executor.go`** — add the mode type, extract args assembly, branch the execution

```go
// At the top of the file, alongside ExecutorConfig:

type ExecutionMode string

const (
	// ExecutionModeContainer (default for backward compatibility) docker cp's
	// the workload binary into the named container and runs it via docker exec.
	ExecutionModeContainer ExecutionMode = "container"
	// ExecutionModeNative runs the workload binary directly on the host where
	// the orchestrator runs. Used for multi-node clusters where the workload
	// connects over the network to one of N nodes.
	ExecutionModeNative ExecutionMode = "native"
)

type ExecutorConfig struct {
	Mode             ExecutionMode // default: ExecutionModeContainer
	BinaryPath       string        // for ExecutionModeNative; default "./workload"
	ContainerName    string        // for ExecutionModeContainer
	DBType           string
	Op               string
	KeyType          string
	NumRecords       int
	NumOps           int
	BatchSize        int
	Threads          int
	ConnectionString string
	InsertPct        int
	ReadPct          int
	UpdatePct        int
	TableName        string
}
```

Pull the existing args-building loop into a helper:

```go
func buildExecArgs(cfg ExecutorConfig) []string {
	args := []string{
		"--db-type", cfg.DBType,
		"--op", cfg.Op,
		"--key-type", cfg.KeyType,
		"--connection-string", cfg.ConnectionString,
	}
	if cfg.NumRecords > 0 {
		args = append(args, "--num-records", strconv.Itoa(cfg.NumRecords))
	}
	if cfg.NumOps > 0 {
		args = append(args, "--num-ops", strconv.Itoa(cfg.NumOps))
	}
	if cfg.BatchSize > 0 {
		args = append(args, "--batch-size", strconv.Itoa(cfg.BatchSize))
	}
	if cfg.Threads > 0 {
		args = append(args, "--threads", strconv.Itoa(cfg.Threads))
	}
	if cfg.InsertPct > 0 || cfg.ReadPct > 0 || cfg.UpdatePct > 0 {
		args = append(args,
			"--insert-pct", strconv.Itoa(cfg.InsertPct),
			"--read-pct", strconv.Itoa(cfg.ReadPct),
			"--update-pct", strconv.Itoa(cfg.UpdatePct),
		)
	}
	if cfg.TableName != "" {
		args = append(args, "--table-name", cfg.TableName)
	}
	return args
}
```

Branch the actual command in `Execute`:

```go
func Execute(cfg ExecutorConfig) (*WorkloadResult, error) {
	mode := cfg.Mode
	if mode == "" {
		mode = ExecutionModeContainer
	}
	args := buildExecArgs(cfg)

	var cmd *exec.Cmd
	switch mode {
	case ExecutionModeContainer:
		if cfg.ContainerName == "" {
			return nil, fmt.Errorf("ExecutionModeContainer requires ContainerName")
		}
		if err := CopyToContainer(cfg.ContainerName); err != nil {
			return nil, err
		}
		full := append([]string{"exec", cfg.ContainerName, "/tmp/workload"}, args...)
		cmd = exec.Command("docker", full...)
	case ExecutionModeNative:
		path := cfg.BinaryPath
		if path == "" {
			path = "./workload"
		}
		cmd = exec.Command(path, args...)
	default:
		return nil, fmt.Errorf("unknown ExecutionMode: %q", mode)
	}

	out, err := cmd.CombinedOutput()
	if err != nil {
		return nil, fmt.Errorf("workload execute (%s): %w (output: %s)", mode, err, out)
	}
	return ParseResult(string(out))
}
```

- [ ] **Step 4: Run tests**

Run: `go test ./internal/benchmark/workload/ -v`

Expected: PASS for all subtests including the new ones plus the existing parser tests.

- [ ] **Step 5: Run a single-node regression** to confirm container mode still works (default mode preserved):

```bash
go build -o uuid-benchmark cmd/benchmark/main.go
go build -o workload cmd/workload/main.go
./uuid-benchmark -database=cassandra -scenario=insert-performance \
    -num-records=1000 -batch-size=100 -connections=2
```

Expected: completes — existing call sites that don't set `Mode` still hit the container path.

- [ ] **Step 6: Pause for review.**

---

## Phase 3 — Local 3-node Cassandra cluster (laptop testing)

**Why third:** This gives us a way to test the multi-node code paths without depending on the Taurus admin. The laptop cluster won't produce meaningful performance numbers — it's for code correctness, not measurement.

**⚠ Networking caveat (read before implementing):**
The Compose cluster runs all three Cassandra nodes on a Docker bridge network. Only `cassandra-1` publishes port 9042 to the host (you can't bind the same port from three containers). The gocql driver on the host will:

1. Connect to `127.0.0.1:9042` (which is `cassandra-1`)
2. Learn the other two nodes' addresses via gossip — these come back as Docker-internal IPs (e.g. `172.18.0.3`, `172.18.0.4`) which are **not reachable from the host**
3. Try to connect to those IPs and silently fail or fall back to coordinator routing

**Practical effect:** All client queries route through `cassandra-1` as coordinator. `cassandra-1` then internally replicates to the other nodes via the Docker bridge network (which works fine — node-to-node traffic stays inside the network). So:
- ✅ Cluster forms, ring stabilizes, RF=3 replication works
- ✅ Multi-node metrics (cgroup IO, nodetool tablestats per node) all work — they're collected via `docker exec`, not via the network
- ✅ `WaitForRing`, `Backend.Start/Stop/ExecOnNode` all exercise multi-node code paths
- ❌ Token-aware routing is degraded — every query is coordinated through `cassandra-1`, distorting per-node load
- ❌ Performance numbers are NOT representative of a real distributed deployment

This is acceptable for code-correctness validation. **Real performance measurement requires the remote-cluster mode on Taurus**, where each Cassandra runs on its own physical host with a real, externally-reachable IP. Document this caveat in CLAUDE.md (Task 7.1) and never use LocalCluster numbers in the paper.

### Task 3.1: Write the cluster docker-compose file

**Files:**
- Create: `docker/docker-compose.cassandra-cluster.yml`

- [ ] **Step 1: Create the compose file with 3 services**

```yaml
services:
  cassandra-1:
    image: cassandra:5
    container_name: uuid-bench-cassandra-1
    environment:
      CASSANDRA_CLUSTER_NAME: UUIDBenchCluster
      CASSANDRA_DC: dc1
      CASSANDRA_ENDPOINT_SNITCH: GossipingPropertyFileSnitch
      CASSANDRA_SEEDS: cassandra-1
      MAX_HEAP_SIZE: 1G
      HEAP_NEWSIZE: 256M
    ports:
      - "9042:9042"
    networks:
      - benchmark_network
    deploy:
      resources:
        limits:
          cpus: "2"
          memory: 2G

  cassandra-2:
    image: cassandra:5
    container_name: uuid-bench-cassandra-2
    environment:
      CASSANDRA_CLUSTER_NAME: UUIDBenchCluster
      CASSANDRA_DC: dc1
      CASSANDRA_ENDPOINT_SNITCH: GossipingPropertyFileSnitch
      CASSANDRA_SEEDS: cassandra-1
      MAX_HEAP_SIZE: 1G
      HEAP_NEWSIZE: 256M
    depends_on:
      - cassandra-1
    networks:
      - benchmark_network
    deploy:
      resources:
        limits:
          cpus: "2"
          memory: 2G

  cassandra-3:
    image: cassandra:5
    container_name: uuid-bench-cassandra-3
    environment:
      CASSANDRA_CLUSTER_NAME: UUIDBenchCluster
      CASSANDRA_DC: dc1
      CASSANDRA_ENDPOINT_SNITCH: GossipingPropertyFileSnitch
      CASSANDRA_SEEDS: cassandra-1
      MAX_HEAP_SIZE: 1G
      HEAP_NEWSIZE: 256M
    depends_on:
      - cassandra-1
    networks:
      - benchmark_network
    deploy:
      resources:
        limits:
          cpus: "2"
          memory: 2G

networks:
  benchmark_network:
    driver: bridge
```

- [ ] **Step 2: Manual smoke test — bring the cluster up**

Run:
```bash
docker compose -f docker/docker-compose.cassandra-cluster.yml up -d
sleep 90
docker exec uuid-bench-cassandra-1 nodetool status
```

Expected output should show 3 nodes all `UN`:
```
--  Address      Load       ...  Status
UN  172.x.x.2    ...
UN  172.x.x.3    ...
UN  172.x.x.4    ...
```

- [ ] **Step 3: Tear down**

Run: `docker compose -f docker/docker-compose.cassandra-cluster.yml down -v --remove-orphans`

- [ ] **Step 4: Pause for review.**

### Task 3.2: `nodetool status` parser and `WaitForRing`

**Files:**
- Create: `internal/cluster/ring.go`
- Test: `internal/cluster/ring_test.go`

- [ ] **Step 1: Write the failing test**

```go
package cluster

import "testing"

const sampleNodetoolStatus = `Datacenter: dc1
===============
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address       Load        Tokens  Owns  Host ID                               Rack
UN  172.18.0.2    250 KiB     16      ?     a1b2c3d4-0000-0000-0000-000000000001  rack1
UN  172.18.0.3    248 KiB     16      ?     a1b2c3d4-0000-0000-0000-000000000002  rack1
UN  172.18.0.4    245 KiB     16      ?     a1b2c3d4-0000-0000-0000-000000000003  rack1
`

const sampleNodetoolPartial = `Datacenter: dc1
===============
--  Address       Load   Tokens  Owns  Host ID  Rack
UN  172.18.0.2    250 K  16      ?     a-b-c    rack1
DN  172.18.0.3    0      16      ?     d-e-f    rack1
UJ  172.18.0.4    0      16      ?     g-h-i    rack1
`

func TestParseNodetoolStatus(t *testing.T) {
	t.Run("all UN nodes counted", func(t *testing.T) {
		nodes, err := ParseNodetoolStatus(sampleNodetoolStatus)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got, want := len(nodes), 3; got != want {
			t.Fatalf("got %d nodes want %d", got, want)
		}
		for _, n := range nodes {
			if n.Status != "UN" {
				t.Errorf("node %s: status %q != UN", n.Address, n.Status)
			}
		}
	})
	t.Run("non-UN nodes still parsed but flagged", func(t *testing.T) {
		nodes, err := ParseNodetoolStatus(sampleNodetoolPartial)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if got, want := len(nodes), 3; got != want {
			t.Fatalf("got %d nodes want %d", got, want)
		}
		un := 0
		for _, n := range nodes {
			if n.Status == "UN" {
				un++
			}
		}
		if un != 1 {
			t.Errorf("expected 1 UN node, got %d", un)
		}
	})
}

func TestAllNodesUp(t *testing.T) {
	t.Run("all up returns true", func(t *testing.T) {
		nodes, _ := ParseNodetoolStatus(sampleNodetoolStatus)
		if !AllNodesUp(nodes, 3) {
			t.Error("expected AllNodesUp to be true")
		}
	})
	t.Run("expected count not met", func(t *testing.T) {
		nodes, _ := ParseNodetoolStatus(sampleNodetoolStatus)
		if AllNodesUp(nodes, 5) {
			t.Error("expected false when fewer UN nodes than expected")
		}
	})
	t.Run("not all UN", func(t *testing.T) {
		nodes, _ := ParseNodetoolStatus(sampleNodetoolPartial)
		if AllNodesUp(nodes, 3) {
			t.Error("expected false when some nodes not UN")
		}
	})
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/cluster/ -v`

Expected: FAIL — `ParseNodetoolStatus` and `AllNodesUp` don't exist.

- [ ] **Step 3: Implement `ring.go`**

```go
package cluster

import (
	"fmt"
	"strings"
	"time"
)

type RingNode struct {
	Status  string // "UN", "DN", "UJ", "UL", "UM"
	Address string
}

func ParseNodetoolStatus(output string) ([]RingNode, error) {
	var nodes []RingNode
	for _, line := range strings.Split(output, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		st := fields[0]
		if len(st) != 2 {
			continue
		}
		// Status codes: 2 chars, first is U or D, second is N/J/L/M
		first := st[0]
		second := st[1]
		if (first != 'U' && first != 'D') || strings.IndexByte("NJLM", second) < 0 {
			continue
		}
		nodes = append(nodes, RingNode{Status: st, Address: fields[1]})
	}
	if len(nodes) == 0 {
		return nil, fmt.Errorf("no nodes found in nodetool output")
	}
	return nodes, nil
}

func AllNodesUp(nodes []RingNode, expected int) bool {
	un := 0
	for _, n := range nodes {
		if n.Status == "UN" {
			un++
		}
	}
	return un == expected && len(nodes) == expected
}

// WaitForRing polls nodetool status (via the backend's exec on node 0) until
// all `expected` nodes report UN, or until timeout.
func WaitForRing(b Backend, expected int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		out, err := b.ExecOnNode(0, "nodetool status")
		if err == nil {
			nodes, perr := ParseNodetoolStatus(out)
			if perr == nil && AllNodesUp(nodes, expected) {
				return nil
			}
		}
		time.Sleep(3 * time.Second)
	}
	return fmt.Errorf("ring did not stabilize within %s (expected %d UN nodes)", timeout, expected)
}
```

- [ ] **Step 4: Run the test**

Run: `go test ./internal/cluster/ -v`

Expected: PASS.

- [ ] **Step 5: Pause for review.** Note: `Backend` interface is referenced but not yet defined — that's the next task. Build will fail until then. That's acceptable; we'll fix it in 3.3.

### Task 3.3: Define the `Backend` interface

**Files:**
- Create: `internal/cluster/backend.go`

- [ ] **Step 1: Add the interface**

```go
package cluster

// Backend abstracts the lifecycle and node access for a Cassandra deployment,
// whether it's a local single container, a local multi-container compose,
// or remote SSH-managed nodes.
type Backend interface {
	// Start brings the cluster up. Idempotent — safe to call after a previous
	// Stop.
	Start() error

	// Stop tears the cluster down and removes all volumes.
	Stop() error

	// WaitForReady blocks until the cluster reports healthy. For multi-node,
	// this means all nodes UN in nodetool status.
	WaitForReady() error

	// ExecOnNode runs a shell command inside (or on the host of) the i-th
	// node. Used for nodetool, cgroup reads, etc.
	ExecOnNode(i int, cmd string) (string, error)

	// CopyToNode copies a file to the i-th node. Used when a backend needs
	// to deploy helpers; not used by the standard workload (which runs on
	// the orchestrator).
	CopyToNode(i int, src, dst string) error

	// NodeAddresses returns gocql contact points (one per node, in CQL form,
	// e.g. "taurus5" or "127.0.0.1:9043").
	NodeAddresses() []string

	// NodeContainerIDs returns the docker container ID on each node's host.
	// Used for cgroup-v2 IO metrics. Empty strings mean unavailable.
	NodeContainerIDs() ([]string, error)

	// NodeCount returns the number of nodes managed by this backend.
	NodeCount() int

	// Mode returns the cluster mode this backend implements.
	Mode() Mode
}
```

- [ ] **Step 2: Build to confirm everything compiles**

Run: `go build ./...`

Expected: clean build. The interface is referenced from `ring.go`'s `WaitForRing` but has no implementations yet — that's fine; nothing tries to instantiate one yet.

- [ ] **Step 3: Pause for review.**

### Task 3.4: Implement `LocalSingleBackend`

**Files:**
- Create: `internal/cluster/local_single.go`
- Test: `internal/cluster/local_single_test.go` (interface compliance check only — no real container)

- [ ] **Step 1: Write the interface compliance test**

```go
package cluster

import "testing"

func TestLocalSingleImplementsBackend(t *testing.T) {
	var _ Backend = (*LocalSingleBackend)(nil)
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/cluster/ -run TestLocalSingleImplementsBackend -v`

Expected: FAIL — `LocalSingleBackend` doesn't exist.

- [ ] **Step 3: Implement `LocalSingleBackend`**

```go
package cluster

import (
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const localSingleContainer = "uuid-bench-cassandra"
const localSingleCompose = "docker/docker-compose.cassandra.yml"

type LocalSingleBackend struct{}

func NewLocalSingle() *LocalSingleBackend { return &LocalSingleBackend{} }

func (b *LocalSingleBackend) Mode() Mode { return ModeLocalSingle }
func (b *LocalSingleBackend) NodeCount() int { return 1 }

func (b *LocalSingleBackend) Start() error {
	if err := exec.Command("docker", "compose", "-f", localSingleCompose, "down", "-v", "--remove-orphans").Run(); err != nil {
		// non-fatal: maybe nothing was running
	}
	out, err := exec.Command("docker", "compose", "-f", localSingleCompose, "up", "-d").CombinedOutput()
	if err != nil {
		return fmt.Errorf("compose up: %v: %s", err, out)
	}
	return nil
}

func (b *LocalSingleBackend) Stop() error {
	out, err := exec.Command("docker", "compose", "-f", localSingleCompose, "down", "-v", "--remove-orphans").CombinedOutput()
	if err != nil {
		return fmt.Errorf("compose down: %v: %s", err, out)
	}
	return nil
}

func (b *LocalSingleBackend) WaitForReady() error {
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		err := exec.Command("docker", "exec", localSingleContainer, "cqlsh", "-e", "SELECT release_version FROM system.local").Run()
		if err == nil {
			return nil
		}
		time.Sleep(2 * time.Second)
	}
	return fmt.Errorf("Cassandra single-node not ready within 120s")
}

func (b *LocalSingleBackend) ExecOnNode(i int, cmd string) (string, error) {
	if i != 0 {
		return "", fmt.Errorf("LocalSingleBackend has only node 0, got %d", i)
	}
	out, err := exec.Command("docker", "exec", localSingleContainer, "sh", "-c", cmd).CombinedOutput()
	return string(out), err
}

func (b *LocalSingleBackend) CopyToNode(i int, src, dst string) error {
	if i != 0 {
		return fmt.Errorf("LocalSingleBackend has only node 0, got %d", i)
	}
	out, err := exec.Command("docker", "cp", src, fmt.Sprintf("%s:%s", localSingleContainer, dst)).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker cp: %v: %s", err, out)
	}
	return nil
}

func (b *LocalSingleBackend) NodeAddresses() []string {
	return []string{"127.0.0.1"}
}

func (b *LocalSingleBackend) NodeContainerIDs() ([]string, error) {
	out, err := exec.Command("docker", "ps", "--filter", "name="+localSingleContainer, "--format", "{{.ID}}", "--no-trunc").Output()
	if err != nil {
		return nil, err
	}
	id := strings.TrimSpace(string(out))
	return []string{id}, nil
}
```

- [ ] **Step 4: Run the compliance test**

Run: `go test ./internal/cluster/ -run TestLocalSingleImplementsBackend -v`

Expected: PASS.

- [ ] **Step 5: Pause for review.**

### Task 3.5: Implement `LocalClusterBackend`

**Files:**
- Create: `internal/cluster/local_cluster.go`
- Test: `internal/cluster/local_cluster_test.go`

- [ ] **Step 1: Compliance test + container name generation test**

```go
package cluster

import "testing"

func TestLocalClusterImplementsBackend(t *testing.T) {
	var _ Backend = (*LocalClusterBackend)(nil)
}

func TestLocalClusterContainerNames(t *testing.T) {
	b := NewLocalCluster(3)
	want := []string{"uuid-bench-cassandra-1", "uuid-bench-cassandra-2", "uuid-bench-cassandra-3"}
	got := b.containerNames()
	if !equalStringSlice(got, want) {
		t.Errorf("got %v want %v", got, want)
	}
}

func equalStringSlice(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/cluster/ -run TestLocalCluster -v`

Expected: FAIL.

- [ ] **Step 3: Implement `local_cluster.go`**

```go
package cluster

import (
	"fmt"
	"os/exec"
	"strings"
	"time"
)

const localClusterCompose = "docker/docker-compose.cassandra-cluster.yml"

type LocalClusterBackend struct {
	nodes int
}

func NewLocalCluster(nodes int) *LocalClusterBackend {
	return &LocalClusterBackend{nodes: nodes}
}

func (b *LocalClusterBackend) Mode() Mode { return ModeLocalCluster }
func (b *LocalClusterBackend) NodeCount() int { return b.nodes }

func (b *LocalClusterBackend) containerNames() []string {
	names := make([]string, b.nodes)
	for i := 0; i < b.nodes; i++ {
		names[i] = fmt.Sprintf("uuid-bench-cassandra-%d", i+1)
	}
	return names
}

func (b *LocalClusterBackend) Start() error {
	_ = exec.Command("docker", "compose", "-f", localClusterCompose, "down", "-v", "--remove-orphans").Run()
	out, err := exec.Command("docker", "compose", "-f", localClusterCompose, "up", "-d").CombinedOutput()
	if err != nil {
		return fmt.Errorf("compose up cluster: %v: %s", err, out)
	}
	return nil
}

func (b *LocalClusterBackend) Stop() error {
	out, err := exec.Command("docker", "compose", "-f", localClusterCompose, "down", "-v", "--remove-orphans").CombinedOutput()
	if err != nil {
		return fmt.Errorf("compose down cluster: %v: %s", err, out)
	}
	return nil
}

func (b *LocalClusterBackend) WaitForReady() error {
	// Wait up to 5 minutes for the ring to form
	return WaitForRing(b, b.nodes, 5*time.Minute)
}

func (b *LocalClusterBackend) ExecOnNode(i int, cmd string) (string, error) {
	if i < 0 || i >= b.nodes {
		return "", fmt.Errorf("node index %d out of range [0, %d)", i, b.nodes)
	}
	out, err := exec.Command("docker", "exec", b.containerNames()[i], "sh", "-c", cmd).CombinedOutput()
	return string(out), err
}

func (b *LocalClusterBackend) CopyToNode(i int, src, dst string) error {
	if i < 0 || i >= b.nodes {
		return fmt.Errorf("node index %d out of range [0, %d)", i, b.nodes)
	}
	out, err := exec.Command("docker", "cp", src, fmt.Sprintf("%s:%s", b.containerNames()[i], dst)).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker cp: %v: %s", err, out)
	}
	return nil
}

func (b *LocalClusterBackend) NodeAddresses() []string {
	// All cluster nodes share the host's port 9042 via the seed; only one
	// container can publish 9042. The driver will discover the others via
	// gossip after the initial connect.
	return []string{"127.0.0.1"}
}

func (b *LocalClusterBackend) NodeContainerIDs() ([]string, error) {
	ids := make([]string, b.nodes)
	for i, name := range b.containerNames() {
		out, err := exec.Command("docker", "ps", "--filter", "name="+name, "--format", "{{.ID}}", "--no-trunc").Output()
		if err != nil {
			return nil, err
		}
		ids[i] = strings.TrimSpace(string(out))
	}
	return ids, nil
}
```

- [ ] **Step 4: Run the tests**

Run: `go test ./internal/cluster/ -v`

Expected: PASS for all tests in the package.

- [ ] **Step 5: Manual end-to-end smoke**

Run:
```bash
go build -o uuid-benchmark cmd/benchmark/main.go
go build -o workload cmd/workload/main.go
docker compose -f docker/docker-compose.cassandra-cluster.yml up -d
# Wait 60-90s for the ring to form
sleep 90
docker exec uuid-bench-cassandra-1 nodetool status
# Should show 3 UN nodes
docker compose -f docker/docker-compose.cassandra-cluster.yml down -v
```

Expected: 3 UN nodes, then clean teardown. (CLI integration of cluster mode comes in Phase 6.)

- [ ] **Step 6: Pause for review.**

---

## Phase 4 — Remote SSH backend

**Why fourth:** Final piece needed for the real Taurus cluster. Built on top of the same `Backend` interface so the runner code doesn't change.

### Task 4.1: Minimal SSH executor

**Files:**
- Create: `internal/remote/ssh.go`
- Test: `internal/remote/ssh_test.go`

- [ ] **Step 1: Add `golang.org/x/crypto` to dependencies**

Run: `go get golang.org/x/crypto/ssh`

- [ ] **Step 2: Write the failing test**

```go
package remote

import (
	"strings"
	"testing"
)

func TestSSHCommandFormat(t *testing.T) {
	// We test the command-string assembly logic, not actual SSH.
	t.Run("simple cmd", func(t *testing.T) {
		got := buildShellCommand("nodetool status")
		if !strings.Contains(got, "nodetool status") {
			t.Errorf("got %q", got)
		}
	})
	t.Run("escapes are preserved", func(t *testing.T) {
		got := buildShellCommand(`echo "hello world"`)
		if !strings.Contains(got, `echo "hello world"`) {
			t.Errorf("got %q", got)
		}
	})
}
```

- [ ] **Step 3: Run to verify failure**

Run: `go test ./internal/remote/ -v`

Expected: FAIL — package doesn't exist.

- [ ] **Step 4: Implement `ssh.go`**

```go
package remote

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"time"

	"golang.org/x/crypto/ssh"
)

type Client struct {
	user   string
	keyPath string
}

func NewClient(user, keyPath string) *Client {
	return &Client{user: user, keyPath: keyPath}
}

func (c *Client) signer() (ssh.Signer, error) {
	path := c.keyPath
	if path == "" {
		path = filepath.Join(os.Getenv("HOME"), ".ssh", "id_ed25519")
	}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read key %s: %w", path, err)
	}
	return ssh.ParsePrivateKey(data)
}

func (c *Client) dial(host string) (*ssh.Client, error) {
	signer, err := c.signer()
	if err != nil {
		return nil, err
	}
	cfg := &ssh.ClientConfig{
		User:            c.user,
		Auth:            []ssh.AuthMethod{ssh.PublicKeys(signer)},
		HostKeyCallback: ssh.InsecureIgnoreHostKey(), // benchmark on private network
		Timeout:         15 * time.Second,
	}
	addr := host
	if !hasPort(host) {
		addr = host + ":22"
	}
	return ssh.Dial("tcp", addr, cfg)
}

func hasPort(s string) bool {
	_, _, err := net.SplitHostPort(s)
	return err == nil
}

// Exec runs a command on the remote host and returns combined stdout+stderr.
func (c *Client) Exec(host, cmd string) (string, error) {
	client, err := c.dial(host)
	if err != nil {
		return "", err
	}
	defer client.Close()
	session, err := client.NewSession()
	if err != nil {
		return "", err
	}
	defer session.Close()
	var buf bytes.Buffer
	session.Stdout = &buf
	session.Stderr = &buf
	full := buildShellCommand(cmd)
	if err := session.Run(full); err != nil {
		return buf.String(), fmt.Errorf("ssh %s: %w (output: %s)", host, err, buf.String())
	}
	return buf.String(), nil
}

func buildShellCommand(cmd string) string {
	// Run via `sh -c` so quoting and pipelines work.
	return "sh -c " + shellQuote(cmd)
}

func shellQuote(s string) string {
	// Single-quote, escaping internal single quotes.
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
```

(Add `"strings"` to the imports.)

- [ ] **Step 5: Run tests**

Run: `go test ./internal/remote/ -v`

Expected: PASS.

- [ ] **Step 6: Pause for review.** Note: this implementation uses `InsecureIgnoreHostKey` because the Taurus cluster is on a private VPN. For a public-internet deployment we'd verify host keys, but for this benchmark on a closed network it's acceptable. Document this in the security note in CLAUDE.md.

### Task 4.2: SSH file copy via SCP

**Files:**
- Modify: `internal/remote/ssh.go`
- Test: `internal/remote/ssh_test.go` (extend)

- [ ] **Step 1: Add a copy method using `scp` shelling out** — simpler than implementing the SCP protocol in Go for our needs.

```go
// Copy uses the local `scp` binary to transfer src → user@host:dst.
// Requires scp to be available on the orchestrator and ssh keys configured.
func (c *Client) Copy(host, src, dst string) error {
	keyArg := []string{}
	if c.keyPath != "" {
		keyArg = []string{"-i", c.keyPath}
	}
	args := append(keyArg,
		"-o", "StrictHostKeyChecking=no",
		"-o", "UserKnownHostsFile=/dev/null",
		src,
		fmt.Sprintf("%s@%s:%s", c.user, host, dst),
	)
	out, err := exec.Command("scp", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("scp %s -> %s:%s: %v: %s", src, host, dst, err, out)
	}
	return nil
}
```

(Add `"os/exec"` to imports.)

- [ ] **Step 2: Pause for review.** No test needed for `Copy` itself — it's a thin shell-out and would need real network to test meaningfully.

### Task 4.3: Implement `RemoteClusterBackend`

**Files:**
- Create: `internal/cluster/remote_cluster.go`
- Test: `internal/cluster/remote_cluster_test.go`

- [ ] **Step 1: Compliance test**

```go
package cluster

import "testing"

func TestRemoteClusterImplementsBackend(t *testing.T) {
	var _ Backend = (*RemoteClusterBackend)(nil)
}
```

- [ ] **Step 2: Implement**

```go
package cluster

import (
	"fmt"
	"strings"
	"time"

	"github.com/moguls753/uuid-benchmark/internal/remote" // adjust to actual module path
)

// RemoteClusterBackend manages a Cassandra cluster across multiple physical
// hosts via SSH. Each host runs a single `cassandra` Docker container.
type RemoteClusterBackend struct {
	hostnames []string
	user      string
	ssh       *remote.Client
	cluster   string // CASSANDRA_CLUSTER_NAME
	dc        string
	heap      string // e.g. "8G"
	newGen    string
}

func NewRemoteCluster(cfg ClusterConfig) *RemoteClusterBackend {
	return &RemoteClusterBackend{
		hostnames: cfg.Hostnames,
		user:      cfg.SSHUser,
		ssh:       remote.NewClient(cfg.SSHUser, cfg.SSHKeyPath),
		cluster:   "UUIDBenchCluster",
		dc:        "dc1",
		heap:      "8G",
		newGen:    "2G",
	}
}

func (b *RemoteClusterBackend) Mode() Mode      { return ModeRemoteCluster }
func (b *RemoteClusterBackend) NodeCount() int  { return len(b.hostnames) }

func (b *RemoteClusterBackend) Start() error {
	if len(b.hostnames) == 0 {
		return fmt.Errorf("no hostnames configured")
	}
	seed := b.hostnames[0]
	for i, host := range b.hostnames {
		// Wipe any stale container/volume from previous runs.
		_, _ = b.ssh.Exec(host, "docker rm -f cassandra >/dev/null 2>&1 || true")
		_, _ = b.ssh.Exec(host, "docker volume rm cassandra-data-"+host+" >/dev/null 2>&1 || true")

		runCmd := fmt.Sprintf(
			"docker run -d --name cassandra "+
				"-e CASSANDRA_SEEDS=%s "+
				"-e CASSANDRA_CLUSTER_NAME=%s "+
				"-e CASSANDRA_DC=%s "+
				"-e CASSANDRA_ENDPOINT_SNITCH=GossipingPropertyFileSnitch "+
				"-e MAX_HEAP_SIZE=%s "+
				"-e HEAP_NEWSIZE=%s "+
				"-p 9042:9042 -p 7000:7000 -p 7001:7001 -p 7199:7199 "+
				"-v cassandra-data-%s:/var/lib/cassandra "+
				"--cpus=8 --memory=32g "+
				"cassandra:5",
			seed, b.cluster, b.dc, b.heap, b.newGen, host,
		)
		out, err := b.ssh.Exec(host, runCmd)
		if err != nil {
			return fmt.Errorf("start node %d (%s): %w (out: %s)", i, host, err, out)
		}
		// Stagger seed bootstrap so node 0 has time to come up before others
		// try to gossip with it.
		if i == 0 {
			time.Sleep(45 * time.Second)
		} else {
			time.Sleep(15 * time.Second)
		}
	}
	return nil
}

func (b *RemoteClusterBackend) Stop() error {
	var firstErr error
	for _, host := range b.hostnames {
		_, err := b.ssh.Exec(host, "docker rm -f cassandra >/dev/null 2>&1 || true; docker volume rm cassandra-data-"+host+" >/dev/null 2>&1 || true")
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (b *RemoteClusterBackend) WaitForReady() error {
	return WaitForRing(b, len(b.hostnames), 6*time.Minute)
}

func (b *RemoteClusterBackend) ExecOnNode(i int, cmd string) (string, error) {
	if i < 0 || i >= len(b.hostnames) {
		return "", fmt.Errorf("node index %d out of range", i)
	}
	wrapped := "docker exec cassandra " + cmd
	return b.ssh.Exec(b.hostnames[i], wrapped)
}

func (b *RemoteClusterBackend) CopyToNode(i int, src, dst string) error {
	if i < 0 || i >= len(b.hostnames) {
		return fmt.Errorf("node index %d out of range", i)
	}
	tmp := "/tmp/" + filepathBase(src)
	if err := b.ssh.Copy(b.hostnames[i], src, tmp); err != nil {
		return err
	}
	cp := fmt.Sprintf("docker cp %s cassandra:%s", tmp, dst)
	if _, err := b.ssh.Exec(b.hostnames[i], cp); err != nil {
		return err
	}
	return nil
}

func filepathBase(p string) string {
	if i := strings.LastIndex(p, "/"); i >= 0 {
		return p[i+1:]
	}
	return p
}

func (b *RemoteClusterBackend) NodeAddresses() []string {
	out := make([]string, len(b.hostnames))
	copy(out, b.hostnames)
	return out
}

func (b *RemoteClusterBackend) NodeContainerIDs() ([]string, error) {
	ids := make([]string, len(b.hostnames))
	for i, host := range b.hostnames {
		out, err := b.ssh.Exec(host, "docker inspect --format '{{.Id}}' cassandra")
		if err != nil {
			return nil, err
		}
		ids[i] = strings.TrimSpace(out)
	}
	return ids, nil
}
```

- [ ] **Step 3: Run the compliance test**

Run: `go test ./internal/cluster/ -run TestRemoteClusterImplementsBackend -v`

Expected: PASS.

- [ ] **Step 4: Build everything**

Run: `go build ./...`

Expected: clean.

- [ ] **Step 5: Pause for review.** Real validation happens once you have Taurus access; for now we've proven the code compiles and implements the contract.

---

## Phase 5 — Multi-node metrics aggregation

**Why fifth:** The runner needs to collect metrics from N nodes and roll them up. Today's `metrics.go` assumes one container.

### Task 5.1: `nodetool tablestats` aggregation

**Files:**
- Modify: `internal/benchmark/cassandra/metrics.go`
- Test: `internal/benchmark/cassandra/metrics_test.go` (extend)

- [ ] **Step 1: Write the failing aggregation test**

Append to `metrics_test.go`:
```go
func TestAggregateTableStats(t *testing.T) {
	a := &CassandraMetricsSnapshot{
		SSTableCount:        4,
		SpaceUsedLive:       100 * 1024 * 1024,
		SpaceUsedTotal:      120 * 1024 * 1024,
		MemtableSwitchCount: 7,
		BloomFilterFP:       42,
	}
	b := &CassandraMetricsSnapshot{
		SSTableCount:        6,
		SpaceUsedLive:       200 * 1024 * 1024,
		SpaceUsedTotal:      240 * 1024 * 1024,
		MemtableSwitchCount: 9,
		BloomFilterFP:       8,
	}
	got := AggregateSnapshots([]*CassandraMetricsSnapshot{a, b})
	if got.SSTableCount != 10 {
		t.Errorf("SSTableCount: got %d want 10", got.SSTableCount)
	}
	if got.SpaceUsedLive != 300*1024*1024 {
		t.Errorf("SpaceUsedLive: got %d want %d", got.SpaceUsedLive, 300*1024*1024)
	}
	if got.MemtableSwitchCount != 16 {
		t.Errorf("MemtableSwitchCount: got %d want 16", got.MemtableSwitchCount)
	}
	if got.BloomFilterFP != 50 {
		t.Errorf("BloomFilterFP: got %d want 50", got.BloomFilterFP)
	}
}
```

- [ ] **Step 2: Run to verify failure**

Run: `go test ./internal/benchmark/cassandra/ -run TestAggregateTableStats -v`

Expected: FAIL — function doesn't exist.

- [ ] **Step 3: Implement `AggregateSnapshots`**

```go
// AggregateSnapshots sums per-node metrics into a single cluster-wide snapshot.
// For ratio fields (BloomFilterFPRatio, KeyCacheHitRate) it reports the mean.
func AggregateSnapshots(snaps []*CassandraMetricsSnapshot) *CassandraMetricsSnapshot {
	out := &CassandraMetricsSnapshot{}
	if len(snaps) == 0 {
		return out
	}
	var (
		fpRatioSum   float64
		keyHitSum    float64
		ratioCount   int
	)
	for _, s := range snaps {
		out.SSTableCount += s.SSTableCount
		out.SpaceUsedLive += s.SpaceUsedLive
		out.SpaceUsedTotal += s.SpaceUsedTotal
		out.MemtableSwitchCount += s.MemtableSwitchCount
		out.BloomFilterFP += s.BloomFilterFP
		out.CompactionBytesTotal += s.CompactionBytesTotal
		fpRatioSum += s.BloomFilterFPRatio
		keyHitSum += s.KeyCacheHitRate
		ratioCount++
	}
	if ratioCount > 0 {
		out.BloomFilterFPRatio = fpRatioSum / float64(ratioCount)
		out.KeyCacheHitRate = keyHitSum / float64(ratioCount)
	}
	return out
}
```

- [ ] **Step 4: Run the test**

Run: `go test ./internal/benchmark/cassandra/ -run TestAggregateTableStats -v`

Expected: PASS.

- [ ] **Step 5: Add `CaptureMetricsBeforeAll` and `MeasureMetricsAll` that work across nodes**

```go
import "github.com/moguls753/uuid-benchmark/internal/cluster" // adjust path

func (c *CassandraBenchmarker) CaptureMetricsBeforeAll(b cluster.Backend) error {
	snaps := make([]*CassandraMetricsSnapshot, b.NodeCount())
	for i := 0; i < b.NodeCount(); i++ {
		out, err := b.ExecOnNode(i, "nodetool tablestats "+c.cfg.Keyspace+"."+c.tableName)
		if err != nil {
			return fmt.Errorf("nodetool on node %d: %w", i, err)
		}
		s, err := parseTableStats(out)
		if err != nil {
			return fmt.Errorf("parse tablestats node %d: %w", i, err)
		}
		snaps[i] = s
	}
	c.metricsBefore = AggregateSnapshots(snaps)
	return nil
}

func (c *CassandraBenchmarker) MeasureMetricsAll(b cluster.Backend) (*benchmark.BenchmarkResult, error) {
	snaps := make([]*CassandraMetricsSnapshot, b.NodeCount())
	for i := 0; i < b.NodeCount(); i++ {
		out, err := b.ExecOnNode(i, "nodetool tablestats "+c.cfg.Keyspace+"."+c.tableName)
		if err != nil {
			return nil, fmt.Errorf("nodetool on node %d: %w", i, err)
		}
		s, err := parseTableStats(out)
		if err != nil {
			return nil, fmt.Errorf("parse tablestats node %d: %w", i, err)
		}
		snaps[i] = s
	}
	after := AggregateSnapshots(snaps)
	// Build a BenchmarkResult relative to c.metricsBefore as before.
	return buildBenchmarkResult(c.metricsBefore, after), nil
}
```

- [ ] **Step 6: Extract `buildBenchmarkResult` from the existing single-node `MeasureMetrics`**

The current `MeasureMetrics` in `metrics.go` builds a `*benchmark.BenchmarkResult` from `c.metricsBefore` and a freshly-captured snapshot inline. Extract that assembly logic into a private package-level helper:

```go
// buildBenchmarkResult assembles a BenchmarkResult from before/after snapshots.
// Shared between single-node MeasureMetrics and multi-node MeasureMetricsAll.
func buildBenchmarkResult(before, after *CassandraMetricsSnapshot) *benchmark.BenchmarkResult {
    // Move here whatever the existing MeasureMetrics does inline:
    //   - Compute deltas (SSTableCount, SpaceUsed*, CompactionBytesTotal, …)
    //   - Compute ratios (BloomFilterFPRatio, KeyCacheHitRate)
    //   - Wrap into the BenchmarkResult struct
}
```

Update the existing `MeasureMetrics` to call `buildBenchmarkResult` instead of doing the assembly inline. Both `MeasureMetrics` (single-node) and `MeasureMetricsAll` (multi-node, after `AggregateSnapshots`) now share the same assembly path.

**Cleanup note for after Phase 6:** once Phase 6 wires `LocalSingleBackend` everywhere, single-node mode also goes through `CaptureMetricsBeforeAll` / `MeasureMetricsAll` (a 1-node loop is fine). At that point the original `CaptureMetricsBefore` / `MeasureMetrics` become dead code and can be deleted, and the `*All` suffix can be dropped from the multi-node variants. Track this as a follow-up; don't delete during Phase 5 because the single-node path still uses the old methods until Phase 6 lands.

- [ ] **Step 7: Run all tests in the package**

Run: `go test ./internal/benchmark/cassandra/ -v`

Expected: PASS.

- [ ] **Step 8: Pause for review.**

### Task 5.2: Multi-node IO metrics

**Files:**
- Modify: `internal/benchmark/io/io_metrics.go` (`package docker`)
- Test: `internal/benchmark/io/io_metrics_test.go` (create)

**Note on package naming:** This file's path is `internal/benchmark/io/` but its package declaration is `package docker`. Existing callers import it with the alias `iometrics`. Within `io_metrics.go`, type references are unqualified (`IOStats`, `NodeRef` — no prefix). Outside callers (in Task 6.1) use `iometrics.IOStats`, `iometrics.NodeRef`, etc.

The current implementation has parsing inlined in `GetContainerIOStats`. We need to factor out `parseIOStatContent` so both local-cgroup reads and SSH-cat-the-file reads can share parsing logic.

- [ ] **Step 1 (RED): Write a failing test for `parseIOStatContent`**

```go
package docker

import "testing"

func TestParseIOStatContent(t *testing.T) {
	t.Run("multi-device sums correctly", func(t *testing.T) {
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
		input := "garbage line\n8:0 rbytes=100\n"
		got, err := parseIOStatContent(input)
		if err != nil {
			t.Fatal(err)
		}
		if got.ReadBytes != 100 {
			t.Errorf("ReadBytes: got %d want 100", got.ReadBytes)
		}
	})
}
```

Run: `go test ./internal/benchmark/io/ -run TestParseIOStatContent -v`

Expected: build error — `parseIOStatContent` doesn't exist.

- [ ] **Step 2: Extract `parseIOStatContent`** — move the inner loop of the current `GetContainerIOStats` into a pure string-parser function:

```go
// parseIOStatContent parses the cgroup v2 io.stat format into an IOStats.
// Each line is `<major>:<minor> rbytes=X wbytes=Y rios=Z wios=W`.
// Multi-device totals are summed.
func parseIOStatContent(content string) (*IOStats, error) {
	stats := &IOStats{Timestamp: time.Now()}
	for _, line := range strings.Split(content, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		for _, field := range fields[1:] {
			parts := strings.SplitN(field, "=", 2)
			if len(parts) != 2 {
				continue
			}
			value, err := strconv.ParseUint(parts[1], 10, 64)
			if err != nil {
				continue
			}
			switch parts[0] {
			case "rbytes":
				stats.ReadBytes += value
			case "wbytes":
				stats.WriteBytes += value
			case "rios":
				stats.ReadOps += value
			case "wios":
				stats.WriteOps += value
			}
		}
	}
	return stats, nil
}
```

- [ ] **Step 3: Extract `readIOStatFile(path)`** — wraps file open + parseIOStatContent:

```go
func readIOStatFile(path string) (*IOStats, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", path, err)
	}
	return parseIOStatContent(string(data))
}
```

- [ ] **Step 4: Refactor existing `GetContainerIOStats` to use the new helpers**

The existing function (lines 32-96 in current `io_metrics.go`) becomes:
```go
func GetContainerIOStats(containerName string) (*IOStats, error) {
	cgroupPath, err := findContainerCgroupPath(containerName)
	if err != nil {
		return nil, fmt.Errorf("failed to find container cgroup: %w", err)
	}
	return readIOStatFile(cgroupPath + "/io.stat")
}
```

(Delete the inline scanner + parsing loop. The `bufio` import becomes unused — remove it.)

- [ ] **Step 5: Run the parser test (GREEN)**

Run: `go test ./internal/benchmark/io/ -run TestParseIOStatContent -v`

Expected: PASS.

- [ ] **Step 6: Run a quick smoke test against existing single-node** to confirm `GetContainerIOStats` still works with the refactor:

```bash
go build -o uuid-benchmark cmd/benchmark/main.go
./uuid-benchmark -database=cassandra -scenario=insert-performance \
    -num-records=1000 -batch-size=100 -connections=2
```

Expected: completes, IO metrics in output are non-zero (proving the refactored read path works).

- [ ] **Step 7: Add `NodeRef`, `GetClusterIOStats`, and `getLocalIOStatsByID`/`getRemoteIOStats`**

Append to `io_metrics.go`:

```go
import (
	// ... existing imports
	"github.com/moguls753/uuid-benchmark/internal/remote"
)

// NodeRef identifies a Cassandra node for IO metrics collection. For nodes
// running on the orchestrator's host (local-single, local-cluster modes)
// Host is empty or "localhost" and we read cgroup directly. For remote nodes
// we read the cgroup file via SSH.
type NodeRef struct {
	Host        string // empty / "localhost" for local
	ContainerID string
}

// GetClusterIOStats sums IO across the given nodes. Returns one aggregated
// IOStats representing total cluster IO at the moment of capture.
func GetClusterIOStats(refs []NodeRef, sshUser, sshKey string) (*IOStats, error) {
	if len(refs) == 0 {
		return &IOStats{Timestamp: time.Now()}, nil
	}
	totals := &IOStats{Timestamp: time.Now()}
	for _, r := range refs {
		var stats *IOStats
		var err error
		if r.Host == "" || r.Host == "localhost" {
			stats, err = getLocalIOStatsByID(r.ContainerID)
		} else {
			stats, err = getRemoteIOStats(r.Host, r.ContainerID, sshUser, sshKey)
		}
		if err != nil {
			return nil, fmt.Errorf("io stats for %s/%s: %w", r.Host, r.ContainerID, err)
		}
		totals.ReadBytes += stats.ReadBytes
		totals.WriteBytes += stats.WriteBytes
		totals.ReadOps += stats.ReadOps
		totals.WriteOps += stats.WriteOps
	}
	return totals, nil
}

func getLocalIOStatsByID(containerID string) (*IOStats, error) {
	candidates := []string{
		fmt.Sprintf("/sys/fs/cgroup/system.slice/docker-%s.scope/io.stat", containerID),
		fmt.Sprintf("/sys/fs/cgroup/docker/%s/io.stat", containerID),
	}
	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return readIOStatFile(path)
		}
	}
	return nil, fmt.Errorf("no cgroup io.stat found for container %s", containerID)
}

func getRemoteIOStats(host, containerID, user, key string) (*IOStats, error) {
	client := remote.NewClient(user, key)
	candidates := []string{
		fmt.Sprintf("/sys/fs/cgroup/system.slice/docker-%s.scope/io.stat", containerID),
		fmt.Sprintf("/sys/fs/cgroup/docker/%s/io.stat", containerID),
	}
	var lastErr error
	for _, path := range candidates {
		out, err := client.Exec(host, "cat "+path)
		if err == nil {
			return parseIOStatContent(out)
		}
		lastErr = err
	}
	return nil, fmt.Errorf("could not read cgroup io.stat for container %s on %s: %w", containerID, host, lastErr)
}
```

- [ ] **Step 8: Add an interface compliance smoke test for `NodeRef` usage**

```go
func TestGetClusterIOStatsEmpty(t *testing.T) {
	got, err := GetClusterIOStats(nil, "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.ReadBytes != 0 || got.WriteBytes != 0 {
		t.Errorf("empty cluster expected zero stats, got %+v", got)
	}
}
```

- [ ] **Step 9: Run all tests in the io package**

Run: `go test ./internal/benchmark/io/ -v`

Expected: PASS.

- [ ] **Step 10: Pause for review.**

---

## Phase 6 — Runner & CLI integration

**Why sixth:** Wires everything up so the user can actually run a multi-node benchmark.

### Task 6.1: Runner accepts a `Backend` and `ClusterConfig`

**Files:**
- Modify: `internal/runner/cassandra.go`

- [ ] **Step 1: Change the `CassandraInsertPerformance` signature**

```go
func CassandraInsertPerformance(
	keyType string,
	numRecords, batchSize, connections int,
	cfg cluster.ClusterConfig,
	backend cluster.Backend,
) (*benchmark.InsertPerformanceResult, error) {
	bench := cassandra.New(cfg)
	if err := bench.Connect(); err != nil {
		return nil, err
	}
	defer bench.Close()
	if err := bench.CreateTable(keyType); err != nil {
		return nil, err
	}
	if err := bench.CaptureMetricsBeforeAll(backend); err != nil {
		return nil, err
	}

	ids, err := backend.NodeContainerIDs()
	if err != nil {
		return nil, err
	}
	hostsForIO := buildNodeRefs(cfg, ids)
	ioBefore, _ := iometrics.GetClusterIOStats(hostsForIO, cfg.SSHUser, cfg.SSHKeyPath)

	// Inserts use comma-separated contact points; ExecutionMode chosen by cfg.Mode
	connStr := strings.Join(backend.NodeAddresses(), ",")
	execMode := workload.ExecutionModeNative
	if cfg.Mode == cluster.ModeLocalSingle {
		execMode = workload.ExecutionModeContainer
	}
	wlResult, err := bench.InsertRecords(keyType, numRecords, batchSize, connections, connStr, execMode)
	if err != nil {
		return nil, err
	}

	ioAfter, _ := iometrics.GetClusterIOStats(hostsForIO, cfg.SSHUser, cfg.SSHKeyPath)
	ioMetrics := iometrics.CalculateIOMetrics(ioBefore, ioAfter)

	bm, err := bench.MeasureMetricsAll(backend)
	if err != nil {
		return nil, err
	}
	return assembleInsertResult(wlResult, bm, ioMetrics), nil
}

func buildNodeRefs(cfg cluster.ClusterConfig, ids []string) []iometrics.NodeRef {
	if cfg.Mode == cluster.ModeRemoteCluster {
		out := make([]iometrics.NodeRef, len(cfg.Hostnames))
		for i, h := range cfg.Hostnames {
			out[i] = iometrics.NodeRef{Host: h, ContainerID: ids[i]}
		}
		return out
	}
	out := make([]iometrics.NodeRef, len(ids))
	for i, id := range ids {
		out[i] = iometrics.NodeRef{Host: "", ContainerID: id}
	}
	return out
}
```

- [ ] **Step 2: Apply the same pattern to `CassandraReadPerformance`, `CassandraUpdatePerformance`, `CassandraMixedWorkloadInsertHeavy`, `CassandraMixedWorkloadReadUpdate`** — each gets `cfg` and `backend` parameters, builds `connStr` from `backend.NodeAddresses()`, and uses `bench.MeasureMetricsAll`/`CaptureMetricsBeforeAll` instead of the single-node variants.

- [ ] **Step 3: Update `cassandra.InsertRecords`/`ReadRecords`/`UpdateRecords`/`RunMixedWorkload`** in `internal/benchmark/cassandra/{insert,read,update,mixed}.go` to take both a `connString string` and `execMode workload.ExecutionMode` parameter, then build the `ExecutorConfig` accordingly:

```go
func (c *CassandraBenchmarker) InsertRecords(keyType string, numRecords, batchSize, connections int, connString string, execMode workload.ExecutionMode) (*workload.WorkloadResult, error) {
	if err := c.CaptureMetricsBefore(); err != nil {
		fmt.Printf("Warning: Could not capture metrics before insert: %v\n", err)
	}
	cfg := workload.ExecutorConfig{
		Mode:             execMode,
		DBType:           "cassandra",
		Op:               "insert",
		KeyType:          keyType,
		NumRecords:       numRecords,
		BatchSize:        batchSize,
		Threads:          connections,
		ConnectionString: connString,
	}
	if execMode == workload.ExecutionModeContainer {
		cfg.ContainerName = ContainerName
	}
	return workload.Execute(cfg)
}
```

Apply the same pattern to `ReadRecords`, `UpdateRecords`, and `RunMixedWorkload`. The `ContainerName` is only set in container mode; in native mode it's unused. Each method should also set `NumBuckets: c.numBuckets` (the field added in Task 1.2).

- [ ] **Step 4: Remove the now-dead `WorkloadConnString` constant**

Once every call site passes `connString` explicitly via the new method parameter, the `WorkloadConnString = "127.0.0.1"` constant in `internal/benchmark/cassandra/connection.go` is unused. Delete it. Verify with `grep -rn WorkloadConnString internal/` — expect zero matches.

- [ ] **Step 5: Build to confirm**

Run: `go build ./...`

Expected: clean.

- [ ] **Step 6: Pause for review.**

### Task 6.2: New CLI flags

**Files:**
- Modify: `cmd/benchmark/main.go`

- [ ] **Step 1: Add new flags near the existing flag block (lines 79-88)**

```go
clusterMode := flag.String("cluster-mode", "local-single", "Cluster mode: local-single, local-cluster, remote-cluster")
nodes := flag.String("nodes", "", "Comma-separated hostnames for remote-cluster mode (e.g. taurus5,taurus6,taurus7)")
sshUser := flag.String("ssh-user", "", "SSH user for remote-cluster mode")
sshKey := flag.String("ssh-key", "", "SSH private key path (default: ~/.ssh/id_ed25519)")
replicationFactor := flag.Int("replication-factor", 0, "Replication factor (default: 1 for local-single, 3 for cluster modes)")
consistency := flag.String("consistency", "", "Consistency level: local_one, local_quorum, quorum (default: local_one for single, local_quorum for cluster)")
clusterNodeCount := flag.Int("cluster-nodes", 3, "Number of nodes for local-cluster mode")
```

(`-num-buckets` was already added in Task 1.2 — it lives next to these as a Cassandra-specific tuning knob, independent of cluster mode.)

- [ ] **Step 2: Build a `ClusterConfig` from flags**

```go
func buildClusterConfig(
	mode, nodesStr, sshUser, sshKey, consistency string,
	rf, localNodeCount int,
) (cluster.ClusterConfig, error) {
	cfg := cluster.ClusterConfig{
		Keyspace: "uuid_benchmark",
		Mode:     cluster.Mode(mode),
	}
	switch cfg.Mode {
	case cluster.ModeLocalSingle:
		cfg.ContactPoints = []string{"127.0.0.1"}
		if rf == 0 {
			rf = 1
		}
		if consistency == "" {
			consistency = "local_one"
		}
	case cluster.ModeLocalCluster:
		cfg.ContactPoints = []string{"127.0.0.1"}
		// All nodes share host 127.0.0.1; gocql will discover the rest via gossip.
		// The contact-point list is a seed for the driver, not the node count.
		// rf must still be <= number of actual cluster nodes; we'll relax the
		// validate check by allowing the local-cluster mode to set rf based on
		// node count rather than contact-point count.
		if rf == 0 {
			rf = 3
		}
		if consistency == "" {
			consistency = "local_quorum"
		}
	case cluster.ModeRemoteCluster:
		hosts := strings.Split(nodesStr, ",")
		for i, h := range hosts {
			hosts[i] = strings.TrimSpace(h)
		}
		cfg.ContactPoints = hosts
		cfg.Hostnames = hosts
		cfg.SSHUser = sshUser
		cfg.SSHKeyPath = sshKey
		if rf == 0 {
			rf = 3
		}
		if consistency == "" {
			consistency = "local_quorum"
		}
	default:
		return cfg, fmt.Errorf("unknown cluster mode %q", mode)
	}
	cfg.ReplicationFactor = rf
	cfg.Consistency = consistency
	if err := cfg.Validate(); err != nil {
		return cfg, err
	}
	return cfg, nil
}
```

- [ ] **Step 3: Build a backend from the cfg**

(Note: `Validate` was already written mode-conditional in Task 2.1 — no further changes needed here. LocalSingle/LocalCluster modes use one published contact point with RF up to the actual cluster size; only RemoteCluster enforces RF ≤ len(Hostnames). See Task 2.1 for the test cases.)

```go
func buildBackend(cfg cluster.ClusterConfig, localNodeCount int) cluster.Backend {
	switch cfg.Mode {
	case cluster.ModeLocalSingle:
		return cluster.NewLocalSingle()
	case cluster.ModeLocalCluster:
		return cluster.NewLocalCluster(localNodeCount)
	case cluster.ModeRemoteCluster:
		return cluster.NewRemoteCluster(cfg)
	}
	return nil
}
```

- [ ] **Step 4: Wire backend into the orchestration loop** — replace the existing `container.Start(cassandraConfig)` call with `backend.Start(); backend.WaitForReady()`, and replace `container.Stop` with `backend.Stop()`. The existing per-key-type fresh-container loop stays the same; just the lifecycle calls change.

Specifically, in the `runScenario` loop, when `dbConfig.id == "cassandra"`, branch to use the backend:

```go
if dbConfig.id == "cassandra" {
	backend := buildBackend(clusterCfg, *clusterNodeCount)
	if err := backend.Start(); err != nil {
		log.Fatalf("backend start: %v", err)
	}
	if err := backend.WaitForReady(); err != nil {
		log.Fatalf("backend wait: %v", err)
	}
	defer backend.Stop()
	// ... call runner with (clusterCfg, backend)
}
```

- [ ] **Step 5: Update the `dbConfig` struct** so the Cassandra runner functions match the new signatures, OR keep the type-erased function signature and adapt at the call site. Easiest is to wrap the runners in closures that capture `clusterCfg` and `backend`:

```go
insertFunc: func(keyType string, numRecords, batchSize, connections int) (*benchmark.InsertPerformanceResult, error) {
	return runner.CassandraInsertPerformance(keyType, numRecords, batchSize, connections, clusterCfg, backend)
},
```

- [ ] **Step 6: Build and run a single-mode benchmark to confirm regression-free**

```bash
go build -o uuid-benchmark cmd/benchmark/main.go
./uuid-benchmark -database=cassandra -scenario=insert-performance \
    -num-records=1000 -batch-size=100 -connections=2 \
    -cluster-mode=local-single
```

Expected: completes successfully for all UUID types.

- [ ] **Step 7: Pause for review.**

### Task 6.3: Local-cluster smoke test

**Files:** (none modified)

- [ ] **Step 1: Run a smoke benchmark in `local-cluster` mode**

```bash
./uuid-benchmark -database=cassandra -scenario=insert-performance \
    -num-records=10000 -batch-size=100 -connections=4 \
    -cluster-mode=local-cluster -cluster-nodes=3 \
    -replication-factor=3 -consistency=local_quorum
```

Expected:
- Three containers come up (`uuid-bench-cassandra-1/2/3`)
- Ring forms (you can verify with `docker exec uuid-bench-cassandra-1 nodetool status`)
- Inserts succeed (recall the networking caveat at the top of Phase 3 — all writes route through `cassandra-1` as coordinator, then replicate internally)
- After completion, `nodetool tablestats` on each node shows replicated data (NOT 1/3 each; with RF=3 every node has every row). Per-node load may still differ because cassandra-1 carried the coordinator overhead.
- Containers and volumes are torn down between UUID types

- [ ] **Step 2: If anything fails, debug.** The most likely issue is timing — the ring might not be fully ready when the workload starts. Increase `WaitForRing` timeout or add a sleep buffer if needed.

- [ ] **Step 3: Pause for review.** This is the validation milestone for everything except remote SSH. **Do not record performance numbers from local-cluster mode in the paper** — only code-correctness validation.

---

## Phase 7 — Documentation, validation, and Taurus deployment

**Why last:** Code is feature-complete after Phase 6. Phase 7 is the on-cluster validation that requires admin access.

### Task 7.1: Update `CLAUDE.md`

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add a "Cluster Modes" section** documenting the three modes, the new flags, and the architecture. Include:
  - When to use each mode
  - The schema change (`bucket` partition key now spread across N values via `bucket = FNV-1a(id) mod N`, replacing the thesis's `bucket=1` constant; the `PRIMARY KEY ((bucket), id)` DDL itself is unchanged from the thesis)
  - The `-num-buckets` CLI flag (default 1000) and how to choose N at different dataset scales
  - Replication strategy per mode
  - The fact that the workload binary now runs natively on the orchestrator for cluster modes (not inside a container)
  - Security note: SSH uses InsecureIgnoreHostKey because the cluster is on a private VPN

- [ ] **Step 2: Update the "Implementation Status" section** to list multi-node Cassandra as complete.

- [ ] **Step 3: Pause for review.**

### Task 7.2: Update `README.md` with multi-node example

**Files:**
- Modify: `README.md`

- [ ] **Step 1: Add a "Multi-Node Cassandra" section** showing the three invocations (local-single, local-cluster, remote-cluster) with sample output.

- [ ] **Step 2: Pause for review.**

### Task 7.3: Single-node regression sweep (NEW BASELINE, not a regression check)

**Files:** (none modified)

**⚠ Methodology note:** The schema change in Phase 1 is methodologically significant. The thesis used `((bucket=1), id)` with every row in one partition. Phase 1 keeps the same schema DDL — `PRIMARY KEY ((bucket), id)` with the UUID as clustering column — but spreads `bucket` across `N` values (default 1000) via `bucket = FNV-1a(id_bytes) mod N`. Within each bucket the UUID is still the clustering column, byte-sorted; across buckets, the cluster distributes via Murmur3 on the partition key. See "Schema Design Methodology" near the top of the plan for the full rationale. This affects:

- **MemTable structure** — same per-bucket sorted layout as thesis (UUID still drives byte-order sort), now `N` parallel structures instead of one
- **SSTable layout** — `N` partitions of ~`M/N` rows each instead of one `M`-row partition (i.e., partitions are now sized in Cassandra's recommended healthy range)
- **Compaction patterns** — small-partition compaction in parallel across the ring, instead of one wide-partition compaction
- **Read path** — same primary-key point lookups (`WHERE bucket = ? AND id = ?`), but now scattered across `N` partitions

Therefore this run is **not a regression check against the thesis numbers**. Thesis numbers and post-Phase-1 single-node numbers are not directly comparable: they measure different storage shapes (one giant partition vs many small ones). Treat this run as **establishing the new baseline** that distributed runs (Phase 7.4+) will be compared against. The thesis result that UUID type affects within-partition behavior is preserved by construction (the same dynamics happen within each bucket); the bucketed baseline plus distributed runs let us quantify how much the multi-node aggregate effect differs from the single-node bucketed effect.

For the paper, document this explicitly:

> "We modify the schema from the thesis by spreading the `bucket` partition key across `N=1000` values via `bucket = FNV-1a(id_bytes) mod N`, rather than the thesis's `bucket=1` constant. This preserves the thesis's UUID-as-clustering-column dynamics within each partition while enabling Cassandra's native distribution across the ring. We re-establish single-node baselines under the bucketed schema and compare them to multi-node results, both using the bucketed schema. Direct comparison to thesis numbers is not made, because the bucketed schema changes both cluster size and partition size relative to the thesis; only the bucketed single-node baseline is comparable to bucketed multi-node results."

- [ ] **Step 1: Run the new single-node baseline sweep**

```bash
./uuid-benchmark -database=cassandra -scenario=all \
    -num-records=100000 -num-ops=10000 -connections=4 -num-runs=3 \
    -output=results-single-node-new-schema.csv
```

Expected: completes for all 6 UUID types and all 5 scenarios.

- [ ] **Step 2: Sanity-check the relative ordering across UUID types** — even if absolute numbers differ from the thesis, the *ranking* (e.g. UUIDv7 ≥ ULID > UUIDv4 for inserts) should hold qualitatively. If random keys (UUIDv4) are now *faster* than time-ordered ones (UUIDv7), something is wrong with the implementation — investigate before continuing.

- [ ] **Step 3: Save this CSV as the comparison anchor** for Tasks 7.4 and 7.5. The multi-node runs are compared against THIS, not the thesis CSV.

- [ ] **Step 4: Pause for review.**

### Task 7.4: Taurus connectivity dry-run

**Files:** (none modified)

- [ ] **Step 1: Once admin grants SSH access, manually verify connectivity** before running anything via the orchestrator:

```bash
# From your laptop, via VPN:
ssh taurus4
# From taurus4:
ssh taurus5 docker ps
ssh taurus5 docker pull cassandra:5
ssh taurus6 docker ps
ssh taurus7 docker ps
ssh taurus5 nc -zv taurus6 7000  # Cassandra gossip port reachable?
ssh taurus5 nc -zv taurus6 9042  # CQL port reachable?
```

Expected: all commands succeed. If gossip port (7000) is firewalled, the cluster won't form — escalate to admin.

- [ ] **Step 2: Build the binary on taurus4**

```bash
# Copy the source tree to taurus4 (or git clone if available)
scp -r uuid-benchmark/ taurus4:~/
ssh taurus4
cd ~/uuid-benchmark
go build -o uuid-benchmark cmd/benchmark/main.go
go build -o workload cmd/workload/main.go
```

- [ ] **Step 3: Tiny remote run** (10K records, just to validate the wiring)

```bash
./uuid-benchmark -database=cassandra -scenario=insert-performance \
    -num-records=10000 -batch-size=100 -connections=4 \
    -cluster-mode=remote-cluster \
    -nodes=taurus5,taurus6,taurus7 \
    -ssh-user=$USER \
    -replication-factor=3 -consistency=local_quorum
```

Expected: cluster comes up, ring forms within ~2 minutes, inserts complete, results print, tear-down succeeds. If cluster fails to form, check `nodetool status` on each node and look at `docker logs cassandra` on each host.

- [ ] **Step 4: Pause for review.** This is the green-light moment.

### Task 7.5: Production-scale runs

**Files:** (none modified — operational task)

- [ ] **Step 1: Plan a staged scaling test**

  - 1M records: validate end-to-end, all 5 scenarios, 3 runs per UUID type
  - 10M records: same, expect ~30-60 min per scenario
  - 100M records: same, plan for ~24-48h total wall-clock; verify SSD has enough headroom (see Task 7.6)

- [ ] **Step 2: Verify disk before 100M run**

```bash
ssh taurus5 df -h /var/lib/docker
ssh taurus6 df -h /var/lib/docker
ssh taurus7 df -h /var/lib/docker
```

Expected: at least 350GB free per node (rough estimate for 100GB raw × 1.5x overhead × 2x compaction headroom on a single UUID type, plus operating-room slack).

- [ ] **Step 3: Pause for review.** Operational decision point — once you've confirmed disk and connectivity, kick off the full sweep.

### Task 7.6: Update memory with multi-node decisions

**Files:**
- Update: `~/.claude/projects/-home-era-projects-uuid-benchmark/memory/MEMORY.md`

- [ ] **Step 1: Add memory entries** capturing decisions made during implementation:
  - Cluster topology for paper: 3 Cassandra nodes (taurus5/6/7), orchestrator on taurus4, RF=3, LOCAL_QUORUM
  - Schema change rationale: replaced thesis's `bucket=1` constant with `bucket = FNV-1a(id) mod N` (default N=1000) to enable real distribution across the ring while preserving the thesis's UUID-as-clustering-column dynamics within each bucket. Schema DDL itself is unchanged from the thesis.
  - Workload binary location decision: runs natively on orchestrator, not inside container, to mirror real client/server architecture

- [ ] **Step 2: Done.**

---

## Self-Review

This plan covers everything we discussed in the design conversation:

- ✅ Three cluster modes (local-single, local-cluster, remote-cluster) — Tasks 3.4, 3.5, 4.3
- ✅ Schema refactor (thesis's `((bucket), id)` DDL preserved; `bucket=1` constant replaced with `bucket = FNV-1a(id) mod N`) — Phase 1
- ✅ Configurable RF / consistency / contact points — Phase 2
- ✅ Workload executor supports container OR native execution — Task 2.4
- ✅ Local laptop testing path — Phase 3 (with documented networking caveat)
- ✅ Remote SSH backend for Taurus — Phase 4
- ✅ Multi-node metrics aggregation — Phase 5 (with explicit IO refactor)
- ✅ CLI integration & runner changes — Phase 6
- ✅ Validation & docs — Phase 7 (Task 7.3 framed as new baseline, not regression)

**Verified against the codebase:**
- Module path `github.com/moguls753/uuid-benchmark` matches `head -1 go.mod`
- Package alias `iometrics` for `internal/benchmark/io/` matches every existing caller
- `CassandraBenchmarker` struct fields (`session`, `keyType`, `tableName`, `metricsBefore`) match `internal/benchmark/cassandra/cassandra.go:7-12`
- `WorkloadConnString = "127.0.0.1"` constant (the loopback Cassandra connection used by container-mode workloads) is what current `internal/benchmark/cassandra/insert.go:23` passes through
- Existing test patterns (`t.Run` subtests with table-driven cases, fixture strings as package-level consts) match `internal/benchmark/cassandra/metrics_test.go`

**No placeholders detected.** Every task has actual code or actual commands. The "pause for review" instructions replace the skill's default `git commit` steps because the user handles git themselves.

**Type/signature consistency check:**
- `cluster.ClusterConfig` is defined in Task 2.1 and used consistently across Tasks 2.2, 4.3, 6.1, 6.2.
- `cluster.Backend` is defined in Task 3.3 and used in 3.4, 3.5, 4.3, 5.1, 6.1, 6.2.
- `CassandraMetricsSnapshot` is the existing struct (per exploration); `AggregateSnapshots` added in 5.1.
- `parseContactPoints` (Task 2.3) and `parseConsistency`/`replicationStmt` (Task 2.2) are co-located with their callers.
- `iometrics.NodeRef` is defined in Task 5.2 (added to `package docker` at `internal/benchmark/io/`) and used in Task 6.1's `buildNodeRefs`. The package is imported as `iometrics` per the established convention (verified with `grep -rn "internal/benchmark/io"` — every existing call site uses `iometrics "github.com/moguls753/uuid-benchmark/internal/benchmark/io"`).
- `workload.ExecutionMode` is defined in Task 2.4 and used in Tasks 6.1 (cfg-mode-driven branch) and the cassandra package's insert/read/update/mixed wrappers.

**Spec gaps:** None I can find. The conversation's open questions (Kubernetes vs. SSH, where to run the workload binary, single-vs-multi node coexistence) are all resolved in the plan.

---

## Notes for the executing engineer

1. **Module path is verified:** `github.com/moguls753/uuid-benchmark` (confirmed against `head -1 go.mod`). Use as-is in all import statements throughout the plan.

2. **IO metrics package quirk:** The path `internal/benchmark/io/` declares `package docker` (legacy from when it was Docker-stats-only). Existing callers use the alias `iometrics "github.com/moguls753/uuid-benchmark/internal/benchmark/io"`. New code in Phase 6 follows the same convention. Inside the package itself, types are unqualified (`IOStats`, `NodeRef`).

3. **Phase ordering matters:**
   - Phases 1, 2 (incl. 2.4), 3, 4, 5 can be coded entirely on a laptop with no external dependencies
   - Phase 3 cluster forms locally but with degraded routing (see networking caveat at top of Phase 3)
   - Phase 4 SSH code can be coded but only fully validated against Taurus
   - Phase 6 wiring needs Phases 2.4, 3, 4, 5 done first
   - Don't merge phases or skip the `pause for review` checkpoints

4. **Don't run git commands.** Each task ends with "pause for review" — that means stop and wait. The user commits.

5. **TDD discipline:** Every task that introduces new functions starts with a failing test (the RED step), then implementation, then a passing test (GREEN). Tasks 1.5, 2.4, 5.2 explicitly call out the RED step. Don't skip it for the others — write the test, run it (expect fail), then implement.

6. **Local-cluster mode is for code correctness only.** The networking caveat in Phase 3 means performance numbers from local-cluster mode are NOT representative. Never quote them in the paper. Use them only to verify "the multi-node code paths execute without errors."

7. **Phase 7.3 is a NEW baseline, not a regression check.** The schema change in Phase 1 alters storage semantics enough that thesis-era and post-Phase-1 single-node numbers are not directly comparable. The paper must say so.

8. **When the admin replies:** The blocking dependencies for Phase 7.4 are (a) SSH access from taurus4 to taurus5/6/7 with key auth, (b) Docker installed and user in docker group on each node, (c) ports 7000/7001/9042/7199 open between the nodes (NOT 9160 — that's legacy Thrift, not used in modern Cassandra). If any of these are missing, file a ticket before starting Phase 7.4.

9. **SSH security note for the paper:** `internal/remote/ssh.go` uses `ssh.InsecureIgnoreHostKey()`. Acceptable here because the FernUni VPN puts you behind their firewall before reaching the Taurus private network. Document this in CLAUDE.md (Task 7.1) and the methodology section of the paper.
