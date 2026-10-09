# UUID Benchmark — project context

## Purpose

Benchmark sequential integer keys, UUIDv1, UUIDv4, UUIDv7, ULID and monotonic ULID
across PostgreSQL 18, MySQL 8, MongoDB 8 and Cassandra 5. MongoDB additionally
supports ObjectId. All four engines and five standard scenarios are implemented.
Cassandra supports single-node, local-cluster and remote-cluster deployments.

User-facing usage is in [README.md](README.md). Metric definitions are in
[docs/METRICS_METHODOLOGY.md](docs/METRICS_METHODOLOGY.md). Do not equate a
historical plan's status with current implementation or campaign status.

## Build and checks

```bash
go build -o uuid-benchmark cmd/benchmark/main.go
CGO_ENABLED=0 go build -o /tmp/uuid-workload-check cmd/workload/main.go
go test -count=1 ./...
python3 -B -m unittest discover -s scripts -p 'test_ih*.py'
```

Run from the repository root. The normal benchmark supports source ZIPs without
Git: metadata records unavailable Git provenance and still includes binary hashes.
The specialized IH campaign launcher requires a Git checkout for source archiving.

Use `./uuid-benchmark -help` for current flags/defaults. Each standard scenario
automatically runs all supported key types. `-num-runs` repeats each type;
`-output` writes CSV summaries in multi-run mode plus provenance sidecars.
`-campaign-seed=0` preserves fixed key-type order; nonzero seeds randomize order
per repetition and seed Cassandra read-set selection.

## Architecture

| Path | Responsibility |
|---|---|
| `cmd/benchmark/main.go` | CLI, execution order, lifecycle, provenance and export |
| `cmd/workload/main.go` | Shared MySQL/MongoDB/Cassandra workload and key generation |
| `internal/runner/` | Database-specific scenario orchestration and I/O windows |
| `internal/benchmark/postgres/pgbench/` | SQL generation, pgbench execution and log parsing |
| `internal/benchmark/{postgres,mysql,mongodb,cassandra}/` | Connections, workloads and metrics |
| `internal/benchmark/workload/` | Container/native execution and JSON parsing |
| `internal/benchmark/io/` | cgroup v2 snapshots, per-node deltas and rates |
| `internal/cluster/` | Cassandra backend/configuration, ring checks and lifecycle |
| `internal/remote/` | SSH/SCP client |
| `internal/ih/` | Separate corrected insert-heavy protocol and validation |
| `internal/export/`, `internal/display/` | CSV output and console tables |
| `internal/benchmark/statistics/` | Summary statistics and Mann–Whitney U tests |
| `scripts/ih_campaign.py`, `scripts/ih_repeat.py` | Explicitly scheduled IH campaigns/repeats |

Standard scenarios: insert, read, update, 70/30 insert/read and 50/50 read/update.
Each key type/repetition starts with a fresh database. Cleanup removes benchmark
containers and volumes. PostgreSQL uses pgbench and server-side generators;
other engines use the shared Go workload binary and client-side generators.
Normal local workloads execute inside the database container. Cassandra cluster
workloads execute natively on the orchestrator.

## Cassandra

- `local-single`: one container, default RF1 / `local_one`.
- `local-cluster`: three local containers, RF3 / `local_quorum`. Only the seed's
  CQL port is published to the host. Correctness validation, **not performance measurement**.
- `remote-cluster`: SSH-managed Docker containers on separate hosts, default
  RF3 / `local_quorum`. One host requires `-single-node` and defaults to RF1;
  consistency remains `local_quorum`. RF must not exceed remote host count.
- Single-node replication uses `SimpleStrategy`; cluster modes use
  `NetworkTopologyStrategy` with `dc1`.
- Remote defaults per container: 8 CPUs, 32g memory, 8G heap, 2G new generation.
  CLI overrides are remote-only. Local resources are defined in Compose files.
  Pin `-cassandra-image` by digest for reproducible multi-day campaigns.

### Schema and sampling

Normal workloads use `PRIMARY KEY ((bucket), id)` and
`bucket = FNV-1a(id_bytes) mod N`, default N=1000. IDs remain clustering keys;
Cassandra hashes the bucket for placement. UUIDv1 uses `timeuuid`, UUIDv4/v7
use `uuid`, ULIDs use `blob`, sequential integers use `bigint`.

Do not describe bucketed single-node execution as equivalent to the historical
fixed `bucket=1` runs. Partition size and sampling changed. `-num-buckets=1`
hashes to bucket 0, not historical bucket 1.

For current read/update workloads, insertion draws distinct positions uniformly
in one global sample, then splits them across writer ranges. Only IDs from
successful batches become targets. IDs are shuffled and written to a file;
insert and measured phases independently digest it and the runner verifies
matching hashes. Do not replace the global draw with fixed per-thread quotas:
unequal writer ranges would bias inclusion probabilities.

`-head-sampling` restores the legacy per-partition-head fetch for bridge
comparisons. It changes the target population, order and cache pre-warming.
Dataset preparation uses `runner.PrepInsertConnections` (8), not necessarily
the measured worker count. Details: [Cassandra notes](docs/paper-notes.md).

### Metrics

Per-node `nodetool tablestats`, `nodetool info` and cgroup v2 I/O snapshots are
collected via the backend. Storage/counters are summed; node cache ratios are
unweighted means. SSTable and Bloom-filter false-positive deltas are clamped
per node **before** summing. Preserve this distinction from clamp-after-sum.

The shared `PageSplits` field carries SSTable-count delta, not compaction work.
`LeafPages` carries SSTable count. `IndexSize=0` means not separately reported.
Cassandra's fragmentation percentage is `(total − live) / total × 100`, not
`total / live`. Storage includes replicas; key-cache hits are not row-cache hits.

## Engine-specific measurement cautions

- PostgreSQL uses `pgstattuple`, `pg_walinspect`, `uuid-ossp` and `pgx_ulid`.
  Disk sizes come from `pg_table_size` and `pg_indexes_size`.
- MySQL stores UUIDs as `BINARY(16)`. Its fragmentation proxy is the non-leaf
  allocated share from `innodb_index_stats`, including unused allocated pages.
  Leaf density is unavailable (`-1`), not an estimated 90%.
- MongoDB stores UUIDs as BSON Binary subtype 0x04, ULIDs as subtype 0x00.
  The split-related metric counts reconciliation leaf/internal multi-block writes,
  not in-memory cache splits. Leaf density/pages are unavailable (`-1`).
- Cache ratios, fragmentation proxies and storage totals are not identically
  defined across engines. Bulk-insert latencies can describe batches, not rows.
  I/O snapshot windows differ from timed-loop windows. Missing metrics can fall
  back to zero; inspect validity fields and warnings.
- A go-ycsb sequential-key comparison is limited evidence, not proof that every
  UUID workload and metric is correct.

## Corrected insert-heavy path

This is separate from the standard `-scenario=mixed-insert-heavy` path. It uses
one client, a verified fixed preload target pool, fresh inserts, checked read
hits and verified final cardinality. Cassandra uses fixed bucket 1, RF1,
LOCAL_ONE and STCS. Historical and corrected IH results are not interchangeable.

Launchers default to planning only. Execution needs explicit authorization,
`--execute --host-ready`, a quiet host and locally available images. Full runs
normally require pilots; `--skip-pilot` records a bypass, not a passed gate.
Repeats require named source runs and a reason, preserve archived binaries and
reject changed measurement sources. Retain original measurements and provenance.

## Dashboard and documentation

The active site loads `docs/assets/evidence.js`, `evidence.css` and
`docs/data/evidence.json`. The older Chart.js modules/data are legacy assets.
The builder is `scripts/build_evidence.py`, not `scripts/convert_results.py`.
It requires the companion paper checkout, NumPy and selected IH archive;
`--ih-results` overrides archive discovery. It does not discover arbitrary CSVs.

`docs/data/sources/` contains hashed source snapshots. Do not editorially rewrite
those files or change measurements while editing documentation or UI labels.
Historical designs/handoffs in `docs/plans/` are records, not current execution
orders. Dashboard source selection is described in [docs/EVIDENCE.md](docs/EVIDENCE.md).

## Operational safety

Use dedicated benchmark hosts. Default database credentials are public; Cassandra
has no authentication. Compose publishes ports on all interfaces. Remote startup
replaces containers named `cassandra` and volumes named `cassandra-data-<host>`.
SSH intentionally uses `ssh.InsecureIgnoreHostKey()` for ephemeral private-VPN
allocations; do not use remote mode on an untrusted network.

Do not start measurements, deploy, commit, push or remove unrelated resources
without explicit authorization. Preserve original logs, result files and source
snapshots. Documentation review alone does not authorize benchmark execution.
