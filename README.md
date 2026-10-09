# UUID Benchmark

Benchmarks sequential integer keys, UUIDv1, UUIDv4, UUIDv7, ULID and monotonic ULID across **PostgreSQL 18, MySQL 8, MongoDB 8 and Cassandra 5**. MongoDB also includes ObjectId. Measures throughput, latency, storage, cache behavior and I/O, with engine-specific structural metrics.

The project includes the single-node thesis benchmarks, a **Cassandra multi-node extension**, and a separate corrected insert-heavy protocol. Historical results and current protocols are not automatically interchangeable.

## Quick start

Requires **Go 1.22+**, **Docker with Compose V1 ≥ 1.29 or V2**, and Linux with **cgroup v2** for container I/O metrics. Allow at least 8 GB RAM plus host overhead and 20 GB free disk for small local runs; large datasets and clusters need more. Initial setup needs Internet access to fetch images and build PostgreSQL extensions.

Run from the repository root:

```bash
go build -o uuid-benchmark cmd/benchmark/main.go

# Each scenario tests all supported key types automatically.
./uuid-benchmark -database=postgres -scenario=insert-performance \
    -num-records=100000 -connections=1 -num-runs=5 -output=results.csv

# Use mysql, mongodb or cassandra for the other engines.
./uuid-benchmark -help
```

A source ZIP works for normal benchmarks: without Git, provenance records
`git_provenance: "unavailable"`, an empty commit and unknown working-tree state;
binary hashes are still recorded. Use a Git checkout for source provenance.

## Options and scenarios

| Option | Default | Purpose |
|---|---|---|
| `-database` | `postgres` | `postgres`, `mysql`, `mongodb`, `cassandra` |
| `-scenario` | `insert-performance` | One scenario below, or `all` |
| `-num-records` | `100000` | Dataset size |
| `-num-ops` | `10000` | Read/update/mixed operation count |
| `-connections` | `1` | Concurrent workers |
| `-batch-size` | `100` | Batch size |
| `-num-runs` | `1` | Repetitions per key type |
| `-output` | none | CSV results in multi-run mode; provenance sidecars |
| `-campaign-seed` | `0` | Nonzero randomizes key-type order per repetition and seeds read-set sampling |

Scenarios: `insert-performance`, `read-performance`, `update-performance`,
`mixed-insert-heavy` (70% insert / 30% read), `mixed-read-update` (50% read /
50% update), or `all`. The historical `mixed-insert-heavy` scenario is **not**
the corrected IH protocol below. See `-help` for all flags.

## Cassandra: single-node and multi-node

Both are built into the same binary; select a mode with `-cluster-mode`:

| Mode | Deployment | Use |
|---|---|---|
| `local-single` (default) | One local container; workload inside it | Single-node runs |
| `local-cluster` | Three local containers; workload on orchestrator | Correctness checks only, not performance measurements |
| `remote-cluster` | Separate hosts via SSH; workload on orchestrator | Multi-node measurements |

PostgreSQL, MySQL and MongoDB remain single-node. The local Cassandra cluster
exposes only the seed's CQL port, so all workload queries use one coordinator.

```bash
# Local cluster: small correctness check, not a performance measurement.
./uuid-benchmark -database=cassandra -cluster-mode=local-cluster \
    -scenario=insert-performance -num-records=10000

# Remote cluster: dedicated hosts, reachable over SSH and CQL.
./uuid-benchmark -database=cassandra -cluster-mode=remote-cluster \
    -nodes=taurus-01:9042,taurus-02:9042,taurus-03:9042 \
    -ssh-user="$USER" -ssh-key="$HOME/.ssh/id_ed25519" \
    -scenario=insert-performance -num-records=1000000 -num-runs=3 \
    -campaign-seed=42 -output=cassandra-cluster.csv
```

**Remote prerequisites:** Docker accessible to the SSH user on every node,
image-registry access, SSH/CQL connectivity from the orchestrator, and internode
connectivity (port 7000). Use dedicated hosts on a trusted private network;
see [Safety](#safety) before running.

**Defaults and sizing:**
- Single-node uses RF1 / `local_one`; cluster modes use RF3 / `local_quorum`.
  Override with `-replication-factor` and `-consistency`. For two remote hosts,
  set RF to at most 2. One remote host requires `-single-node`, which defaults
  to RF1 but retains `local_quorum`.
- Each remote container defaults to `-cassandra-cpus=8`,
  `-cassandra-memory=32g`, `-cassandra-heap=8G`, `-cassandra-newgen=2G`.
  Adjust to host capacity; new generation must not exceed heap.
- Remote images default to `cassandra:5`, pulled at startup. Pin
  `-cassandra-image=cassandra@sha256:…` for a multi-day campaign.

**Comparison limits:** Normal Cassandra workloads use hash-based partition
buckets (`-num-buckets=1000`), even in single-node mode—not the historical fixed
partition. Read/update targets are now sampled uniformly during insertion;
`-head-sampling` restores the legacy per-partition-head fetch for bridge
comparisons. Network path, replication, partition count and sampling must be
accounted for when comparing results. Cluster metrics aggregate per-node
snapshots; see [paper notes](docs/paper-notes.md) for details.

## Corrected insert-heavy (IH)

The separate single-client IH protocol verifies the preload target pool,
successful reads, fresh inserts and final cardinality. Cassandra uses fixed
**bucket 1**, RF1, LOCAL_ONE and STCS; `-num-buckets=1` is not a substitute.

```bash
# Planning only; Python 3.10+. Does not start measurements.
python3 -B scripts/ih_campaign.py --mode=full --seed=42
python3 -B scripts/ih_repeat.py --help
```

Execution requires a Git checkout, locally available database images, a quiet
host and `--execute --host-ready`. Full mode normally requires pilots;
`--skip-pilot` records a bypass, not a passed gate. Repeats require named source
runs and a reason, reuse archived binaries and reject changed measurement sources.
See the [IH protocol and implementation](docs/plans/ih-corrected-implementation.md).

## Measurement and outputs

Each key type gets a fresh database container; cleanup removes its benchmark
data. PostgreSQL uses pgbench with server-side key generation; the other engines
use a shared Go workload binary with client-side generation. Workloads run inside
the database container **except in Cassandra cluster modes**.

Throughput, latency, disk size and cgroup I/O are collected alongside
engine-specific metrics: PostgreSQL/InnoDB/WiredTiger page splits, and Cassandra
SSTable count/delta and space amplification. **SSTable count is not a compaction
count.** Fragmentation and cache metrics have different engine-specific meanings;
do not treat them as identical measurements. Repeated runs support summary
statistics and Mann–Whitney U comparisons.

With `-output`, provenance is recorded in `<output>.meta.json` (flags,
source/binary identity, seeds and execution order); completed runs are appended
to `<output>.runs.jsonl`. Remote manifests include SSH usernames, key paths and
node addresses—review before sharing. CSV summaries are available in multi-run
mode. See [metrics methodology](docs/METRICS_METHODOLOGY.md) for definitions and
limitations.

## Safety

**Benchmark use only:** The supplied Docker Compose configurations publish
database ports on all host interfaces and use default benchmark credentials
(Cassandra without authentication). Run only on isolated or appropriately
firewalled hosts; do not expose these ports to the public Internet.

Use dedicated benchmark hosts: the normal runner removes benchmark containers
and volumes. Remote Cassandra also replaces containers named `cassandra` and
volumes named `cassandra-data-<host>`. SSH host-key verification is disabled for
remote clusters with ephemeral hosts; use only trusted private networks.

## Results and checks

- [Evidence dashboard and source documentation](docs/EVIDENCE.md)
- [YCSB validation](validation/README.md)
- PDF plots: `pip install -r scripts/requirements.txt`, then
  `python3 scripts/plot.py results.csv --output-dir plots/`

Offline tests (no database measurements):

```bash
go test -count=1 ./...
python3 -B -m unittest discover -s scripts -p 'test_ih*.py'
```
