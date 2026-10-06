# UUID Benchmark

## Project Purpose

Bachelor thesis benchmarking UUID performance (UUIDv1, UUIDv4, UUIDv7, ULID, ULID monotonic) vs sequential integer keys (BIGSERIAL/AUTO_INCREMENT). The thesis proposal (submitted to FernUniversität in Hagen) targets 4 database systems — PostgreSQL, MySQL, MongoDB, Cassandra — to address the research gap of missing cross-database and cross-architecture (B-tree vs LSM-tree) UUID performance evaluations. All 4 databases are implemented.

Focus on measuring:
- Page splits and index fragmentation (B-tree DBs) / compaction overhead (LSM-tree)
- Disk usage (table/index size)
- Query performance (throughput, latency percentiles p50/p95/p99)
- Memory efficiency (buffer pool / cache hit ratios)
- I/O metrics (IOPS, throughput via cgroup v2)

## Implementation Status

**Complete:**
- PostgreSQL 18 with pgbench integration (all 5 scenarios, all metrics)
- MySQL 8 with custom Go workload binary (all 5 scenarios, all metrics)
- MongoDB 8 with custom Go workload binary (all 5 scenarios, all metrics)
- Cassandra 5 with custom Go workload binary (all 5 scenarios, all metrics)
- Shared workload binary (`cmd/workload/main.go`) for MySQL/MongoDB/Cassandra with proper UUID generation via Go libraries
- Statistical analysis (Mann-Whitney U, median, mean, stddev, CV, multi-run mode)
- CSV export (summary stats + raw per-run data)
- Docker orchestration (fresh container per UUID type for isolation)
- YCSB validation (BIGSERIAL throughput/latency matches within ~8%)
- Multi-node Cassandra (paper extension): three deployment modes (single, local cluster, remote cluster), bucketed partition schema, cluster-wide metric aggregation, multi-node cgroup v2 I/O via SSH

## Remaining Roadmap

**Benchmark runs and thesis**
- Full benchmark runs at scale (1M, 10M, 100M records) across all 4 databases
- 3+ runs per configuration for statistical significance
- Data analysis, visualization, thesis writing

## Build & Run

```bash
# Build
go build -o uuid-benchmark cmd/benchmark/main.go

# Run all scenarios for each database
./uuid-benchmark -database=postgres -scenario=all -num-records=1000000 -num-ops=100000 -connections=10 -num-runs=5 -output=results.csv
./uuid-benchmark -database=mysql -scenario=all -num-records=1000000 -num-ops=100000 -connections=10 -num-runs=5 -output=results.csv
./uuid-benchmark -database=mongodb -scenario=all -num-records=1000000 -num-ops=100000 -connections=10 -num-runs=5 -output=results.csv
./uuid-benchmark -database=cassandra -scenario=all -num-records=1000000 -num-ops=100000 -connections=10 -num-runs=5 -output=results.csv

# Run individual scenario
./uuid-benchmark -database=postgres -scenario=insert-performance -num-records=100000 -batch-size=100
./uuid-benchmark -database=mongodb -scenario=read-performance -num-records=1000000 -num-ops=10000

# Cassandra multi-node (local 3-container cluster, code validation only)
./uuid-benchmark -database=cassandra -cluster-mode=local-cluster -scenario=insert-performance -num-records=10000

# Cassandra multi-node (remote 3-node cluster, paper measurements)
./uuid-benchmark -database=cassandra -cluster-mode=remote-cluster \
    -nodes=taurus-01:9042,taurus-02:9042,taurus-03:9042 \
    -ssh-user=$USER -ssh-key=$HOME/.ssh/id_ed25519 \
    -scenario=all -num-records=1000000 -num-runs=3 -output=cassandra-cluster.csv
```

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-database` | `postgres` | Database to test: `postgres`, `mysql`, `mongodb`, `cassandra` |
| `-scenario` | `insert-performance` | Scenario: `insert-performance`, `read-performance`, `update-performance`, `mixed-insert-heavy`, `mixed-read-update`, `all` |
| `-num-records` | `100000` | Dataset size for insert operations |
| `-num-ops` | `10000` | Number of operations for read/update/mixed scenarios |
| `-connections` | `1` | Concurrent workers |
| `-batch-size` | `100` | Records per transaction |
| `-num-runs` | `1` | Runs per UUID type for statistical analysis |
| `-output` | (none) | CSV file for statistical results (multi-run mode only) |
| `-cluster-mode` | `local-single` | Cassandra deployment: `local-single`, `local-cluster`, `remote-cluster` |
| `-cluster-nodes` | `3` | Node count for `local-cluster` mode |
| `-nodes` | (none) | Comma-separated `host[:port]` list for `remote-cluster` mode |
| `-ssh-user` | (none) | SSH user for `remote-cluster` mode |
| `-ssh-key` | (none) | SSH private key path for `remote-cluster` mode |
| `-replication-factor` | `1` (single) / `3` (cluster) | Cassandra keyspace RF |
| `-consistency` | `local_one` (single) / `local_quorum` (cluster) | gocql consistency: `one`, `local_one`, `local_quorum`, `quorum` |
| `-num-buckets` | `1000` | Partition-key bucket count for the bucketed Cassandra schema |
| `-cassandra-heap` | `8G` | `MAX_HEAP_SIZE` per remote Cassandra container (Taurus-sized default; lower for laptop validation) |
| `-cassandra-newgen` | `2G` | `HEAP_NEWSIZE` per remote Cassandra container (must be ≤ heap) |
| `-cassandra-cpus` | `8` | `docker --cpus` per remote container (docker rejects values > host CPU count) |
| `-cassandra-memory` | `32g` | `docker --memory` per remote container |
| `-campaign-seed` | `0` | Seed for key-type execution order and read-set sampling. `0` keeps the historical fixed order; any other value randomises the order per repetition (randomised block design) and is recorded in `<output>.meta.json` |
| `-head-sampling` | `false` | Cassandra: select read/update targets with the legacy per-partition-head fetch instead of drawing them uniformly during the insert. Bridge arm only |
| `-single-node` | `false` | Allow a `remote-cluster` run with exactly one node. Without it a single-entry `-nodes` list is rejected as a likely typo |
| `-cassandra-image` | `cassandra:5` | Image reference for `remote-cluster` mode. The default is a floating tag re-pulled before every container start; pin a digest for a multi-day campaign and it is recorded in `<output>.meta.json` |

**UUID Types Tested:** Sequential integer (BIGSERIAL/AUTO_INCREMENT/bigint), UUIDv1, UUIDv4, UUIDv7, ULID (non-monotonic), ULID (monotonic)

### Read/Update Target Selection (Cassandra)

The read and update scenarios need a set of existing ids to look up. Until
2026-09 that set was fetched from the database right before the timed loop
(`SELECT id FROM bench WHERE bucket = ? PER PARTITION LIMIT k`), which returns
the clustering-smallest ids per partition. That rule means different things per
key type: for every time-ordered type it selects the oldest rows, which sit
densely in old compacted SSTables and had just been warmed by the fetch itself,
while for UUIDv4 it selects rows spread across the whole dataset. The measured
difference therefore covered the key type and the sampler together, and no
decomposition is possible after the fact (the fetch also ran inside the I/O
snapshot window, with its duration unrecorded).

The insert phase now draws the target set instead: `num-ops` distinct insert
positions are drawn uniformly from `[0, num-records)` in one global draw, split
across the writer threads by their record ranges (a fixed share per thread
would give rows unequal inclusion probability whenever the threads own
different numbers of rows), and the ids generated at those positions are
collected, shuffled (seeded) and written to an id file that the read or update
phase loads. Only ids whose insert batch returned without error become targets.
Both phases digest the file independently and the runner compares the two
digests, so a read set that changed between them fails the run. That
makes the selection uniform over all rows and independent of key type, removes
the fetch from the measured window (`t_fetch_s` is 0 by construction), and
removes the pre-warming of exactly the rows about to be measured. The shuffle
matters on its own: in insert order a time-ordered key type would be read in
disk order.

`-head-sampling` restores the old fetch for the bridge arm that measures how
large the difference between the two samplers actually is. The dataset
bootstrap in the read and update scenarios runs at
`runner.PrepInsertConnections` writers (8); it is data preparation, not a
measurement, and at one writer it dominated the campaign's wall-clock time.

Per-run provenance lands in `<output>.meta.json` (commit, working-tree state,
md5 of both binaries, full flag dump, campaign seed, per-run seeds and
execution order) and every finished run is appended to `<output>.runs.jsonl`
as it completes.

## Cluster Modes (Cassandra)

The Cassandra runner supports three deployment modes via `-cluster-mode`, abstracted behind `internal/cluster/Backend`. PostgreSQL, MySQL, and MongoDB always run single-node.

| Mode | Topology | Purpose |
|------|----------|---------|
| `local-single` | One `cassandra:5` container on the orchestrator host | Default. The existing thesis methodology. Workload binary runs inside the container. |
| `local-cluster` | Three `cassandra:5` containers (`docker/docker-compose.cassandra-cluster.yml`) sharing one Docker network | Code-correctness validation for multi-node code paths (ring formation, multi-host metric aggregation, RF handling). **Not for performance measurement**: only `cassandra-1` publishes 9042 to the host, so gocql routes all queries through it as coordinator. |
| `remote-cluster` | Three real machines reached over SSH (Taurus in our setup) | Used for the paper extension's actual measurements. Workload binary runs natively on the orchestrator and talks to the remote ring directly over CQL. |

**Workload execution.** In `local-single` the workload binary is `docker cp`'d into the container and executed via `docker exec` (zero-network, the thesis baseline). In both cluster modes it runs natively on the orchestrator and connects over the network — there is no useful per-node container to inject it into, and measuring the cross-node path is the point of the cluster modes.

**Bucketed schema (paper extension).** The DDL `PRIMARY KEY ((bucket), id)` is unchanged from the thesis, but the thesis pinned `bucket = 1` (single partition, single node owns the data — fine for a one-node test). The cluster extension spreads writes across all nodes by computing `bucket = FNV-1a(id_bytes) mod N` per row. `N` is `-num-buckets` (default 1000). Choosing N: aim for partitions in the low-MB range. With ~1 KiB rows, 1000 buckets gives a ~1 MiB partition per million records — comfortable up to ~100M records; raise N for larger datasets. The single-node mode also uses the bucketed scheme so the code path is identical; with N=1000 and a single node, all buckets land on that node and behavior is equivalent to the thesis baseline.

**Replication.** `local-single` defaults to RF=1 (the keyspace lives on one node anyway). `local-cluster` and `remote-cluster` default to RF=3 with `SimpleStrategy` so every node holds every row, which is what the paper measures. `-replication-factor` overrides.

**Consistency.** `-consistency` is passed through to gocql. `local_one` is the gocql default and what the paper uses; raise to `quorum` to measure the coordination cost.

**Metric aggregation.** For cluster modes, per-node `nodetool` output and per-node cgroup v2 `io.stat` are collected from every node and combined: counters (SSTable count delta, bloom-filter false positives, IO bytes/ops) are summed with per-node clamp-then-sum semantics (so one node's compaction-induced counter decrease can't mask another node's workload-induced increase); ratios (cache hit rate, bloom filter false ratio) are unweighted means across nodes. See `docs/paper-notes.md` for the rationale.

**Security.** SSH to remote nodes uses `ssh.InsecureIgnoreHostKey()`. This is intentional: the Taurus cluster sits on a private VPN with ephemeral host keys, so strict host-key checking would just fail on every new allocation. Do not point `remote-cluster` at hosts on an untrusted network.

## Architecture

```
cmd/
├── benchmark/
│   └── main.go                          # CLI, orchestration loop, container lifecycle
└── workload/
    └── main.go                          # Standalone workload binary for MySQL/MongoDB/Cassandra
                                         # Compiled static, docker cp'd into container
                                         # Accepts: --db-type, --op, --num-records, --key-type, --table-name, etc.
                                         # Outputs: JSON (throughput, p50/p95/p99, duration)

internal/
├── runner/
│   ├── postgres.go                  # PostgreSQL scenario orchestration
│   ├── mysql.go                     # MySQL scenario orchestration
│   ├── mongodb.go                   # MongoDB scenario orchestration
│   └── cassandra.go                 # Cassandra scenario orchestration
├── benchmark/
│   ├── benchmark.go                 # Shared interfaces
│   ├── results.go                   # Result structs (InsertPerformanceResult, etc.)
│   ├── io/
│   │   └── io_metrics.go            # cgroup v2 I/O metrics (container-isolated)
│   ├── postgres/
│   │   ├── postgres.go              # PostgresBenchmarker struct
│   │   ├── connection.go            # DB setup, extensions, table creation
│   │   ├── metrics.go               # Page splits (WAL), fragmentation, buffer hits
│   │   ├── insert_pgbench.go        # Insert workload via pgbench
│   │   ├── read_pgbench.go          # Read workload via pgbench
│   │   ├── update_pgbench.go        # Update workload via pgbench
│   │   ├── mixed_pgbench.go         # Mixed workloads via pgbench
│   │   └── pgbench/
│   │       ├── executor.go          # pgbench CLI wrapper (runs inside container)
│   │       ├── parser.go            # Parse pgbench output
│   │       └── scripts.go           # Generate custom SQL scripts per UUID type
│   ├── mysql/
│   │   ├── mysql.go                 # MySQLBenchmarker struct
│   │   ├── connection.go            # DB setup, table creation
│   │   ├── metrics.go               # Page splits (innodb_metrics), fragmentation, buffer hits
│   │   ├── insert.go                    # Insert workload via workload binary
│   │   ├── read.go                      # Read workload via workload binary
│   │   ├── update.go                    # Update workload via workload binary
│   │   └── mixed.go                     # Mixed workloads via workload binary
│   ├── mongodb/
│   │   ├── mongodb.go                   # MongoDBBenchmarker struct
│   │   ├── connection.go               # DB setup, collection creation, WaitForReady
│   │   ├── metrics.go                   # WiredTiger stats, cache hit ratio, fragmentation
│   │   ├── insert.go                    # Insert workload via workload binary
│   │   ├── read.go                      # Read workload via workload binary
│   │   ├── update.go                    # Update workload via workload binary
│   │   └── mixed.go                     # Mixed workloads via workload binary
│   ├── cassandra/
│   │   ├── cassandra.go                 # CassandraBenchmarker struct
│   │   ├── connection.go               # Keyspace/table creation (bucketed schema, FNV-1a bucket)
│   │   ├── metrics.go                   # nodetool tablestats parsing; cluster-wide aggregation
│   │   ├── insert.go                    # Insert workload via workload binary
│   │   ├── read.go                      # Read workload via workload binary
│   │   ├── update.go                    # Update workload via workload binary
│   │   └── mixed.go                     # Mixed workloads via workload binary
│   ├── workload/
│   │   ├── executor.go                  # ExecutionMode: container (docker cp/exec) or native (local exec)
│   │   └── parser.go                    # Parse JSON output from workload binary
│   └── statistics/
│       ├── stats.go                 # Median, Mean, StdDev, CV, Calculate()
│       └── hypothesis.go            # Mann-Whitney U test, p-values, Compare()
├── cluster/
│   ├── backend.go                   # Backend interface: ExecOnNode, CopyToNode, NodeAddresses, ...
│   ├── local_single.go              # Single-container backend (thesis baseline)
│   ├── local_cluster.go             # 3-container compose backend (code validation)
│   └── remote_cluster.go            # SSH-driven N-machine backend (paper measurements)
├── remote/
│   └── client.go                    # SSH/SCP executor (golang.org/x/crypto/ssh)
├── container/
│   └── container.go                 # Docker Compose lifecycle (Start/Stop, fresh per UUID type)
├── display/
│   ├── table.go                     # Console comparison tables per scenario
│   └── statistics.go                # Statistical tables with ASCII box drawing
└── export/
    └── csv.go                       # CSV export (summary stats + raw runs)
```

## Key Design Decisions

**One scenario tests ALL UUID types automatically**
```go
// main.go orchestration loop:
for _, keyType := range allKeyTypes {
    container.Start(dbConfig)    // Fresh database container
    result := runner.Scenario(keyType)
    results[keyType] = result
    container.Stop(dbConfig)     // Remove container + volumes
}
display.ComparisonTable(results)
```

**Fresh container per UUID type** ensures isolated, reproducible measurements. WAL accumulation (PostgreSQL) or innodb_metrics counters (MySQL) from previous types would contaminate results.

**Workload execution inside containers** -- pgbench and the custom Go workload binary run inside Docker containers (localhost connection, zero network overhead). Go orchestrator manages lifecycle from outside.

**Shared workload binary for MySQL, MongoDB, and Cassandra** -- MySQL, MongoDB, and Cassandra use a single `cmd/workload/main.go` binary compiled statically (`CGO_ENABLED=0`), `docker cp`'d into the container, and executed via `docker exec`. This binary handles proper UUID generation using Go libraries and outputs JSON results for the orchestrator to parse. PostgreSQL is the only database still using a dedicated tool (pgbench).

## Scenarios

| Scenario | Description | Key Metrics |
|----------|-------------|-------------|
| `insert-performance` | Sequential bulk inserts | Throughput, page splits, fragmentation, disk size |
| `read-performance` | Insert → reset stats → read workload | Buffer pool hit ratios, read throughput |
| `update-performance` | Insert → random updates | Update throughput, fragmentation change |
| `mixed-insert-heavy` | 70% insert, 30% read | Mixed throughput, latency under write pressure |
| `mixed-read-update` | 50% read, 50% update (YCSB Workload A) | OLTP simulation throughput, latency |

## Database-Specific Details

### PostgreSQL

**Docker:** `docker/docker-compose.postgres.yml` + `docker/Dockerfile.postgres`
- Base: `postgres:18`, custom build with Rust toolchain for pgx_ulid
- Container: `uuid-bench-postgres`, port `5432`
- Config: `checkpoint_timeout=1h`, `max_wal_size=50GB`, 8GB memory, 4 CPUs

**Extensions:**
- `pgstattuple` -- index fragmentation (`pgstatindex()` → `leaf_fragmentation`, `avg_leaf_density`)
- `pg_walinspect` -- page split counting via WAL analysis between LSN snapshots
- `uuid-ossp` -- UUIDv1 generation (`uuid_generate_v1()`)
- `pgx_ulid` -- ULID generation via Rust/pgrx (`gen_ulid()`, `gen_monotonic_ulid()`)

**UUID generation:** Server-side via PostgreSQL functions (proper time-ordered generation for UUIDv7/ULID)

**Key SQL queries:**
```sql
-- Page Splits (LSN-based WAL analysis)
SELECT COALESCE(SUM(count), 0)::int
FROM pg_get_wal_stats($startLSN::pg_lsn, $endLSN::pg_lsn, per_record := true)
WHERE "resource_manager/record_type" IN ('Btree/SPLIT_L', 'Btree/SPLIT_R')

-- Index Fragmentation
SELECT leaf_fragmentation, avg_leaf_density FROM pgstatindex('bench_pkey')

-- Buffer Pool Hit Ratio (after pg_stat_reset())
SELECT blks_hit::float / NULLIF(blks_hit + blks_read, 0)
FROM pg_stat_database WHERE datname = 'uuid_benchmark'
```

**Connection:** `localhost:5432`, user `benchmark`, password `benchmark123`, database `uuid_benchmark`

### MySQL

**Docker:** `docker/docker-compose.mysql.yml`
- Base: `mysql:8-debian`
- Container: `uuid-bench-mysql`, port `3307` (avoids local MySQL conflict)
- Config: `innodb_buffer_pool_size=4G`, `performance_schema=ON`, `innodb_monitor_enable=index_page_splits`

**UUID storage:** `BINARY(16)` for all UUID types (efficient storage, no string overhead)

**Workload tool:** Custom Go workload binary (`cmd/workload/main.go`), shared with MongoDB and Cassandra.

**UUID generation:** Client-side via Go libraries inside the workload binary (`github.com/google/uuid`, `github.com/oklog/ulid`). Proper time-ordered generation for UUIDv7/ULID matching the other databases.

**Metrics:**
- Page splits: delta from `information_schema.innodb_metrics` (`index_page_splits` counter)
- Fragmentation: `(data_free / (data_length + index_length))` -- B-tree overhead ratio, **NOT the same metric as PostgreSQL's physical page ordering**
- Buffer pool hit ratio: `performance_schema` -- `(read_requests - disk_reads) / read_requests`
- Leaf density: estimated at 90% (InnoDB does not expose actual values)

**Connection:** `localhost:3307`, user `benchmark`, password `benchmark123`, database `uuid_benchmark`

### MongoDB

**Docker:** `docker/docker-compose.mongo.yml`
- Base: `mongo:8` (WiredTiger storage engine, B-tree indexes)
- Container: `uuid-bench-mongodb`, port `27017`
- Config: 4 CPUs, 8GB memory limit, 4GB reservation

**Storage engine:** WiredTiger with B-tree indexes. The `_id` field always gets a mandatory B-tree index. Non-clustered (data and index are separate structures, like PostgreSQL). WiredTiger has its own cache separate from OS page cache.

**Workload tool:** Custom Go workload binary (`cmd/workload/main.go`). No standard MongoDB benchmark tool supports custom `_id` generation with proper UUID types:
- `mongosh`: Single-threaded, `benchRun()` removed in MongoDB 6.0+
- `mongo-perf`: Unmaintained since 2019, needs legacy shell
- `YCSB`: Keys are strings (`user123`), would need fork for Binary UUID `_id`
- Custom Go binary: Full control, proper BSON Binary types, runs inside container

**UUID storage:** BSON `Binary(subtype 0x04)` for all UUID types (16 bytes, efficient). ULID stored as `Binary(subtype 0x00)`. ObjectId (12 bytes, time-ordered) as additional MongoDB-native comparison point.

**UUID generation:** Client-side via Go libraries inside the workload binary. Proper time-ordered generation for UUIDv7/ULID using `github.com/google/uuid` and `github.com/oklog/ulid`.

**Key types in MongoDB:**

| Key Type | BSON Type | Storage | Time-ordered? |
|----------|-----------|---------|--------------|
| Sequential int | Int64 | 8 bytes | Yes |
| ObjectId | ObjectId | 12 bytes | Yes (native MongoDB) |
| UUIDv1 | Binary(0x04) | 16 bytes | Partially (bad byte order) |
| UUIDv4 | Binary(0x04) | 16 bytes | No (random) |
| UUIDv7 | Binary(0x04) | 16 bytes | Yes |
| ULID | Binary(0x00) | 16 bytes | Yes |
| ULID monotonic | Binary(0x00) | 16 bytes | Yes |

**Metrics collection** (via Go driver calling MongoDB commands from inside container):

```javascript
// Page splits (WiredTiger cache stats — delta before/after workload)
db.serverStatus().wiredTiger.cache["in-memory page splits"]
db.serverStatus().wiredTiger.cache["pages split during eviction"]

// Cache hit ratio (analogous to buffer pool hit ratio)
pages_requested = wiredTiger.cache["pages requested from the cache"]
pages_read = wiredTiger.cache["pages read into cache"]
hit_ratio = 1 - (pages_read / pages_requested)

// Index fragmentation proxy
db.bench.stats().freeStorageSize / db.bench.stats().storageSize

// Collection and index sizes
db.bench.stats().storageSize       // compressed on-disk data size
db.bench.stats().totalIndexSize    // total index size
db.bench.stats().indexSizes        // per-index breakdown

// B-tree depth (unique to MongoDB — PG/MySQL don't expose this)
db.bench.stats({indexDetails: true})  // btree["maximum tree depth"] per index

// Leaf pages per index
db.bench.stats({indexDetails: true})  // btree["row-store leaf pages"]
```

**Connection:** `localhost:27017`, user `benchmark`, password `benchmark123`, database `uuid_benchmark`

### Cassandra

**Docker:** `docker/docker-compose.cassandra.yml`
- Base: `cassandra:5` (LSM-tree storage engine)
- Container: `uuid-bench-cassandra`, port `9042` (CQL) / `7199` (JMX)
- Config: 4 CPUs, 8GB memory limit, 4G heap (`MAX_HEAP_SIZE`), 1G new gen (`HEAP_NEWSIZE`)

**Storage engine:** LSM-tree. Fundamentally different from B-tree databases:
- Writes go to in-memory MemTable → flushed to immutable SSTables on disk
- No page splits during writes (append-only)
- Compaction merges SSTables periodically (the key overhead metric, replaces page splits)
- Read amplification: may need to check multiple SSTables per read (bloom filters help)

**Workload tool:** Custom Go workload binary (`cmd/workload/main.go`), same as MongoDB. `cassandra-stress` (built-in) supports `uuid`, `timeuuid`, and `bigint` natively but CANNOT generate UUIDv7 or ULID. The Go binary gives consistent methodology across all databases.

**UUID storage and types:**

| Key Type | CQL Type | Storage | Time-ordered in Cassandra? |
|----------|----------|---------|---------------------------|
| Sequential int | `bigint` | 8 bytes | Yes |
| UUIDv1 | `timeuuid` | 16 bytes | Yes (native, time-sorted) |
| UUIDv4 | `uuid` | 16 bytes | No (random, byte-order) |
| UUIDv7 | `uuid` | 16 bytes | Yes (timestamp in MSB, byte-order = time-order) |
| ULID | `blob` | 16 bytes | Yes (timestamp in MSB, byte-order = time-order) |
| ULID monotonic | `blob` | 16 bytes | Yes |

**UUID generation:** Client-side via Go libraries inside the workload binary.

**Clustering sort order note:** Cassandra sorts `timeuuid` clustering columns by extracted timestamp, not raw byte order. This means UUIDv1 gets native time-sorted clustering in Cassandra — unlike B-tree databases (PostgreSQL, MySQL, MongoDB) where UUIDv1's swapped timestamp bytes cause poor ordering. UUIDv7/ULID/`uuid`/`blob` types are sorted by raw byte order, which preserves their inherent time-ordering (timestamp in MSB).

**Schema per key type** — uses compound primary key `PRIMARY KEY ((bucket), id)` so the UUID becomes a clustering key with preserved sort ordering. Cassandra's Murmur3Partitioner hashes partition keys, which would destroy UUID ordering if the UUID were the partition key. By making it a clustering key, MemTable sorting, SSTable layout, and read patterns reflect actual UUID ordering:
```sql
-- Example for UUIDv7
CREATE TABLE uuid_benchmark.bench (
    bucket int,
    id uuid,
    payload blob,
    PRIMARY KEY ((bucket), id)
) WITH compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Example for ULID (stored as blob, byte-order preserves time-order)
CREATE TABLE uuid_benchmark.bench (
    bucket int,
    id blob,
    payload blob,
    PRIMARY KEY ((bucket), id)
) WITH compaction = {'class': 'SizeTieredCompactionStrategy'};
```

**Bucket value (paper extension).** The thesis pinned `bucket = 1` to keep the entire dataset on one node. The cluster extension instead computes `bucket = FNV-1a(id_bytes) mod N` per row, where `N` is `-num-buckets` (default 1000). FNV-1a was chosen because it is fast, dependency-free, and well-distributed for binary keys; any hashing that doesn't reintroduce UUID-byte ordering would do. All CQL operations carry the row's bucket: inserts pass `(bucket, id)`, reads/updates/fetches use `WHERE bucket = ? AND id = ?`. Single-node mode runs the same code path — with one node every bucket lands on that node, so behavior matches the thesis baseline.

**Compaction strategy:** SizeTieredCompactionStrategy (STCS) is the Cassandra default and most commonly used. STCS groups similarly-sized SSTables for compaction, which makes the impact of random vs sorted keys more visible — random keys produce SSTables with highly overlapping key ranges, forcing more compaction work. LCS (Leveled) would sort data into non-overlapping levels, potentially masking the UUID ordering effect.

**Metrics collection** (via `docker exec nodetool tablestats` parsed from Go):

```bash
# Primary metric source — run before and after each workload phase
docker exec uuid-bench-cassandra nodetool tablestats uuid_benchmark.bench

# Key metrics from tablestats:
# - SSTable count                    (more = more read amplification)
# - Space used (live)                (actual data)
# - Space used (total)               (total/live = space amplification ratio)
# - Bloom filter false positives     (random keys may cause more)
# - Bloom filter false ratio
# - Memtable switch count            (write pressure)
# - Local read latency / write latency (ms)
# - Key cache hit rate               (analogous to buffer pool hit ratio)
# - Compacted partition mean bytes

# Compaction history — shows merge activity during workload
docker exec uuid-bench-cassandra nodetool compactionhistory

# Cache hit ratios
docker exec uuid-bench-cassandra nodetool info
# Reports: Key Cache hit rate, Row Cache hit rate
```

**Metric mapping (LSM-tree equivalents):**

| B-tree Concept | Cassandra LSM-tree Equivalent | Why It Matters |
|----------------|-------------------------------|----------------|
| Page splits | SSTable count delta (per-node clamped) | Random keys → more overlapping SSTables → more compaction work |
| Fragmentation | SSTable count + space amplification ratio | More SSTables = more scattered data |
| Buffer pool hit ratio | Key cache hit rate | Same concept — reads served from memory vs disk |
| Leaf density | N/A | LSM-tree doesn't have B-tree leaves |
| Index size | Bloom filter space + index summary size | Different index structure |
| Table size | Space used (live) from tablestats | Direct comparison |

**Key hypothesis:** With UUID as clustering key (compound primary key), LSM-tree MemTable sorting and SSTable layout reflect actual UUID byte ordering. Time-ordered keys (UUIDv7, ULID) produce sequential clustering within the partition, while random keys (UUIDv4) produce scattered ordering. LSM-tree architecture may be less sensitive to random UUIDs for writes (MemTable absorbs randomness), but read performance may still suffer due to higher SSTable count and read amplification with random keys.

**Connection:** `localhost:9042`, keyspace `uuid_benchmark`, no authentication (Cassandra default). In `remote-cluster` mode the contact list comes from `-nodes`; in `local-cluster` mode only the seed (`cassandra-1`) is reachable from the host.

**Cluster-mode metrics:** in `local-cluster` and `remote-cluster` modes, `nodetool tablestats` and cgroup v2 `io.stat` are collected from every node (via `docker exec` and SSH, respectively). Counters are summed, ratios averaged. PageSplits is clamped at 0 in Cassandra because LSM-tree compactions can run between snapshots and produce negative deltas — documented in `docs/paper-notes.md`.

## I/O Metrics

Uses Linux cgroup v2 for container-isolated I/O measurement:
- Path: `/sys/fs/cgroup/system.slice/docker-<container_id>.scope/io.stat`
- Zero overhead (kernel-level accounting, no sampling)
- Metrics: Read/Write IOPS, Read/Write throughput (MB/s)
- Container-isolated (not system-wide like iostat/vmstat)

## Cross-Database Metric Comparability

| Metric | PostgreSQL | MySQL | MongoDB | Cassandra | Comparable? |
|--------|-----------|-------|---------|-----------|-------------|
| Throughput (ops/sec) | pgbench | Go workload binary | Go workload binary | Go workload binary | Yes |
| Latency (p50/p95/p99) | pgbench | Go workload binary | Go workload binary | Go workload binary | Yes |
| Page splits / compaction | WAL analysis (exact) | innodb_metrics delta | WiredTiger cache splits delta | Compaction count + bytes | **Conceptually** (B-tree splits vs LSM compaction) |
| Fragmentation | Physical page ordering % | B-tree overhead ratio | freeStorageSize/storageSize | SSTable count + space amplification | **No** (4 different definitions) |
| Cache hit ratio | pg_stat_database | performance_schema | WiredTiger cache (pages requested vs read) | Key cache hit rate (nodetool info) | Yes (same concept) |
| Leaf density | pgstatindex (exact %) | Estimated 90% | Leaf pages from indexDetails | N/A (no B-tree leaves) | **No** |
| B-tree depth | Not exposed | Not exposed | btree["maximum tree depth"] | N/A | MongoDB-only |
| SSTable count | N/A | N/A | N/A | nodetool tablestats | Cassandra-only |
| Read amplification | N/A | N/A | N/A | Bloom filter false positives | Cassandra-only |
| Disk size | pg_relation_size | information_schema | db.collection.stats() | nodetool tablestats | Yes |
| I/O | cgroup v2 | cgroup v2 | cgroup v2 | cgroup v2 | Yes |

**Note on cross-architecture comparison:** B-tree databases (PostgreSQL, MySQL, MongoDB) share comparable concepts (page splits, fragmentation, leaf density). Cassandra's LSM-tree architecture uses fundamentally different mechanisms (compaction, SSTables, bloom filters). The thesis discusses these as architecture-specific effects rather than forcing direct comparisons where they don't apply.
