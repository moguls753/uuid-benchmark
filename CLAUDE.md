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
- MySQL 8 with sysbench integration (all 5 scenarios, all metrics)
- MongoDB 8 with custom Go workload binary (all 5 scenarios, all metrics)
- Cassandra 5 with custom Go workload binary (all 5 scenarios, all metrics)
- Shared workload binary (`cmd/workload/main.go`) for MongoDB/Cassandra with proper UUID generation via Go libraries
- Statistical analysis (Mann-Whitney U, median, mean, stddev, CV, multi-run mode)
- CSV export (summary stats + raw per-run data)
- Docker orchestration (fresh container per UUID type for isolation)
- YCSB validation (BIGSERIAL throughput/latency matches within ~8%)

**Known limitation:**
- MySQL UUIDv7/ULID generation is NOT time-ordered. Sysbench Lua scripts use `sysbench.rand.unique()` (random bytes), not proper UUID libraries. In MySQL benchmarks, UUIDv7 and ULID behave like UUIDv4.

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

**UUID Types Tested:** Sequential integer (BIGSERIAL/AUTO_INCREMENT/bigint), UUIDv1, UUIDv4, UUIDv7, ULID (non-monotonic), ULID (monotonic)

## Architecture

```
cmd/
├── benchmark/
│   └── main.go                          # CLI, orchestration loop, container lifecycle
└── workload/
    └── main.go                          # Standalone workload binary for MongoDB/Cassandra
                                         # Compiled static, docker cp'd into container
                                         # Accepts: --db-type, --op, --num-records, --key-type, etc.
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
│   │   ├── insert_sysbench.go       # Insert workload via sysbench
│   │   ├── read_sysbench.go         # Read workload via sysbench
│   │   ├── update_sysbench.go       # Update workload via sysbench
│   │   ├── mixed_sysbench.go        # Mixed workloads via sysbench
│   │   └── sysbench/
│   │       ├── executor.go          # sysbench CLI wrapper (runs inside container)
│   │       ├── parser.go            # Parse sysbench output
│   │       └── scripts.go           # Generate Lua scripts per UUID type
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
│   │   ├── connection.go               # DB setup, keyspace/table creation, WaitForReady
│   │   ├── metrics.go                   # nodetool tablestats parsing, compaction metrics
│   │   ├── insert.go                    # Insert workload via workload binary
│   │   ├── read.go                      # Read workload via workload binary
│   │   ├── update.go                    # Update workload via workload binary
│   │   └── mixed.go                     # Mixed workloads via workload binary
│   ├── workload/
│   │   ├── executor.go                  # Build, docker cp, docker exec workload binary
│   │   └── parser.go                    # Parse JSON output from workload binary
│   └── statistics/
│       ├── stats.go                 # Median, Mean, StdDev, CV, Calculate()
│       └── hypothesis.go            # Mann-Whitney U test, p-values, Compare()
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

**Workload execution inside containers** -- pgbench, sysbench, and the custom Go workload binary all run inside Docker containers (localhost connection, zero network overhead). Go orchestrator manages lifecycle from outside.

**Shared workload binary for NoSQL databases** -- MongoDB and Cassandra use a single `cmd/workload/main.go` binary compiled statically (`CGO_ENABLED=0`), `docker cp`'d into the container, and executed via `docker exec`. This binary handles proper UUID generation using Go libraries (fixing the MySQL limitation where sysbench used random bytes) and outputs JSON results for the orchestrator to parse.

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
- Config: `checkpoint_timeout=1h`, `max_wal_size=10GB`, 8GB memory, 4 CPUs

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

**Docker:** `docker/docker-compose.mysql.yml` + `docker/Dockerfile.mysql`
- Base: `mysql:8-debian` with sysbench installed
- Container: `uuid-bench-mysql`, port `3307` (avoids local MySQL conflict)
- Config: `innodb_buffer_pool_size=4G`, `performance_schema=ON`, `innodb_monitor_enable=index_page_splits`

**UUID storage:** `BINARY(16)` for all UUID types (efficient storage, no string overhead)

**UUID generation:** Client-side via sysbench Lua scripts using `sysbench.rand.unique()`.
⚠️ **This generates random bytes, NOT proper UUIDv7/ULID.** Time-ordered variants behave identically to UUIDv4 in MySQL benchmarks.

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

**Workload tool:** Custom Go workload binary (`cmd/workload/main.go`), same as MongoDB. `cassandra-stress` (built-in) supports `uuid`, `timeuuid`, and `bigint` natively but CANNOT generate UUIDv7 or ULID — same limitation as sysbench. The Go binary gives consistent methodology across all databases.

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

**Schema per key type:**
```sql
-- Example for UUIDv7
CREATE TABLE uuid_benchmark.bench (
    id uuid PRIMARY KEY,
    payload blob
) WITH compaction = {'class': 'SizeTieredCompactionStrategy'};

-- Example for ULID (stored as blob, byte-order preserves time-order)
CREATE TABLE uuid_benchmark.bench (
    id blob PRIMARY KEY,
    payload blob
) WITH compaction = {'class': 'SizeTieredCompactionStrategy'};
```

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
| Page splits | Compaction count + bytes compacted | Random keys → more overlapping SSTables → more compaction work |
| Fragmentation | SSTable count + space amplification ratio | More SSTables = more scattered data |
| Buffer pool hit ratio | Key cache hit rate | Same concept — reads served from memory vs disk |
| Leaf density | N/A | LSM-tree doesn't have B-tree leaves |
| Index size | Bloom filter space + index summary size | Different index structure |
| Table size | Space used (live) from tablestats | Direct comparison |

**Key hypothesis:** LSM-tree architecture may be less sensitive to random UUIDs for writes (MemTable absorbs randomness), but read performance may still suffer due to higher SSTable count and read amplification with random keys.

**Connection:** `localhost:9042`, keyspace `uuid_benchmark`, no authentication (Cassandra default)

## I/O Metrics

Uses Linux cgroup v2 for container-isolated I/O measurement:
- Path: `/sys/fs/cgroup/system.slice/docker-<container_id>.scope/io.stat`
- Zero overhead (kernel-level accounting, no sampling)
- Metrics: Read/Write IOPS, Read/Write throughput (MB/s)
- Container-isolated (not system-wide like iostat/vmstat)

## Cross-Database Metric Comparability

| Metric | PostgreSQL | MySQL | MongoDB | Cassandra | Comparable? |
|--------|-----------|-------|---------|-----------|-------------|
| Throughput (ops/sec) | pgbench | sysbench | Go workload binary | Go workload binary | Yes |
| Latency (p50/p95/p99) | pgbench | sysbench | Go workload binary | Go workload binary | Yes |
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
