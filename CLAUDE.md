# CLAUDE.md

## Project Purpose

Bachelor thesis benchmarking UUID performance (UUIDv1, UUIDv4, UUIDv7, ULID, ULID monotonic) vs sequential integer keys (BIGSERIAL/AUTO_INCREMENT) across multiple databases. Focus on measuring:
- Page splits and index fragmentation
- Disk usage (table/index size)
- Query performance (throughput, latency percentiles p50/p95/p99)
- Memory efficiency (buffer pool hit ratios)
- I/O metrics (IOPS, throughput via cgroup v2)

## Implementation Status

**Complete:**
- PostgreSQL 18 with pgbench integration (all 6 scenarios, all metrics)
- MySQL 8 with sysbench integration (all 6 scenarios, all metrics)
- Statistical analysis (Mann-Whitney U, median, mean, stddev, CV, multi-run mode)
- CSV export (summary stats + raw per-run data)
- Docker orchestration (fresh container per UUID type for isolation)

**Not implemented:**
- Cassandra: Docker Compose config exists (`docker/docker-compose.cassandra.yml`), no Go code
- MongoDB: Docker Compose config exists (`docker/docker-compose.mongo.yml`), no Go code

**Known limitation:**
- MySQL UUIDv7/ULID generation is NOT time-ordered. Sysbench Lua scripts use `sysbench.rand.unique()` (random bytes), not proper UUID libraries. In MySQL benchmarks, UUIDv7 and ULID behave like UUIDv4.

## Build & Run

```bash
# Build
go build -o uuid-benchmark cmd/benchmark/main.go

# Run all scenarios for PostgreSQL (comprehensive thesis benchmark)
./uuid-benchmark -database=postgres -scenario=all -num-records=1000000 -num-ops=100000 -connections=10 -num-runs=5 -output=results.csv

# Run all scenarios for MySQL
./uuid-benchmark -database=mysql -scenario=all -num-records=1000000 -num-ops=100000 -connections=10 -num-runs=5 -output=results.csv

# Run individual scenario
./uuid-benchmark -database=postgres -scenario=insert-performance -num-records=100000 -batch-size=100
./uuid-benchmark -database=mysql -scenario=read-after-fragmentation -num-records=1000000 -num-ops=10000
```

### CLI Flags

| Flag | Default | Description |
|------|---------|-------------|
| `-database` | `postgres` | Database to test: `postgres`, `mysql` |
| `-scenario` | `insert-performance` | Scenario: `insert-performance`, `read-after-fragmentation`, `update-performance`, `mixed-insert-heavy`, `mixed-read-heavy`, `mixed-balanced`, `all` |
| `-num-records` | `100000` | Dataset size for insert operations |
| `-num-ops` | `10000` | Number of operations for read/update/mixed scenarios |
| `-connections` | `1` | Concurrent workers |
| `-batch-size` | `100` | Records per transaction |
| `-num-runs` | `1` | Runs per UUID type for statistical analysis |
| `-output` | (none) | CSV file for statistical results (multi-run mode only) |

**UUID Types Tested:** BIGSERIAL/AUTO_INCREMENT, UUIDv1, UUIDv4, UUIDv7, ULID (non-monotonic), ULID (monotonic)

## Architecture

```
cmd/benchmark/
└── main.go                          # CLI, orchestration loop, container lifecycle

internal/
├── runner/
│   ├── postgres.go                  # PostgreSQL scenario orchestration
│   └── mysql.go                     # MySQL scenario orchestration
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

**Workload execution inside containers** -- pgbench and sysbench run inside the Docker container (localhost connection, zero network overhead). Go orchestrates from outside.

## Scenarios

| Scenario | Description | Key Metrics |
|----------|-------------|-------------|
| `insert-performance` | Sequential bulk inserts | Throughput, page splits, fragmentation, disk size |
| `read-after-fragmentation` | Insert → reset stats → read workload | Buffer pool hit ratios, read throughput |
| `update-performance` | Insert → random updates | Update throughput, fragmentation change |
| `mixed-insert-heavy` | 90% insert, 10% read | Mixed throughput, latency |
| `mixed-read-heavy` | 10% insert, 90% read | Mixed throughput, latency |
| `mixed-balanced` | 50% insert, 30% read, 20% update | OLTP simulation |

## Database-Specific Details

### PostgreSQL

**Docker:** `docker/docker-compose.postgres.yml` + `docker/Dockerfile.postgres`
- Base: `postgres:18`, custom build with Rust toolchain for pgx_ulid
- Container: `uuid-bench-postgres`, port `5432`
- Config: `checkpoint_timeout=1h`, `max_wal_size=10GB`, 4-8GB memory, 4 CPUs

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

### Cassandra (not implemented)

**Docker:** `docker/docker-compose.cassandra.yml`
- Base: `cassandra:5`, port `9042` (CQL) / `7199` (JMX)
- No Go benchmark code exists

### MongoDB (not implemented)

**Docker:** `docker/docker-compose.mongo.yml`
- Base: `mongo:8`, port `27017`
- No Go benchmark code exists

## I/O Metrics

Uses Linux cgroup v2 for container-isolated I/O measurement:
- Path: `/sys/fs/cgroup/system.slice/docker-<container_id>.scope/io.stat`
- Zero overhead (kernel-level accounting, no sampling)
- Metrics: Read/Write IOPS, Read/Write throughput (MB/s)
- Container-isolated (not system-wide like iostat/vmstat)

## Cross-Database Metric Comparability

| Metric | PostgreSQL | MySQL | Comparable? |
|--------|-----------|-------|-------------|
| Throughput (ops/sec) | pgbench | sysbench | Yes |
| Latency (p50/p95/p99) | pgbench | sysbench | Yes |
| Page splits | WAL analysis (exact) | innodb_metrics delta (global counter) | Yes (both count splits) |
| Fragmentation | Physical page ordering % | B-tree overhead ratio | **No** (different definitions) |
| Buffer pool hit ratio | pg_stat_database | performance_schema | Yes (same concept) |
| Leaf density | pgstatindex (exact %) | Estimated 90% | **No** (MySQL doesn't expose) |
| Disk size | pg_relation_size | information_schema | Yes (note: MySQL clustered index includes data) |
| I/O | cgroup v2 | cgroup v2 | Yes |
