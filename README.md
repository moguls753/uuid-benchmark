# UUID Benchmark

Benchmarks UUID types (UUIDv1, UUIDv4, UUIDv7, ULID non-monotonic, ULID monotonic) vs sequential integer keys across PostgreSQL, MySQL, MongoDB, and Cassandra. Measures page splits, fragmentation, buffer pool / cache hit ratios, disk usage, throughput, and latency percentiles.

## Requirements

- Go 1.25+
- Docker & Docker Compose
- Linux (for I/O metrics)

## Build & Run

```bash
go build -o uuid-benchmark cmd/benchmark/main.go

# Run all scenarios for a database
./uuid-benchmark -database=postgres -scenario=all -num-records=1000000 -num-ops=100000 -connections=10 -num-runs=5 -output=results.csv
./uuid-benchmark -database=mysql -scenario=all -num-records=1000000 -num-ops=100000 -connections=10 -num-runs=5 -output=results.csv
./uuid-benchmark -database=mongodb -scenario=all -num-records=1000000 -num-ops=100000 -connections=10 -num-runs=5 -output=results.csv
./uuid-benchmark -database=cassandra -scenario=all -num-records=1000000 -num-ops=100000 -connections=10 -num-runs=5 -output=results.csv

# Run single scenario (tests all UUID types automatically)
./uuid-benchmark -database=postgres -scenario=insert-performance -num-records=100000 -connections=10

# Run with statistical analysis (5 runs per UUID type)
./uuid-benchmark -database=mongodb -scenario=insert-performance -num-records=100000 -num-runs=5 -output=results.csv
```

## Options

- `-database` - Database to benchmark: `postgres`, `mysql`, `mongodb`, `cassandra` (default: postgres)
- `-scenario` - Scenario to run: `insert-performance`, `read-after-fragmentation`, `update-performance`, `mixed-insert-heavy`, `mixed-read-heavy`, `mixed-balanced`, `all`
- `-num-records` - Dataset size for insert scenarios (default: 100000)
- `-num-ops` - Number of operations for read/update/mixed (default: 10000)
- `-connections` - Concurrent workers (default: 1)
- `-batch-size` - Records per transaction (default: 100)
- `-num-runs` - Number of runs per UUID type for statistical analysis (default: 1)
- `-output` - CSV file for statistical results (multi-run mode only)

## Scenarios

- `insert-performance` - Page splits, fragmentation, disk usage, throughput
- `read-after-fragmentation` - Buffer pool hit ratios, memory efficiency
- `update-performance` - Update throughput, fragmentation impact
- `mixed-insert-heavy` - 90% insert, 10% read workload
- `mixed-read-heavy` - 10% insert, 90% read workload
- `mixed-balanced` - 50% insert, 30% read, 20% update (OLTP simulation)
- `all` - Runs all scenarios sequentially (comprehensive benchmark)

## How It Works

Each database uses a workload tool that runs **inside the Docker container** (localhost connection, zero network overhead):

| Database | Workload Tool | UUID Generation |
|---|---|---|
| PostgreSQL | pgbench with custom SQL scripts | Server-side (PostgreSQL functions) |
| MySQL | sysbench with custom Lua scripts | Client-side (sysbench random bytes) |
| MongoDB | Custom Go binary | Client-side (Go UUID/ULID libraries) |
| Cassandra | Custom Go binary | Client-side (Go UUID/ULID libraries) |

**Workflow:** For each key type (SEQUENTIAL, UUIDv4, UUIDv7, ULID, ULID_MONOTONIC, UUIDv1), the benchmark:
1. Starts a **fresh database container** to ensure isolated measurements
2. Creates the benchmark table/collection with the appropriate key type
3. Executes the workload inside the container
4. Collects database-specific metrics after the workload completes
5. Stops and removes the container (including volumes)

**Metrics collected per database:**

| Metric | PostgreSQL | MySQL | MongoDB | Cassandra |
|---|---|---|---|---|
| Page splits / compaction | WAL analysis | innodb_metrics | WiredTiger cache splits | SSTable count delta |
| Fragmentation | pgstatindex | B-tree overhead ratio | freeStorageSize/storageSize | Space amplification |
| Cache hit ratio | pg_stat_database | performance_schema | WiredTiger cache | Key cache (nodetool) |
| Disk size | pg_relation_size | information_schema | collStats | nodetool tablestats |
| Throughput & latency | pgbench | sysbench | Go workload binary | Go workload binary |
| I/O | cgroup v2 | cgroup v2 | cgroup v2 | cgroup v2 |

**Key Design Decisions:**
- **Fresh container per UUID type:** Prevents metric contamination between runs
- **Workload inside container:** Eliminates network latency from measurements
- **Custom Go workload binary for NoSQL:** Enables proper UUID generation with Go libraries (`github.com/google/uuid`, `github.com/oklog/ulid`) — no existing MongoDB/Cassandra benchmark tool supports custom UUID `_id` generation
- **Statistical analysis mode:** Multiple runs with Mann-Whitney U tests provide p-values and significance testing

## Validation

PostgreSQL results validated against **go-ycsb** (industry-standard benchmark) for overlapping metrics (throughput, latency). Both tools run inside containers with identical architecture (client inside container → localhost). See `validation/` directory.

```bash
cd validation
./run-comparison.sh balanced  # Runs both tools, compares sequential int results
```
