# UUID Benchmark

Benchmarks UUID types (UUIDv1, UUIDv4, UUIDv7, ULID non-monotonic, ULID monotonic) vs BIGSERIAL in PostgreSQL. Measures page splits, fragmentation, buffer pool efficiency, and throughput.

## Requirements

- Go 1.21+
- Docker & Docker Compose
- Linux (for I/O metrics)

## Build & Run

```bash
go build -o uuid-benchmark cmd/benchmark/main.go

# Run all scenarios (comprehensive thesis benchmark)
./uuid-benchmark -scenario=all -num-records=1000000 -num-ops=100000 -connections=10 -num-runs=5 -output=results.csv

# Run single scenario (tests all UUID types automatically)
./uuid-benchmark -scenario=insert-performance -num-records=100000 -connections=10

# Run with statistical analysis (5 runs per UUID type)
./uuid-benchmark -scenario=insert-performance -num-records=100000 -num-runs=5 -output=results.csv
```

## Options

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

The benchmark uses **pgbench** (PostgreSQL's standard benchmarking tool) for workload execution, extended with custom metrics collection via Go.

**Architecture:** The Go application orchestrates Docker containers and metrics collection, while pgbench executes SQL workloads inside the PostgreSQL container. This ensures all database operations happen on localhost (loopback interface) with zero network overhead.

**Workflow:** For each UUID type (BIGSERIAL, UUIDv4, UUIDv7, ULID, ULID_MONOTONIC, UUIDv1), the benchmark:
1. Starts a **fresh PostgreSQL container** to ensure isolated measurements
2. Creates the benchmark table with the appropriate ID type (e.g., `id UUID PRIMARY KEY` for UUIDv7, `id ulid PRIMARY KEY` for ULID)
3. Generates a SQL script in Go and copies it into the container
4. Executes the workload via **pgbench inside the container** using server-side ID generation functions (`gen_random_uuid()`, `uuidv7()`, `gen_ulid()`, etc.)
5. Collects metrics after the workload completes
6. Stops and removes the container

**Metrics collected:**
- **Page Splits:** Counted via WAL analysis (`pg_walinspect` extension) - indicates B-tree index fragmentation during inserts
- **Index Fragmentation:** Measured via `pgstatindex()` - shows % of index pages that are out-of-order
- **Buffer Pool Hit Ratios:** Measures memory efficiency - % of reads served from cache vs disk
- **Table/Index Size:** Disk usage in MB
- **Throughput & Latency:** Transactions per second, p50/p95/p99 latency from pgbench
- **I/O Metrics:** Read/write IOPS and throughput via Linux cgroup v2 (container-isolated)

**Key Design Decisions:**
- **Fresh container per UUID type:** Ensures clean WAL state and prevents contamination of page split counts
- **Server-side ID generation:** All UUID types use PostgreSQL functions for fair comparison (no client-side pre-generation)
- **pgbench inside container:** Eliminates network latency from measurements
- **Statistical analysis mode:** Multiple runs with Mann-Whitney U tests provide p-values and significance testing
