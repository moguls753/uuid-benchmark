# UUID Benchmark

Benchmarks UUID types (UUIDv1, UUIDv4, UUIDv7, ULID non-monotonic, ULID monotonic) vs sequential integer keys across PostgreSQL, MySQL, MongoDB, and Cassandra. Measures page splits, fragmentation, buffer pool / cache hit ratios, disk usage, throughput, and latency percentiles.

## Requirements

| Requirement | Minimum | Notes |
|---|---|---|
| Go | 1.22 | Needed on the host to build the benchmark binary and the workload binary |
| Docker | 20.10 | Must be able to run containers with `--cpus` and `--memory` flags |
| Docker Compose | V1 ≥ 1.29 (`docker-compose`) **or** V2 plugin (`docker compose`) | Either works |
| Linux | kernel ≥ 5.10 | cgroup v2 required for container-isolated I/O metrics |
| Disk space | ≥ 20 GB free | Each database image + volumes can reach several GB; images are rebuilt per run |
| RAM | ≥ 8 GB | PostgreSQL and MongoDB containers are configured with 8 GB memory limits |
| Internet access | — | First run fetches Docker base images and builds pgx_ulid from source |

**cgroup v2 check:**
```bash
mount | grep cgroup2   # should show a cgroup2 mount
```
If cgroup v2 is not mounted, I/O metrics will be zeroed out but all other metrics will work normally.

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
- `-scenario` - Scenario to run: `insert-performance`, `read-performance`, `update-performance`, `mixed-insert-heavy`, `mixed-read-update`, `all`
- `-num-records` - Dataset size for insert scenarios (default: 100000)
- `-num-ops` - Number of operations for read/update/mixed (default: 10000)
- `-connections` - Concurrent workers (default: 1)
- `-batch-size` - Records per transaction (default: 100)
- `-num-runs` - Number of runs per UUID type for statistical analysis (default: 1)
- `-output` - CSV file for statistical results (multi-run mode only)
- `-cluster-mode` - Cassandra deployment: `local-single` (default), `local-cluster`, `remote-cluster`
- `-cluster-nodes` - Node count for `local-cluster` mode (default: 3)
- `-nodes` - Comma-separated `host[:port]` list for `remote-cluster` mode
- `-ssh-user` - SSH user for `remote-cluster` mode
- `-ssh-key` - SSH private key path for `remote-cluster` mode
- `-replication-factor` - Cassandra keyspace RF (default: 1 single-node, 3 cluster)
- `-consistency` - gocql consistency level: `one`, `local_one`, `local_quorum`, `quorum` (default: `local_one` for `local-single`, `local_quorum` for cluster modes)
- `-num-buckets` - Partition-key bucket count for the bucketed Cassandra schema (default: 1000)

## Scenarios

- `insert-performance` - Page splits, fragmentation, disk usage, throughput
- `read-performance` - Buffer pool hit ratios, memory efficiency
- `update-performance` - Update throughput, fragmentation impact
- `mixed-insert-heavy` - 70% insert, 30% read workload
- `mixed-read-update` - 50% read, 50% update (YCSB Workload A)
- `all` - Runs all scenarios sequentially (comprehensive benchmark)

## Multi-Node Cassandra

Cassandra supports three deployment modes via `-cluster-mode`. PostgreSQL, MySQL, and MongoDB are always single-node — this section only applies to `-database=cassandra`.

- `local-single` (default) — one `cassandra:5` container on the orchestrator host. Workload runs inside the container. This is the thesis baseline.
- `local-cluster` — three `cassandra:5` containers by default on one Docker network. Only `cassandra-1` publishes 9042 to the host, so all CQL queries are routed through a single coordinator. **For code-correctness validation only — not for performance measurement.**
- `remote-cluster` — real machines reached over SSH (e.g. an HPC allocation). Workload runs natively on the orchestrator and connects to the ring over the network. This is what produces the paper-extension measurements.

See `CLAUDE.md` ("Cluster Modes (Cassandra)") for the deeper rationale on bucketed schema, replication, consistency, and per-node metric aggregation.

### Invocation examples

```bash
# local-single: existing thesis methodology, no flag changes needed
./uuid-benchmark -database=cassandra -scenario=all -num-records=1000000 -num-runs=3 -output=cassandra.csv

# local-cluster: 3-container compose ring, code validation only
./uuid-benchmark -database=cassandra -cluster-mode=local-cluster \
    -scenario=insert-performance -num-records=10000

# remote-cluster: 3 real nodes over SSH (Taurus-style)
./uuid-benchmark -database=cassandra -cluster-mode=remote-cluster \
    -nodes=taurus-01:9042,taurus-02:9042,taurus-03:9042 \
    -ssh-user=$USER -ssh-key=$HOME/.ssh/id_ed25519 \
    -scenario=all -num-records=1000000 -num-runs=3 -output=cassandra-cluster.csv
```

### Sample output (illustrative — numbers are not real measurements)

```
UUID Benchmark - Cassandra
======================================================================
Database:     Cassandra
Cluster mode: remote-cluster
Scenario:     insert-performance
Records:      1000000
Runs:         3 (statistical mode)
Testing:      [SEQUENTIAL UUIDV1 UUIDV4 UUIDV7 ULID ULID_MONOTONIC]
======================================================================

COMPARISON - Insert Performance
=====================================================================================================
Metric              SEQUENTIAL    UUIDV1        UUIDV4        UUIDV7        ULID          ULID_MONO
---------------------------------------------------------------------------------------------------
Throughput          48213 rec/s   42105 rec/s   31872 rec/s   46540 rec/s   46011 rec/s   46324 rec/s
SSTable Delta       12            14            21            13            13            13
SSTable Count       28            31            42            29            29            29
Index Size          18.4 MB       24.1 MB       24.6 MB       24.2 MB       24.2 MB       24.2 MB
Space Amplification 1.08%         1.12%         1.41%         1.10%         1.10%         1.10%
Latency p99         2.4ms         3.1ms         5.8ms         2.7ms         2.7ms         2.7ms
Read MB/s           0.42          0.55          0.71          0.50          0.50          0.50
Write MB/s          18.30         19.10         22.40         18.80         18.80         18.80
```

Cluster-mode runs sum counters (SSTable count, page splits, IO bytes) across all nodes and average ratios (cache hit rate, bloom filter false ratio). Per-node `nodetool` output and per-node cgroup v2 `io.stat` are collected via `docker exec` (local-cluster) or SSH (remote-cluster).

### Security note

SSH to remote nodes uses `ssh.InsecureIgnoreHostKey()`. This is intentional for ephemeral private-VPN clusters where host keys change per allocation (e.g. Taurus). Do not point `-cluster-mode=remote-cluster` at hosts on an untrusted network.

## How It Works

Each database uses a workload tool that runs **inside the Docker container** (localhost connection, zero network overhead):

| Database | Workload Tool | UUID Generation |
|---|---|---|
| PostgreSQL | pgbench with custom SQL scripts | Server-side (PostgreSQL functions) |
| MySQL | Custom Go binary | Client-side (Go UUID/ULID libraries) |
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
| Page splits / compaction | WAL analysis | innodb_metrics | WiredTiger cache splits | SSTable count |
| Fragmentation | pgstatindex | B-tree overhead ratio | freeStorageSize/storageSize | Space amplification |
| Cache hit ratio | pg_stat_database | performance_schema | WiredTiger cache | Key cache (nodetool) |
| Disk size | pg_relation_size | information_schema | collStats | nodetool tablestats |
| Throughput & latency | pgbench | Go workload binary | Go workload binary | Go workload binary |
| I/O | cgroup v2 | cgroup v2 | cgroup v2 | cgroup v2 |

**Key Design Decisions:**
- **Fresh container per UUID type:** Prevents metric contamination between runs
- **Workload inside container:** Eliminates network latency from measurements
- **Custom Go workload binary for MySQL/MongoDB/Cassandra:** Enables proper UUID generation with Go libraries (`github.com/google/uuid`, `github.com/oklog/ulid`) — no existing benchmark tool supports custom UUID key generation for all types
- **Statistical analysis mode:** Multiple runs with Mann-Whitney U tests provide p-values and significance testing

## Plotting

Generate PDF bar charts from benchmark CSV results:

```bash
pip install -r scripts/requirements.txt

# Generate all plots
python3 scripts/plot.py results.csv --output-dir plots/

# Filter by scenario or metric
python3 scripts/plot.py results.csv --scenario insert_performance
python3 scripts/plot.py results.csv --metric p99_latency_us
```

Output: one PDF per (scenario, metric) pair, named `{scenario}_{metric}.pdf`.

## Validation

PostgreSQL results validated against **go-ycsb** (industry-standard benchmark) for overlapping metrics (throughput, latency). Both tools run inside containers with identical architecture (client inside container → localhost). See `validation/` directory.

```bash
cd validation
./run-comparison.sh insert  # Runs both tools, compares sequential int results
```
