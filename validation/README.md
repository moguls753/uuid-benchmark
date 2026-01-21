# Validation Against go-ycsb

Validates uuid-benchmark measurement methodology against industry-standard go-ycsb.

## Architecture

**Identical execution environment for both tools:**

```
YCSB Validation:
┌────────────────────────────────┐
│  PostgreSQL Container          │
│  go-ycsb → localhost → postgres│  <- Inside container
└────────────────────────────────┘

uuid-benchmark:
┌────────────────────────────────┐
│  PostgreSQL Container          │
│  pgbench → localhost → postgres│  <- Inside container
└────────────────────────────────┘
```

Both tools:
- Run inside fresh containers
- Connect via localhost (zero network overhead)
- Test BIGSERIAL keys (YCSB limitation: can't generate UUIDs/ULIDs)

## Setup

```bash
# Build go-ycsb (one-time)
cd /home/eike/dev/studium/go-ycsb
make

# Build uuid-benchmark
cd /home/eike/dev/studium/uuid-benchmark
go build -o uuid-benchmark cmd/benchmark/main.go
```

## Usage

```bash
cd validation

# Run full comparison (both tools, automatic comparison)
./run-comparison.sh balanced

# Available scenarios: balanced, read_heavy, insert_heavy, mixed_read_heavy
```

Results automatically saved to `results/`. View comparison:
```bash
./compare-results.sh
```

## What Gets Validated

**Overlapping metrics (BIGSERIAL keys):**
- ✓ Throughput (ops/sec)
- ✓ Latency percentiles (p50/p95/p99)

**uuid-benchmark unique metrics:**
- Page splits (WAL analysis)
- Index fragmentation (pgstatindex)
- Buffer pool hit ratios (pg_stat_database)
- Container I/O metrics (cgroups)

## Why Only BIGSERIAL?

YCSB generates numeric keys (1, 2, 3...) suitable for BIGSERIAL, but cannot generate proper UUIDs or ULIDs:
- UUIDv7: Time-ordered, requires specific algorithm
- ULID: Requires specific encoding
- UUIDv4: Random, YCSB can't track for reads/updates

**Validation approach:** Confirm BIGSERIAL measurements match industry standard, then trust uuid-benchmark's extended measurements for other key types.

## Workload Mapping

| uuid-benchmark Scenario | YCSB Workload | Operations |
|-------------------------|---------------|------------|
| `read-after-fragmentation` | `read_heavy` | 100% read |
| `mixed-balanced` | `balanced` | 50% read, 50% update |
| `mixed-insert-heavy` | `insert_heavy` | 90% insert, 10% read |
| `mixed-read-heavy` | `mixed_read_heavy` | 10% insert, 90% read |

## Expected Results

Both tools should show similar ranges for BIGSERIAL:
- Throughput: 30k-50k ops/sec (workload dependent)
- p95 latency: 2-10ms (workload dependent)

Differences expected due to:
- Client implementation (pgbench vs go-ycsb)
- Connection pooling
- Batch sizes

## Files

- `run-ycsb.sh` - Run go-ycsb inside container
- `run-comparison.sh` - Run both tools sequentially
- `compare-results.sh` - Parse and compare outputs
- `workloads/` - YCSB workload configurations
- `results/` - Output files (created on first run)
