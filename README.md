# UUID Benchmark

Benchmarks UUID types (UUIDv1, UUIDv4, UUIDv7, ULID) vs BIGSERIAL in PostgreSQL. Measures page splits, fragmentation, buffer pool efficiency, and throughput.

## Requirements

- Go 1.21+
- Docker & Docker Compose
- Linux (for I/O metrics)

## Build & Run

```bash
go build -o uuid-benchmark cmd/benchmark/main.go

# Run scenario (tests all UUID types automatically)
./uuid-benchmark -scenario=insert-performance -num-records=100000
./uuid-benchmark -scenario=read-after-fragmentation -num-records=1000000
./uuid-benchmark -scenario=mixed-balanced -num-ops=100000 -connections=4
```

## Scenarios

- `insert-performance` - Page splits, fragmentation, disk usage
- `read-after-fragmentation` - Buffer pool hit ratios
- `update-performance` - Update throughput
- `mixed-insert-heavy`, `mixed-read-heavy`, `mixed-balanced` - Mixed workloads

## Output

```
COMPARISON - Insert Performance
======================================
Metric         BIGSERIAL    UUIDV4
--------------------------------------
Throughput     74095 rec/s  65002 rec/s
Page Splits    27           51
Fragmentation  0.00%        48.08%
Index Size     240.0 KB     432.0 KB
```

Each UUID type runs in a fresh PostgreSQL 18 container for isolated measurements.
