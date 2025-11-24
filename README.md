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

## Options

- `-scenario` - Scenario to run (required)
- `-num-records` - Dataset size for insert scenarios (default: 100000)
- `-num-ops` - Number of operations for read/update/mixed (default: 10000)
- `-connections` - Concurrent workers (default: 1)
- `-batch-size` - Records per transaction (default: 100)

## Scenarios

- `insert-performance` - Page splits, fragmentation, disk usage
- `read-after-fragmentation` - Buffer pool hit ratios
- `update-performance` - Update throughput
- `mixed-insert-heavy`, `mixed-read-heavy`, `mixed-balanced` - Mixed workloads
