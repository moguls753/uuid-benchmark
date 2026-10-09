# Recorded PostgreSQL comparison

Historical result: 10,000 inserts, ten clients, batch size 1, sequential integer
keys. This is one comparison, not a general validation certificate.

| Metric | go-ycsb | uuid-benchmark | Difference |
|---|---|---|---|
| Throughput | 21,477 ops/s | 23,264 records/s | +8.3% |
| Latency p50 | 408 µs | 410 µs | +0.5% |
| Latency p95 | 586 µs | 507 µs | −13.5% |
| Latency p99 | 1,007 µs | 563 µs | −44% |

Throughput and median latency were close in this run. Tail latencies were not.
The comparison does not isolate the cause of those differences or validate
other key types and structural metrics. Page splits, index size and fragmentation
are collected separately by uuid-benchmark and are not checked by go-ycsb.

Sources: [go-ycsb output](results/ycsb_insert_only_20260108_003315.txt),
[uuid-benchmark output](results/uuid_benchmark_insert_only_20260108_003315.txt).
See [comparison scripts and limitations](README.md).
