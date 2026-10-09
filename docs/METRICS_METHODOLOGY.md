# Metrics methodology

Definitions for the current implementation. Archived measurements may use older
code or protocols; use their recorded configuration when interpreting them.

## Throughput and latency

PostgreSQL uses pgbench with server-side key generation. MySQL, MongoDB and
Cassandra use the Go workload binary with client-side generation. Workloads run
inside the database container, except Cassandra cluster modes, where they run
on the orchestrator over the network.

- Throughput is the reported operation/record count divided by the workload
  duration. Inspect operation counters: a completed request is not necessarily
  a successful insert or a read returning a row.
- PostgreSQL latency percentiles are calculated from pgbench transaction logs.
  The Go binary records elapsed operation times. Bulk-insert samples time
  **batches**, not individual rows; do not compare them directly with lookup latency.
- The separate corrected insert-heavy protocol counts verified successful
  operations. Its PostgreSQL duration includes pgbench startup, connections and
  transaction logging. It is not interchangeable with historical mixed workloads.

A throughput ratio of 3.48 means 3.48 times the baseline throughput, or 248%
more: `(ratio - 1) × 100`. It does not imply a measured latency reduction.
Ratios in the dashboard use medians from the same experimental configuration.

## Storage and structural metrics

| Metric | PostgreSQL | MySQL / InnoDB | MongoDB / WiredTiger | Cassandra |
|---|---|---|---|---|
| Table size | `pg_table_size` | `data_length + index_length` | `collStats.storageSize` | `Space used (live)` |
| Index size | `pg_indexes_size` | `data_length`, including row data | `collStats.totalIndexSize` | Not separately reported (`0`) |
| Split-related field | WAL `Btree/SPLIT_L` + `SPLIT_R` count | `index_page_splits` counter delta | Reconciliation leaf + internal `page multi-block writes` delta | Nonnegative per-node SSTable-count delta, summed |
| Fragmentation field | `pgstatindex.leaf_fragmentation` | Allocated non-leaf share (see below) | `100 × freeStorageSize / storageSize` | `100 × (total − live) / total` |
| Leaf density | `pgstatindex.avg_leaf_density` | Unavailable (`-1`) | Unavailable (`-1`) | Not applicable (`-1`) |

**Storage is not identically defined.** InnoDB's clustered primary-key index
contains the row data, so its reported table/index sizes overlap. MySQL statistics
remain estimates; the collector attempts `ANALYZE TABLE` before reading them.
MongoDB attempts `fsync` before collecting sizes and reconciliation counters.
Cassandra live-space totals include all measured replicas, not unique logical data.
Compression, payload and replication affect every storage comparison.

**Fragmentation is not one cross-engine metric.** PostgreSQL describes leaf-page
order within the index relation file, not filesystem extent layout or wasted-space
percentage. MySQL uses `100 × (size − n_leaf_pages) / size` from
`mysql.innodb_index_stats`. This includes allocated-but-unused pages, not just
internal B-tree nodes; it does not measure tree depth. MongoDB's free-storage
fraction and Cassandra's non-live-space fraction describe different mechanisms.
The Cassandra percentage is not the `total / live` space-amplification ratio.

**SSTable delta is not compaction work.** The shared `PageSplits` field is reused
for Cassandra as `Σ max(0, after_i − before_i)`. Compaction can reduce SSTable
counts, so zero does not mean no activity. Neither this value nor a Bloom-filter
false-positive count directly measures per-read SSTable access or compaction bytes.
Cassandra's shared `LeafPages` field carries the final SSTable count.

## Cache metrics

| Engine | Source and definition |
|---|---|
| PostgreSQL | `pg_stat_database`: hits / (hits + reads); index ratio from `pg_statio_user_tables` |
| MySQL | `performance_schema.global_status`: 1 − buffer-pool reads / read requests |
| MongoDB | `serverStatus.wiredTiger.cache`: 1 − pages read into cache / pages requested |
| Cassandra | `nodetool info`: key-cache hits / requests, with recent-hit-rate fallback |

These counters cover different caches and scopes. Cassandra's key cache is not
a row-data cache; a database cache miss can still be served by the OS page cache.
MySQL and MongoDB counters are cumulative, not automatically workload-only deltas.
Their index-hit field repeats the general cache ratio rather than measuring a
separate index cache. Cassandra node ratios are averaged **without request weighting**.
Missing metrics can fall back to zero with a warning; zero alone is not proof
that no cache hits or structural events occurred.

## Container I/O

Linux cgroup v2 `io.stat` supplies block read/write bytes and operation counters.
The collector calculates snapshot deltas and divides by the snapshot interval
for IOPS and MB/s. Cluster modes combine per-node container counters.

The I/O window surrounds workload execution and may include process startup,
target fetching and background database work outside the timed operation loop.
Later flushes and compactions can fall outside it. Block operations are not
SQL/CQL operations, and the counters exclude I/O served from memory. Check I/O
validity fields and warnings rather than interpreting missing data as zero I/O.

## Comparing runs

Keep dataset size, payload, concurrency, batching, image version, resource limits,
replication, consistency and target selection explicit. Cassandra's normal
read/update path samples targets during insertion; `-head-sampling` restores the
legacy partition-head fetch and its pre-warming effect.

Repeated-run summaries include medians, means, standard deviation and coefficient
of variation. Hypothesis tests do not establish causation or equivalence.
For the selected dashboard experiments, endpoint definitions and statistical
contrasts are available under **Data & methods** in the
[dashboard](https://moguls753.github.io/uuid-benchmark/).

Implementation: [`internal/benchmark/`](../internal/benchmark/),
[`cmd/workload/main.go`](../cmd/workload/main.go),
[`internal/runner/`](../internal/runner/).
