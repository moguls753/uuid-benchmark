# Cassandra measurement notes

## Partitioning

The normal benchmark uses `PRIMARY KEY ((bucket), id)` with
`bucket = FNV-1a(id_bytes) mod N` (`-num-buckets=1000` by default). Partition keys
are distributed by Cassandra's partitioner; the identifier remains a clustering
column so its ordering can affect layout within each partition.

The historical single-partition runs and corrected insert-heavy protocol use
fixed `bucket = 1`. A single partition is held by its replica set, not necessarily
one node; it cannot distribute independent partitions across the ring.
`-num-buckets=1` hashes to bucket 0 and does not recreate that exact schema usage.
A bucketed single-node run is a separate baseline, not an equivalent historical run.

Ordering, partition size, representation and payload can all affect the results.
The measurements do not by themselves isolate a storage-engine mechanism.

## Batching

The normal bucketed workload uses multi-partition unlogged batches. The supplied
Cassandra configurations raise `batch_size_fail_threshold` to 200 KiB to accept
100-row batches with 1 KiB payloads. This is a benchmark setting, not a production
batching recommendation.

## Read/update targets

The current path uniformly samples distinct insertion positions, records IDs
from successful batches, shuffles them and checks the target-file digest between
load and measurement. Preparation uses eight writers, independently of measured
read/update concurrency.

`-head-sampling` instead fetches the smallest clustering keys in each partition.
This favors older rows for time-ordered schemes and warms the fetched targets.
It changes both the target population and read order, so differences cannot be
attributed to key type alone. Older notes about an unfiltered token-order `LIMIT`
scan do not describe the current sampler.

## Cluster metrics

Counters and storage totals are summed over nodes; key-cache and Bloom-filter
ratios use unweighted node means. A lightly loaded node therefore contributes
as much to a ratio as a busy one. Storage includes replicas.

SSTable-count and Bloom-filter false-positive deltas are clamped **per node before
summing**. The shared `PageSplits` field contains the SSTable delta, not a split
or compaction event count. A zero value does not imply zero compaction. Compaction
history and bytes compacted are not exported by this collector.

Reads can overlap background flushes and compaction; these are not necessarily
steady-state measurements. See [Metrics methodology](METRICS_METHODOLOGY.md) for
cache definitions, I/O windows and cross-engine limitations.
