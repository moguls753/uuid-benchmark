# Paper Notes

Accumulating methodology notes, design-decision rationales, and observation paragraphs to be drawn on while writing the multi-node Cassandra paper extension. Each entry is paper-ready prose plus a short header explaining when/where to use it.

---

## Methodology

### Schema design — bucketed partitioning with hash-derived bucket (use in: methodology section, paragraphs 1–2)

The thesis benchmarked Cassandra UUID-ordering effects under a single-partition schema (`PRIMARY KEY ((bucket), id)` with the `bucket` partition-key column held constant at `1` for all rows). This design pinned every row to the same Cassandra partition, allowing the UUID — used as the clustering column — to drive MemTable and SSTable ordering directly: time-ordered UUIDs (UUIDv7, ULID) produced append-only inserts within the partition, while random UUIDs (UUIDv4) produced inserts at scattered offsets. The resulting metric differences (compaction work, SSTable count, bloom-filter behaviour) cleanly isolated the UUID-ordering variable. For a single-node benchmark this design is methodologically sound, but the construction is incompatible with horizontal scaling: any cluster sized larger than one node would still place every row on the single replica owning the token for `bucket=1`, leaving the remaining nodes idle.

To extend the thesis to a distributed setting we evaluated three schema options:

1. **Retain `bucket=1`.** Trivially preserves the thesis's measurement methodology but eliminates distribution. The cluster behaves as a single node regardless of replica count, which makes the multi-node extension vacuous.
2. **Promote `id` to the partition key (`PRIMARY KEY (id)`).** Cassandra's Murmur3 partitioner hashes the partition-key value before placement, producing uniform distribution across the ring. However, Murmur3 destroys the UUID's byte ordering during placement: UUIDv4 and UUIDv7 produce statistically indistinguishable token distributions, and within each (now single-row) partition there is no clustering column for the UUID-ordering variable to affect. The construction trades distribution for the destruction of the very variable the benchmark measures.
3. **Spread `bucket` across N values via a deterministic hash of `id`.** We use `bucket = FNV-1a(id_bytes) mod N` with N=1000 by default. The 32-bit FNV-1a is sufficient for non-adversarial inputs; the modulo bias at N=1000 is on the order of 10⁻⁷ per bucket, well below the noise floor of any metric we measure. The hash is computed deterministically from the id alone, so reads and updates recompute the bucket at query time without needing to remember (bucket, id) pairs.

The bucketed schema (option 3) preserves the thesis's methodological core: the `PRIMARY KEY ((bucket), id)` DDL is unchanged, the UUID remains the clustering column, and within each partition MemTable and SSTable layout are still driven by UUID byte ordering. The single change is that `bucket` now varies across N values rather than being held constant at 1. The thesis is the degenerate case of this design (N=1); the multi-node extension is the same study with N>1. Distribution emerges via Murmur3 hashing of the partition-key value, while the within-partition UUID-ordering effect is preserved by construction. Direct comparison to the thesis numbers is not made — the partition-size dimension changes simultaneously with cluster size, so a separate single-node baseline run under the bucketed schema is established as the comparison anchor for multi-node results.

**Origin:** core methodological decision of the paper extension. Made after a design discussion that initially proposed option 2 (later identified as flawed) and was revised to option 3.

---

### Cassandra `batch_size_fail_threshold` (use in: setup / methodology section)

Cassandra's `batch_size_fail_threshold` was raised from the default 50 KiB to 200 KiB to permit the same 100-row × 1024-byte batch sizing used across all four databases. The default threshold is a safeguard against multi-partition unlogged batches, which the bucketed schema produces by design. Single-partition batches (the thesis's `bucket=1` configuration) are exempt from the threshold and ran cleanly at the default. The threshold change is a benchmark accommodation, not a production recommendation.

**Origin:** discovered during the Task 1.6 single-node smoke test. Without the bump, every multi-partition batch is rejected with `Batch ... is of size 108.xxx KiB, exceeding specified threshold of 50.000KiB`. Implemented as a `sed` line in the `command:` block of `docker/docker-compose.cassandra.yml`.

---

## Limitations and caveats

### `fetchCassandraIDs` token-order sampling skew (use in: limitations / threats-to-validity section)

The benchmark's read and update workloads sample a target id set via `SELECT id FROM bench LIMIT M`. Cassandra returns rows in token order and terminates the scan once M rows are collected. When M is small relative to per-partition row count, the sample concentrates within the first few partitions encountered in token order, which in turn concentrate on the nodes owning those tokens. At 100M-record / 1000-bucket scale each partition holds approximately 100K rows; a sample of M=10K ids therefore comes from a single partition and exercises read load on at most one or two replicas rather than the full cluster.

We retain the simpler unfiltered scan to keep the implementation minimal and to mirror Cassandra's idiomatic LIMIT-without-WHERE semantics. A stratified alternative (iterating buckets 0..N-1 and fetching M/N ids from each) would spread the load uniformly but adds N CQL round-trips per workload setup. For our scale runs we report the sample's bucket-diversity statistic and interpret per-node read variance accordingly. Where multi-node read variance is suspiciously low, the result reflects the sampling concentration rather than per-node behaviour.

**Origin:** flagged by the Task 1.5 code-quality reviewer; empirically confirmed during the Task 1.6 smoke test (5K-row run produced 202 distinct buckets across a 1000-row sample — the dilution at small partition sizes hides the issue, but at 100K rows-per-partition it concentrates sharply).

---

### Cluster-wide metric aggregation: sums for counters, unweighted means for ratios (use in: methodology / limitations section)

Multi-node Cassandra metrics are aggregated from per-node `nodetool tablestats` and `nodetool info` snapshots before being reported. Counters (`SSTable count`, `Space used`, `Memtable switch count`, `Bloom filter false positives`, bytes compacted) are summed across nodes; ratios (`Bloom filter false ratio`, `Key Cache hit rate`) are averaged with equal weight per node.

The unweighted mean for `Key Cache hit rate` is a deliberate simplification with one known limitation: if read load is concentrated on a subset of nodes (e.g. due to the token-order sampling skew documented above), nodes serving few requests contribute equally to the mean as nodes serving many. A request-weighted mean would require parsing the `H hits, R requests` counters per node and computing `sum(H) / sum(R)` directly; we chose the simpler arithmetic mean to keep the aggregation pure (one function, no per-metric special cases) and because the token-skew caveat already discloses the same load-concentration issue at the workload-design layer. Where multi-node key-cache hit-rate numbers look implausibly high or low, the load distribution should be checked first.

**Origin:** noted by the holistic Phase 1-6 reviewer as a methodology disclosure to make explicit before production runs.

---

### `PageSplits` clamping for Cassandra (use in: results / methodology footnote)

The cross-database `BenchmarkResult.PageSplits` field is repurposed for Cassandra as the per-workload SSTable-count delta (after — before, where both are cluster-wide aggregates from `nodetool tablestats`). The delta is clamped to a non-negative value before being reported: Cassandra's SSTable count can legitimately decrease across a workload if compaction merges more files than the workload created (typical for read-heavy or balanced mixed workloads). The clamp keeps the field semantically consistent with the B-tree databases' "page splits performed" — a count of structural change events triggered by the workload — rather than reporting a negative number that would be a category error for the other databases sharing the column.

The trade-off is that a workload that strongly triggers compaction (e.g. a UUIDv4 insert burst that creates many SSTables, followed by mixed reads that interleave compaction) will report `PageSplits=0` rather than the signed delta. The signed compaction signal is recoverable from the `nodetool compactionhistory` snapshots if the paper needs it, but it is not surfaced in the default CSV.

**Origin:** noted by the holistic Phase 1-6 reviewer; the clamping itself predates the multi-node extension (inherited from the single-node code path), but the cluster-wide aggregation makes it more visible because compactions across N nodes net out more often than on a single node.

---
