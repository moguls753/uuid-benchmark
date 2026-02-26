# Benchmark Anomalies & Open Issues

Status: tracking anomalies found during thesis data review (2026-02-26)

## Action items requiring Taurus cluster

- **MySQL 1M 8conn insert CVs (#1/#7)**: Rerun all databases at 1M 8conn insert-performance on Taurus to isolate InnoDB's true CV from laptop noise. If MySQL still shows 5–8% while others are <2%, report as InnoDB finding.
- **All 8conn benchmarks**: Consider rerunning full `-scenario=all` at 8conn for all 4 databases on Taurus for a clean, controlled baseline. PostgreSQL 8conn mixed scenarios also had 5–6% CVs on laptop.

## Resolved

### 5. ULID reads worse than UUIDv4 in PostgreSQL
- ULID: 20,828 reads/s (p50=46us), UUIDv4: 22,263 reads/s (p50=42us)
- Nearly identical index size (~38 MB both), yet ULID 6.4% slower
- Cause: pgx_ulid `ulid` type is varlena (`typlen=-1`), custom operator class comparison overhead vs native UUID's built-in `memcmp`. ~4us extra per read at p50.
- vs UUIDv7: index size difference is the primary factor (38.66 vs 30.10 MB)

### 6. ULID page splits = UUIDv4 despite perfect ordering
- ULID: 4,944 splits (90% density, varlena type), UUIDv4: 4,872 splits (71% density, native UUID)
- Coincidence from two independent mechanisms: UUIDv4 splits from low density, ULID splits from larger per-entry varlena overhead (~9 bytes/entry)
- Proof: ULID_MONO has same split count (4,955) — confirms it's entry size, not randomness
- Root cause: `struct ulid(u128)` stored as varlena with header, native UUID is fixed-length 16 bytes

## Open

### 1. CV claim overstated
- **Problem**: Thesis claims "CV < 2% across all results" — false
- **Data**: MySQL 8conn insert_performance CVs at 1M: 8–22% across 3 independent reruns. Other databases on same laptop: MongoDB 0.5–2%, Cassandra 2–5%, PostgreSQL 1.4–5.6%.
- **Root cause**: Likely a combination of (1) InnoDB adaptive flushing variability at 1M 8conn scale and (2) laptop environmental noise. MySQL is consistently 2–4x worse than other databases on the same machine, suggesting a real InnoDB component. 100K 8conn showed low CVs (1.5–3.5%) — either laptop was quieter or workload too short to trigger InnoDB flush storms.
- **Fix needed**: Correct the "CV < 2%" claim. Frame as: "CV < 2% for single-connection and most multi-connection results. MySQL 8-connection insert performance shows elevated variance (8–22% CV), likely reflecting InnoDB's adaptive flushing behavior under sustained concurrent writes, potentially amplified by the non-isolated test environment. Dedicated server reruns are planned to isolate the InnoDB component."

### 2. Bloom filter FP = 0 in Cassandra
- **Problem**: Thesis explains this as "STCS compaction resolved SSTable overlap" — wrong
- **Correct explanation**: Bloom filters operate on partition keys. Schema uses `PRIMARY KEY ((bucket), id)` with `bucket=1` for all rows. Every SSTable contains the sole partition, so FP = 0 is trivially expected.
- **Thesis text**: "Bloom filter false positive rate was 0 for all key types at all scales. This is expected given the single-partition schema (`bucket=1`) — bloom filters operate on partition keys, and every SSTable contains the sole partition. This metric would become relevant in a multi-partition or multi-node setup."

### 3. UUIDv1 10M catastrophe
- **Problem**: Thesis agent dismissed UUIDv1 throughput gap as "measurement artifact" — partially wrong. Generation overhead is real at all scales, byte-ordering degradation is real at 10M.
- **Penalty 1 — Generation overhead (constant, all scales)**:
  - At 1M: UUIDv1 B-tree metrics identical to UUIDv7 (3,849 splits, 0% frag, 89.98% density, 30.10 MB index) but 23% lower throughput (26,949 vs 34,993). Same pattern at 100K (26,673 vs 34,401).
  - Cause: `uuid_generate_v1()` server-side cost (MAC lookup, clock sequence management). Pure generation overhead, not B-tree behavior.
- **Penalty 2 — Byte-ordering degradation (scale-dependent, 10M only)**:
  - RFC 4122 places `time_low` (least significant 32 bits of 60-bit timestamp) in bytes 0–3. PostgreSQL B-trees sort by raw byte order. `time_low` wraps every ~429 seconds (2^32 × 100ns).
  - After wrap: new UUIDs inserted into earlier B-tree positions → page splits in non-tail pages.
  - 10M data: 65,277 splits (median), 53% leaf density, 510 MB index — **worse than UUIDv4** (52,688 splits, 65% density, 429 MB).
  - Bimodal runs: early wrap → more damage (runs 1–3: 65K–68K splits, 51–53% density, 510–531 MB), late wrap → less damage (runs 4–5: 40K splits, 85% density, 315–319 MB).
  - UUIDv1 is the only type whose B-tree behavior degrades non-linearly with scale — transitions from UUIDv7-like to worse-than-UUIDv4 once workload exceeds the `time_low` wrap interval.
- **Cross-database confirmation**:
  - Cassandra: UUIDv1 (`timeuuid`) matches UUIDv7 throughput (67,289 vs 66,915 at 1M) — sorts by extracted timestamp, not byte order. No penalty.
  - MySQL: client-side generation equalizes overhead with UUIDv7 (33,925 vs 33,705 at 1M). Byte ordering not yet wrapped at 1M.
  - MongoDB: UUIDv1 clusters with time-ordered types (192,034 vs SEQUENTIAL 207,622 at 1M).

### 4. ULID 8conn behavior — two compounding effects
- **Data at 8conn**:
  - UUIDv4: 4,808 splits, 49.80% frag, 37.59 MB index, 83,133 throughput
  - ULID: 6,193 splits, 28.43% frag, 48.41 MB index, 86,990 throughput
  - ULID_MONO: 4,955 splits, 0% frag, 38.74 MB index, 87,940 throughput
- **Two compounding effects**:
  1. **Varlena overhead** (always present): ULID_MONO baseline is already larger than UUIDv4 (38.74 vs 37.59 MB) due to ~9 bytes/entry varlena storage. This is the same effect as resolved anomalies #5/#6.
  2. **Concurrency randomness** (8conn only): 8 independent `gen_ulid()` calls produce random values within the same millisecond → 28.43% fragmentation, inflating index to 48.41 MB and causing extra splits. `gen_monotonic_ulid()` avoids this via shared state (`shared_preload_libraries`).
- **Result**: ULID at 8conn is worse than UUIDv4 on page splits (6,193 vs 4,808) AND index size (48.41 vs 37.59 MB), but better on fragmentation (28.43% vs 49.80%). UUIDv4 has worse fragmentation but smaller per-entry size, so its absolute index is still smaller. ULID gets hit by both problems at once.
- **At 1conn**: ULID and ULID_MONO are identical (4,944 vs 4,955 splits, both 0% frag) — concurrency effect disappears.

### 7. MySQL 8conn insert CVs — partially resolved
- **100K rerun**: CVs of 1.5–3.5% (workload too short to trigger flush storms)
- **Three 1M reruns on laptop**: CVs remain 3–22%, varying randomly across key types per run
- **Cross-database comparison**: MySQL consistently 2–4x worse CV than PG/MongoDB/Cassandra on same hardware → real InnoDB component
- **Action needed**: Rerun on Taurus cluster to isolate InnoDB's true CV from laptop noise. If MySQL still shows 5–8% while others are <2%, report as InnoDB finding.

### 8. Cassandra reads: near-zero differentiation
- **At 1M**: All types ~814 ops/s, indistinguishable. Cache hit ratio 1.00, bloom filter FP = 0.
- **At 10M**: Small but statistically significant gap emerges. Ordered types (SEQUENTIAL, UUIDv7, ULID, ULID_MONO) at 810–812 ops/s. UUIDv4 at 789 ops/s (~3% slower, CV 0.73%). UUIDv1 at 803 ops/s. Cache hit ratio still 1.00 everywhere.
- **SSTable count**: 4 for all types at 10M (STCS compaction produces equal counts). SSTable sizes and key range overlap differ by key type but were not directly measured.
- **Likely mechanism**: Read amplification from overlapping key ranges. Random UUIDv4 inserts produce SSTables with overlapping ranges; ordered types produce SSTables with distinct ranges. Even with all data in cache, checking more SSTables per read adds index lookup overhead. Direct per-read SSTable access counts were not captured.
- **Bloom filters don't help**: Single-partition schema (`bucket=1`) means bloom filters return "present" for every SSTable regardless of key type (see #2). Bloom filters cannot narrow down which SSTable contains a specific clustering key.
- **Thesis text**: "At 10M, UUIDv4 shows a small but statistically significant read throughput reduction (~3%) compared to time-ordered types, despite identical SSTable counts (4) and 100% cache hit ratios. This is consistent with read amplification from overlapping key ranges in UUIDv4's SSTables, though SSTable overlap and per-read access counts were not directly measured."

### 9. MongoDB mixed_insert_heavy: sequential slower than UUIDs — TO BE RESEARCHED
- **Data at 1conn**: SEQUENTIAL 12,449 < UUIDv4 15,369 < UUIDv7 16,063. Same pattern at 8conn (39,527 < 46,471 < 47,131).
- **Reversal is specific to mixed_insert_heavy only**:
  - Pure insert: SEQUENTIAL fastest (207,622 vs UUIDv4 148,450)
  - Pure read: all equal (~17,100–17,250)
  - Mixed read+update (50/50): SEQUENTIAL fastest (13,758 vs UUIDv4 13,325)
  - Mixed insert+read (70/30): SEQUENTIAL **slowest** (12,449 vs UUIDv4 15,369)
- **NOT tail-append contention**: happens at 1conn, so no concurrency involved. Earlier explanation was wrong.
- **Likely mechanism**: interleaving new sequential inserts (all targeting rightmost leaf) with random reads causes WiredTiger page management overhead (split/reconciliation during read traversal). Random inserts spread this overhead across the tree. But no WiredTiger internal metrics to confirm.
- **Action needed**: Dive deeper into WiredTiger internals. Possible approaches: WiredTiger statistics (`db.serverStatus().wiredTiger`), page eviction metrics, or reproduce with WiredTiger verbose logging.

### 10. Cassandra status in thesis
- **Problem**: thesis-writing.md still says Cassandra results are "re-running / TBD"
- **Reality**: All Cassandra results are complete — 1M (1conn + 8conn), 10M (insert, read, update). Available in `results/cassandra_*.csv`.
- **Fix needed**: Update thesis-writing.md to reflect complete data availability. All 4 databases are done.
