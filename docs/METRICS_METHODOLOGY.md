# Metrics Methodology

This document describes how each benchmark metric is measured in PostgreSQL and MySQL, the differences between implementations, and important caveats for interpreting results.

## Overview

The benchmark measures several categories of metrics:
1. **Performance**: Throughput, latency, duration
2. **Storage**: Table size, index size
3. **B-tree Health**: Page splits, fragmentation, leaf density
4. **I/O**: Buffer hit ratios, read/write IOPS

---

## 1. Throughput & Duration

### PostgreSQL
- **Tool**: pgbench (PostgreSQL's built-in benchmarking tool)
- **Measurement**: pgbench reports transactions per second (TPS) and total time
- **Latency**: pgbench provides p50, p95, p99 percentiles via `--log` option

### MySQL
- **Tool**: sysbench (industry-standard database benchmarking tool)
- **Measurement**: sysbench reports transactions per second and total execution time
- **Latency**: sysbench provides p50, p95, p99 percentiles in its output

### Comparability
Both tools measure the same concept (operations per second), but implementation details differ:
- pgbench runs inside the PostgreSQL container, connecting via Unix socket
- sysbench runs inside the MySQL container, connecting via TCP localhost

This is comparable because both tools execute operations from within the same container as the database, eliminating network latency as a variable.

---

## 2. Table Size

### PostgreSQL
```sql
SELECT pg_table_size('table_name')
```
- **Type**: Real-time function
- **What it measures**: On-disk size of the table's data (excluding indexes)
- **Accuracy**: Exact - scans filesystem metadata

### MySQL
```sql
SELECT data_length FROM information_schema.tables
WHERE table_schema = 'db' AND table_name = 'table'
```
- **Type**: Cached statistic
- **What it measures**: Estimated size of the clustered index (data + primary key B-tree)
- **Accuracy**: Approximate - based on InnoDB's internal statistics
- **Refresh**: Requires `ANALYZE TABLE` for accurate results after bulk operations

### Key Difference
PostgreSQL's `pg_table_size()` queries the filesystem directly. MySQL's `information_schema.tables` uses cached statistics that InnoDB updates asynchronously. The benchmark explicitly runs `ANALYZE TABLE` before measuring to ensure accuracy.

---

## 3. Index Size

### PostgreSQL
```sql
SELECT pg_indexes_size('table_name')
```
- **What it measures**: Total size of all indexes on the table
- **For primary key**: This is a separate B-tree structure pointing to heap tuples

### MySQL (InnoDB)
```sql
SELECT data_length FROM information_schema.tables
```
- **What it measures**: Size of the clustered index
- **Important**: In InnoDB, the primary key IS the table data (clustered index)

### Key Difference: Clustered vs Non-Clustered Indexes

| Aspect | PostgreSQL | MySQL (InnoDB) |
|--------|-----------|----------------|
| Table storage | Heap (unordered) | Clustered index (ordered by PK) |
| Primary key | Separate B-tree → points to heap | IS the table data |
| `index_length` | Secondary indexes only | Secondary indexes only |
| `data_length` | Table heap | Clustered index (PK + data) |

**Implication**: For a table with only a primary key:
- PostgreSQL: `pg_indexes_size()` returns the PK B-tree size
- MySQL: `data_length` contains both data AND the PK B-tree structure

The benchmark reports `data_length` as "Index Size" for MySQL because the clustered B-tree structure is what we're measuring for UUID vs sequential key performance.

---

## 4. Page Splits

Page splits occur when a B-tree leaf page is full and a new record must be inserted. The page is split into two pages, which can cause:
- Increased tree depth
- Reduced sequential I/O efficiency
- Write amplification

### PostgreSQL
```sql
SELECT SUM(count)::int
FROM pg_get_wal_stats($start_lsn::pg_lsn, $end_lsn::pg_lsn, per_record := true)
WHERE "resource_manager/record_type" IN ('Btree/SPLIT_L', 'Btree/SPLIT_R')
```
- **Extension required**: `pg_walinspect`
- **Method**: Analyzes Write-Ahead Log (WAL) between two Log Sequence Numbers (LSN)
- **Granularity**: Per-operation - counts splits that occurred during the benchmark
- **What SPLIT_L/SPLIT_R mean**: Left and right page splits in B-tree operations

### MySQL
```sql
SELECT count FROM information_schema.innodb_metrics
WHERE name = 'index_page_splits'
```
- **Prerequisite**: Must enable via `--innodb_monitor_enable=index_page_splits`
- **Method**: Global counter incremented on each page split
- **Granularity**: Per-operation - we capture start/end values and calculate delta

### Comparability
Both methods count actual B-tree page splits. PostgreSQL's method is more precise (WAL inspection) while MySQL uses a global counter. Since we run fresh containers per benchmark, the MySQL counter starts at 0 and the delta accurately reflects splits during the test.

---

## 5. Fragmentation

"Fragmentation" in B-tree indexes can mean different things:
1. **Leaf page fragmentation**: Leaf pages physically out of order on disk
2. **Internal fragmentation**: Wasted space within pages (low fill factor)
3. **B-tree overhead**: Ratio of internal (non-leaf) pages to total pages

### PostgreSQL
```sql
SELECT leaf_fragmentation, avg_leaf_density, leaf_pages, empty_pages
FROM pgstatindex('index_name')
```
- **Extension required**: `pgstattuple`
- **leaf_fragmentation**: Percentage of leaf pages that are physically out of order
- **avg_leaf_density**: Average percentage of space used in leaf pages
- **leaf_pages**: Total number of leaf pages
- **empty_pages**: Number of completely empty pages

**What leaf_fragmentation measures**: If leaf pages are numbered 1,2,3,4,5 but stored on disk as 3,1,5,2,4, the fragmentation is high. This affects sequential scan performance because the disk must seek between non-adjacent pages.

### MySQL
```sql
SELECT
    MAX(CASE WHEN stat_name = 'n_leaf_pages' THEN stat_value END) as leaf_pages,
    MAX(CASE WHEN stat_name = 'size' THEN stat_value END) as total_pages
FROM mysql.innodb_index_stats
WHERE database_name = ? AND table_name = ? AND index_name = 'PRIMARY'
```
- **What we calculate**: `(total_pages - leaf_pages) / total_pages * 100`
- **Meaning**: Percentage of B-tree pages that are internal (non-leaf) nodes

**Important**: This is NOT the same metric as PostgreSQL's `leaf_fragmentation`.

### What MySQL's "Fragmentation" Actually Shows

| Metric | PostgreSQL | MySQL |
|--------|-----------|-------|
| Name | leaf_fragmentation | B-tree overhead |
| Measures | Physical page ordering | Internal node ratio |
| Low value means | Pages in sequential order | Shallow, efficient tree |
| High value means | Pages scattered on disk | Deep tree, more internal nodes |

### Why MySQL Cannot Measure Physical Fragmentation

InnoDB stores data in tablespace files (`.ibd`) and doesn't expose the physical page ordering to SQL queries. Tools like `innodb_ruby` can analyze `.ibd` files directly, but this requires:
- Stopping the database or using file snapshots
- Parsing binary InnoDB page format
- Not practical for automated benchmarks

### Interpreting the MySQL Metric

The B-tree overhead percentage is still useful:
- **Sequential inserts** (BIGSERIAL): Create balanced trees with minimal internal nodes (~3%)
- **Random inserts** (UUIDv4): Cause page splits, creating more internal nodes (~44%)

Higher overhead indicates:
- More page splits occurred during inserts
- Deeper B-tree structure
- More I/O required to traverse from root to leaf

---

## 6. Leaf Density

### PostgreSQL
```sql
SELECT avg_leaf_density FROM pgstatindex('index_name')
```
- **What it measures**: Average percentage of each leaf page that contains actual data
- **Range**: 0-100%
- **Typical values**: 70-90% (PostgreSQL default fillfactor is 90%)

### MySQL
- **Not directly available**: InnoDB doesn't expose per-page fill statistics
- **Approximation**: We report 90% as InnoDB targets ~15/16 (93.75%) fill factor

### Why This Matters

Low leaf density means:
- More pages needed to store the same data
- More I/O for sequential scans
- Often caused by random inserts and page splits

---

## 7. Buffer Hit Ratio

Measures how often requested pages are found in memory vs read from disk.

### PostgreSQL
```sql
-- Database-level buffer hit ratio
SELECT blks_hit::float / NULLIF(blks_hit + blks_read, 0)
FROM pg_stat_database WHERE datname = 'uuid_benchmark'

-- Index-specific buffer hit ratio
SELECT idx_blks_hit::float / NULLIF(idx_blks_hit + idx_blks_read, 0)
FROM pg_statio_user_tables WHERE relname = 'table_name'
```

### MySQL
```sql
SELECT
    (SELECT variable_value FROM performance_schema.global_status
     WHERE variable_name = 'Innodb_buffer_pool_read_requests') as requests,
    (SELECT variable_value FROM performance_schema.global_status
     WHERE variable_name = 'Innodb_buffer_pool_reads') as disk_reads
-- hit_ratio = (requests - disk_reads) / requests
```

### Comparability
Both measure the same concept. PostgreSQL provides per-table granularity; MySQL provides global buffer pool statistics. For benchmarks with a single table, these are comparable.

---

## 8. I/O Statistics (IOPS, MB/s)

### Both Databases
Measured externally using `iostat` on the container's block device:
```bash
iostat -dx 1 $duration
```

This measures:
- **Read IOPS**: Read operations per second
- **Write IOPS**: Write operations per second
- **Read MB/s**: Read throughput
- **Write MB/s**: Write throughput

### Comparability
Identical measurement method for both databases - external observation of block device activity.

---

## Summary: Metric Equivalence

| Metric | PostgreSQL | MySQL | Equivalent? |
|--------|-----------|-------|-------------|
| Throughput | pgbench TPS | sysbench TPS | Yes |
| Latency | pgbench percentiles | sysbench percentiles | Yes |
| Table Size | pg_table_size() | data_length + index_length | Yes |
| Index Size | pg_indexes_size() | data_length (clustered) | Comparable* |
| Page Splits | WAL inspection | innodb_metrics counter | Yes |
| Fragmentation | Physical page order | B-tree overhead ratio | No** |
| Leaf Density | pgstatindex | Not available (estimated) | No |
| Buffer Hit | pg_stat_database | performance_schema | Yes |
| IOPS | iostat | iostat | Yes |

\* MySQL's clustered index means "index size" includes row data in the B-tree
\** Different metrics - both useful but measure different aspects of B-tree health

---

## Recommendations for Thesis

1. **When comparing fragmentation**: Note that PostgreSQL measures physical page ordering while MySQL measures B-tree structural overhead. Both indicate B-tree health but are not directly comparable.

2. **Index size interpretation**: Explain that InnoDB's clustered index architecture means the primary key B-tree IS the table, while PostgreSQL separates heap storage from index storage.

3. **Page splits are comparable**: Both databases count actual B-tree page splits, making this a valid cross-database comparison metric.

4. **Throughput is comparable**: Despite using different tools (pgbench vs sysbench), both measure operations per second under similar conditions (in-container execution, same workload patterns).
