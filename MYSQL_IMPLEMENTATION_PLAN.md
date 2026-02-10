# MySQL Implementation Plan

## Overview

Extend uuid-benchmark to support MySQL, validating that PostgreSQL findings hold across relational databases.

## Research Summary

### MySQL Capabilities

| Feature | MySQL Status | PostgreSQL Equivalent |
|---------|-------------|----------------------|
| Benchmark tool | sysbench | pgbench |
| UUID type | `BINARY(16)` (no native) | `uuid` (native) |
| UUIDv7 | Not native - need workaround | `uuidv7()` native in PG17 |
| UUIDv4 | `UUID()` generates v1, need `UUID_TO_BIN(UUID())` or random | `gen_random_uuid()` |
| Page splits | `information_schema.innodb_metrics` | `pg_walinspect` |
| Fragmentation | `data_free` from `information_schema.tables` | `pgstatindex()` |
| Buffer hit ratio | `SHOW GLOBAL STATUS` | `pg_stat_database` |

### UUIDv7 Generation Options

1. **Third-party component**: https://github.com/lefred/mysql-component-uuid_v7
   - Requires: `INSTALL COMPONENT "file://component_uuid_v7"`
   - Provides: `uuid_v7()` function
   - Problem: Need to build/install component in Docker image

2. **SQL-based generation** (recommended for simplicity):
   ```sql
   -- UUIDv7 generation in pure SQL
   SELECT CONCAT(
     HEX(FLOOR(UNIX_TIMESTAMP(NOW(3)) * 1000)),  -- 48-bit timestamp
     LPAD(HEX(FLOOR(RAND() * 0xFFF) | 0x7000), 4, '0'),  -- version 7 + 12-bit random
     LPAD(HEX(FLOOR(RAND() * 0x3FFFFFFFFFFFFFFF) | 0x8000000000000000), 16, '0')  -- variant + 62-bit random
   );
   ```

3. **Client-side generation in Go** (most reliable):
   - Use `github.com/gofrs/uuid` with v7 support
   - Generate in Go, insert as `BINARY(16)`

### Key Metrics Available

```sql
-- Page splits (global counter)
SELECT COUNT FROM information_schema.innodb_metrics
WHERE NAME = 'index_page_splits';

-- Fragmentation estimate
SELECT
    TABLE_NAME,
    DATA_LENGTH,
    DATA_FREE,
    (DATA_FREE / DATA_LENGTH) * 100 AS fragmentation_pct
FROM information_schema.tables
WHERE TABLE_SCHEMA = 'uuid_benchmark';

-- Buffer pool hit ratio
SHOW GLOBAL STATUS LIKE 'Innodb_buffer_pool_read%';
-- Calculate: (read_requests - reads) / read_requests * 100

-- Index size
SELECT
    INDEX_NAME,
    STAT_VALUE * @@innodb_page_size AS size_bytes
FROM mysql.innodb_index_stats
WHERE stat_name = 'size';
```

## Implementation Steps

### Phase 1: Infrastructure

- [ ] Create `docker/docker-compose.mysql.yml`
- [ ] Create MySQL Docker image with configuration
- [ ] Add MySQL container management in `internal/container/`
- [ ] Add MySQL connection handling in `internal/benchmark/mysql/`

### Phase 2: Schema & ID Generation

- [ ] Create table schemas for each key type:
  - `bigint AUTO_INCREMENT` (baseline)
  - `BINARY(16)` with UUIDv4
  - `BINARY(16)` with UUIDv7
  - `BINARY(16)` with ULID
  - `BINARY(16)` with UUIDv1

- [ ] Implement ID generation strategy:
  - Option A: Client-side Go generation (recommended)
  - Option B: SQL stored procedures
  - Option C: MySQL component (complex Docker setup)

### Phase 3: Benchmark Execution

- [ ] Decide on benchmark tool:
  - **Option A: sysbench** (industry standard for MySQL)
  - **Option B: Custom Go** (like PostgreSQL pgbench wrapper)

- [ ] Implement workloads:
  - Insert performance
  - Read after fragmentation
  - Update performance
  - Mixed workloads

### Phase 4: Metrics Collection

- [ ] Implement `internal/benchmark/mysql/metrics.go`:
  - Page splits from `innodb_metrics`
  - Fragmentation from `information_schema.tables`
  - Buffer pool hit ratio from `GLOBAL STATUS`
  - Index/table size

### Phase 5: Integration

- [ ] Add MySQL to main benchmark runner
- [ ] Update display tables for MySQL results
- [ ] Add MySQL scenarios to CLI

## Directory Structure

```
internal/
├── benchmark/
│   ├── postgres/          # Existing
│   │   ├── connection.go
│   │   ├── metrics.go
│   │   └── ...
│   └── mysql/             # New
│       ├── connection.go
│       ├── metrics.go
│       ├── insert.go
│       ├── read.go
│       ├── update.go
│       └── mixed.go
├── container/
│   ├── postgres.go        # Existing
│   └── mysql.go           # New
docker/
├── docker-compose.postgres.yml  # Existing
└── docker-compose.mysql.yml     # New
```

## Key Decisions Needed

### 1. Benchmark Tool: sysbench vs Custom Go

| Aspect | sysbench | Custom Go |
|--------|----------|-----------|
| Industry standard | Yes | No |
| Latency percentiles | Built-in | Manual from logs |
| Custom workloads | Lua scripts | Native Go |
| Consistency with PG | Different tool | Same approach |
| Implementation effort | Learn sysbench | Reuse PG code |

**Recommendation**: Custom Go (consistent with PostgreSQL approach)

### 2. UUIDv7 Generation: Client vs Server

| Aspect | Client (Go) | Server (SQL/Component) |
|--------|-------------|------------------------|
| Reliability | High | SQL is hacky, component needs Docker build |
| Consistency | Same IDs as PG | MySQL-specific |
| Performance | Network overhead | Server-side |
| Thesis validity | Comparable to PG | Different generation |

**Recommendation**: Client-side Go generation
- More reliable
- Thesis compares UUID *types*, not generation methods
- Can use same Go UUID libraries for both DBs

### 3. Storage Format: BINARY(16) vs CHAR(36)

| Aspect | BINARY(16) | CHAR(36) |
|--------|------------|----------|
| Size | 16 bytes | 36 bytes |
| Performance | Better | Worse |
| Best practice | Yes | No |

**Recommendation**: BINARY(16) (industry best practice)

## Schema Definitions

```sql
-- BIGSERIAL equivalent
CREATE TABLE bench_bigserial (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    data TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- UUID tables (all use BINARY(16))
CREATE TABLE bench_uuidv4 (
    id BINARY(16) PRIMARY KEY,
    data TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE bench_uuidv7 (
    id BINARY(16) PRIMARY KEY,
    data TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE bench_ulid (
    id BINARY(16) PRIMARY KEY,
    data TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE bench_uuidv1 (
    id BINARY(16) PRIMARY KEY,
    data TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;
```

## Metrics Queries

```sql
-- Reset page split counter before test
-- (Note: Cannot reset in standard MySQL, only Percona)
-- Alternative: Record before/after values

-- Get page splits
SELECT COUNT
FROM information_schema.innodb_metrics
WHERE NAME = 'index_page_splits';

-- Get index fragmentation (approximate)
SELECT
    TABLE_NAME,
    ROUND((DATA_FREE / (DATA_LENGTH + INDEX_LENGTH)) * 100, 2) AS fragmentation_pct
FROM information_schema.tables
WHERE TABLE_SCHEMA = 'uuid_benchmark'
  AND TABLE_NAME LIKE 'bench_%';

-- Get buffer pool hit ratio
SELECT
    (1 - (
        (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_reads') /
        (SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME = 'Innodb_buffer_pool_read_requests')
    )) * 100 AS buffer_hit_ratio;

-- Get index size
SELECT
    TABLE_NAME,
    INDEX_LENGTH
FROM information_schema.tables
WHERE TABLE_SCHEMA = 'uuid_benchmark'
  AND TABLE_NAME LIKE 'bench_%';
```

## Docker Configuration

```yaml
# docker/docker-compose.mysql.yml
version: '3.8'
services:
  mysql:
    image: mysql:8.0
    container_name: uuid-bench-mysql
    environment:
      MYSQL_ROOT_PASSWORD: root123
      MYSQL_DATABASE: uuid_benchmark
      MYSQL_USER: benchmark
      MYSQL_PASSWORD: benchmark123
    ports:
      - "3306:3306"
    command:
      - --innodb-buffer-pool-size=1G
      - --innodb-log-file-size=256M
      - --innodb-flush-log-at-trx-commit=1
      - --performance-schema=ON
    volumes:
      - mysql_data:/var/lib/mysql
    tmpfs:
      - /tmp

volumes:
  mysql_data:
```

## Timeline Estimate

| Phase | Tasks | Effort |
|-------|-------|--------|
| Phase 1 | Docker + container management | 1-2 days |
| Phase 2 | Schema + ID generation | 1 day |
| Phase 3 | Benchmark execution | 2-3 days |
| Phase 4 | Metrics collection | 1-2 days |
| Phase 5 | Integration + testing | 1-2 days |
| **Total** | | **6-10 days** |

## Limitations vs PostgreSQL

| Metric | PostgreSQL | MySQL | Impact |
|--------|------------|-------|--------|
| Page splits | Exact count via WAL | Global counter only | Cannot isolate per-table |
| Fragmentation | `pgstatindex()` per index | Estimate from `data_free` | Less precise |
| Leaf density | Available | Not available | Cannot measure |
| UUIDv7 | Native `uuidv7()` | Client-side generation | Different but comparable |

## Next Steps

1. Confirm decisions above
2. Start with Phase 1 (Docker setup)
3. Implement incrementally, test each phase
