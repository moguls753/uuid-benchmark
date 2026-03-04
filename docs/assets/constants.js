// ==========================================================================
//  UUID Benchmark Dashboard — Constants & Configuration
//  Single source of truth for colors, labels, metric info, orderings.
// ==========================================================================

// --- Key type colors (the ONLY color in the monochrome UI) ---
export const KEY_TYPE_COLORS = {
  SEQUENTIAL:     '#78716c',
  OBJECTID:       '#a16207',
  UUIDV1:         '#be123c',
  UUIDV4:         '#1d4ed8',
  UUIDV7:         '#047857',
  ULID:           '#7e22ce',
  ULID_MONOTONIC: '#a855f7',
};

// --- Dash patterns per key type for line charts ---
export const KEY_TYPE_DASH = {
  SEQUENTIAL:     [],
  OBJECTID:       [2, 3],
  UUIDV1:         [8, 4],
  UUIDV4:         [8, 4, 2, 4],
  UUIDV7:         [12, 4],
  ULID:           [4, 4],
  ULID_MONOTONIC: [8, 4, 2, 4, 2, 4],
};

// --- Point styles per key type for line charts ---
export const KEY_TYPE_POINT_STYLE = {
  SEQUENTIAL:     'circle',
  OBJECTID:       'triangle',
  UUIDV1:         'rect',
  UUIDV4:         'rectRot',
  UUIDV7:         'star',
  ULID:           'crossRot',
  ULID_MONOTONIC: 'cross',
};

// --- Database colors ---
export const DATABASE_COLORS = {
  postgres:  '#336791',
  mysql:     '#00758f',
  mongodb:   '#116149',
  cassandra: '#1287B1',
};

// --- Metric labels ---
export const METRIC_LABELS = {
  throughput:          'Throughput (ops/s)',
  p50_latency_us:     'P50 Latency (\u00b5s)',
  p95_latency_us:     'P95 Latency (\u00b5s)',
  p99_latency_us:     'P99 Latency (\u00b5s)',
  page_splits:        'Page Splits',
  fragmentation:      'Fragmentation (%)',
  cache_hit_ratio:    'Cache Hit Ratio',
  index_hit_ratio:    'Index Hit Ratio',
  avg_leaf_density:   'Avg Leaf Density (%)',
  table_size_mb:      'Table Size (MB)',
  index_size_mb:      'Index Size (MB)',
  read_iops:          'Read IOPS',
  write_iops:         'Write IOPS',
  read_throughput_mb: 'Read Throughput (MB/s)',
  write_throughput_mb:'Write Throughput (MB/s)',
  sstable_count:      'SSTable Count',
  bloom_filter_fp:    'Bloom Filter False Positives',
};

// --- Metric definitions and measurement notes ---
export const METRIC_INFO = {
  throughput: {
    definition: 'Operations completed per second during the workload phase. Higher is better.',
    measurement: 'PostgreSQL: pgbench TPS. MySQL/MongoDB/Cassandra: custom Go workload binary. All tools run inside the container (localhost, zero network overhead).',
  },
  p50_latency_us: {
    definition: 'Median latency \u2014 50% of operations completed within this time. The typical user experience.',
    measurement: 'All databases: per-operation timing with percentile calculation. Reported in microseconds (\u00b5s). Lower is better.',
  },
  p95_latency_us: {
    definition: '95th percentile latency \u2014 95% of operations completed within this time. Only 1 in 20 was slower.',
    measurement: 'Same methodology as P50. Captures tail latency that affects user-perceived performance.',
  },
  p99_latency_us: {
    definition: '99th percentile latency \u2014 99% of operations completed within this time. Only 1 in 100 was slower.',
    measurement: 'Same methodology as P50. Critical for SLA compliance. Can spike dramatically under random I/O from UUIDv4.',
  },
  page_splits: {
    definition: 'Number of B-tree leaf page splits during the workload. More splits = more random I/O and wasted space.',
    measurement: 'PostgreSQL: WAL inspection (exact count). MySQL: innodb_metrics counter (exact delta). MongoDB: WiredTiger in-memory page split counter. Comparable across B-tree databases.',
    comparability: 'B-tree databases only. Cassandra uses LSM-tree (compaction instead of splits).',
  },
  fragmentation: {
    definition: 'Index fragmentation percentage. Higher means more wasted space or scattered pages.',
    measurement: 'PostgreSQL: physical leaf page ordering (pgstatindex). MySQL: B-tree overhead ratio (internal/total pages). MongoDB: free storage / total storage ratio.',
    comparability: 'Different definitions per database \u2014 compare trends within one database, not absolute values across databases. PostgreSQL measures physical page ordering; MySQL measures B-tree structural overhead; MongoDB measures free space ratio.',
  },
  cache_hit_ratio: {
    definition: 'Fraction of page requests served from memory (0.0\u20131.0). Higher means less disk I/O.',
    measurement: 'PostgreSQL: pg_stat_database (blks_hit / total). MySQL: performance_schema buffer pool stats. MongoDB: WiredTiger cache pages requested vs read. Cassandra: key cache hit rate.',
  },
  index_hit_ratio: {
    definition: 'Fraction of index page requests served from memory. Indicates whether the index B-tree fits in RAM.',
    measurement: 'PostgreSQL: pg_statio_user_tables (idx_blks_hit / total). Similar concept across databases.',
  },
  avg_leaf_density: {
    definition: 'Average percentage of each B-tree leaf page that contains actual data (0\u2013100%). Low density = wasted space from page splits.',
    measurement: 'PostgreSQL: pgstatindex (exact). MySQL: not exposed, estimated at 90%. MongoDB: leaf page count from indexDetails.',
    comparability: 'Only directly measurable in PostgreSQL. Other databases use approximations.',
  },
  table_size_mb: {
    definition: 'On-disk size of the table data in megabytes.',
    measurement: 'PostgreSQL: pg_table_size() (exact). MySQL: data_length from information_schema (after ANALYZE TABLE). MongoDB: collStats storageSize. Cassandra: nodetool tablestats. Comparable across databases.',
  },
  index_size_mb: {
    definition: 'On-disk size of the index structure in megabytes.',
    measurement: 'PostgreSQL: pg_indexes_size() (separate B-tree). MySQL: data_length includes clustered PK (InnoDB stores data in the PK B-tree). MongoDB: collStats totalIndexSize.',
    comparability: 'MySQL\'s clustered index means PK "index size" includes row data. Not directly comparable with PostgreSQL\'s separate heap + index architecture.',
  },
  read_iops: {
    definition: 'Read I/O operations per second during the workload, measured at the container level.',
    measurement: 'All databases: Linux cgroup v2 io.stat (kernel accounting). Container-isolated, zero overhead. Identical method across all databases.',
  },
  write_iops: {
    definition: 'Write I/O operations per second during the workload, measured at the container level.',
    measurement: 'All databases: Linux cgroup v2 io.stat. Identical method across all databases.',
  },
  read_throughput_mb: {
    definition: 'Read data throughput in MB/s during the workload.',
    measurement: 'All databases: Linux cgroup v2 io.stat. Identical method across all databases.',
  },
  write_throughput_mb: {
    definition: 'Write data throughput in MB/s during the workload.',
    measurement: 'All databases: Linux cgroup v2 io.stat. Identical method across all databases.',
  },
  sstable_count: {
    definition: 'Number of SSTables on disk after the workload. More SSTables = more read amplification.',
    measurement: 'Cassandra only: nodetool tablestats. LSM-tree concept \u2014 no equivalent in B-tree databases.',
    comparability: 'Cassandra only. B-tree databases do not have SSTables.',
  },
  bloom_filter_fp: {
    definition: 'Bloom filter false positive count. Random keys may cause more false positives, increasing unnecessary disk reads.',
    measurement: 'Cassandra only: nodetool tablestats.',
    comparability: 'Cassandra only.',
  },
};

// --- Display labels ---
export const DATABASE_LABELS = {
  postgres:  'PostgreSQL',
  mysql:     'MySQL',
  mongodb:   'MongoDB',
  cassandra: 'Cassandra',
};

export const KEY_TYPE_LABELS = {
  SEQUENTIAL:     'Sequential',
  OBJECTID:       'ObjectId',
  UUIDV1:         'UUIDv1',
  UUIDV4:         'UUIDv4',
  UUIDV7:         'UUIDv7',
  ULID:           'ULID',
  ULID_MONOTONIC: 'ULID Monotonic',
};

// --- Short labels for legend strip ---
export const KEY_TYPE_SHORT = {
  SEQUENTIAL:     'SEQ',
  OBJECTID:       'OID',
  UUIDV1:         'V1',
  UUIDV4:         'V4',
  UUIDV7:         'V7',
  ULID:           'ULID',
  ULID_MONOTONIC: 'ULID_M',
};

// --- Canonical orderings ---
export const KEY_TYPE_ORDER = ['SEQUENTIAL', 'OBJECTID', 'UUIDV1', 'UUIDV4', 'UUIDV7', 'ULID', 'ULID_MONOTONIC'];
export const DATABASE_ORDER = ['postgres', 'mysql', 'mongodb', 'cassandra'];
export const SCALE_ORDER    = ['100k', '1m', '10m', '100m'];

// --- Metric groups for organized dropdowns ---
export const METRIC_GROUPS = [
  { label: 'Performance', metrics: ['throughput'] },
  { label: 'Latency',     metrics: ['p50_latency_us', 'p95_latency_us', 'p99_latency_us'] },
  { label: 'Storage',     metrics: ['table_size_mb', 'index_size_mb'] },
  { label: 'Cache',       metrics: ['cache_hit_ratio', 'index_hit_ratio'] },
  { label: 'I/O',         metrics: ['read_iops', 'write_iops', 'read_throughput_mb', 'write_throughput_mb'] },
  { label: 'B-tree',      metrics: ['page_splits', 'fragmentation', 'avg_leaf_density'] },
  { label: 'LSM-tree',    metrics: ['sstable_count', 'bloom_filter_fp'] },
];

// --- Metrics where lower is better ---
export const LOWER_IS_BETTER = [
  'p50_latency_us', 'p95_latency_us', 'p99_latency_us',
  'fragmentation', 'page_splits', 'sstable_count', 'bloom_filter_fp',
];

// --- Explorer 4-panel config ---
export const EXPLORER_PANELS = {
  default:   ['throughput', 'p50_latency_us', 'page_splits', 'cache_hit_ratio'],
  cassandra: ['throughput', 'p50_latency_us', 'sstable_count', 'cache_hit_ratio'],
};

export const PANEL_CONFIG = {
  throughput:        { label: 'THROUGHPUT',        unit: 'ops/sec' },
  p50_latency_us:    { label: 'LATENCY P50',       unit: '\u00b5s' },
  p95_latency_us:    { label: 'LATENCY P95',       unit: '\u00b5s' },
  p99_latency_us:    { label: 'LATENCY P99',       unit: '\u00b5s' },
  page_splits:       { label: 'PAGE SPLITS',        unit: 'count' },
  cache_hit_ratio:   { label: 'CACHE HIT RATIO',    unit: '0\u20131' },
  sstable_count:     { label: 'SSTABLE COUNT',       unit: 'count' },
  fragmentation:     { label: 'FRAGMENTATION',       unit: '%' },
  table_size_mb:     { label: 'TABLE SIZE',          unit: 'MB' },
  index_size_mb:     { label: 'INDEX SIZE',          unit: 'MB' },
  index_hit_ratio:   { label: 'INDEX HIT RATIO',     unit: '0\u20131' },
  avg_leaf_density:  { label: 'LEAF DENSITY',         unit: '%' },
  read_iops:         { label: 'READ IOPS',            unit: 'ops/sec' },
  write_iops:        { label: 'WRITE IOPS',           unit: 'ops/sec' },
  read_throughput_mb:  { label: 'READ THROUGHPUT',   unit: 'MB/s' },
  write_throughput_mb: { label: 'WRITE THROUGHPUT',  unit: 'MB/s' },
  bloom_filter_fp:   { label: 'BLOOM FILTER FP',     unit: 'count' },
};

// --- Which filters are visible per view mode ---
export const VIEW_FILTERS = {
  'cross-uuid': ['database', 'scenario', 'scale', 'connections'],
  'cross-db':   ['keyType', 'scenario', 'scale', 'connections'],
  'scale':      ['database', 'scenario', 'connections'],
  'raw-data':   ['database', 'scenario', 'scale', 'connections'],
};

// --- Non-comparable metrics for cross-db view ---
export const CROSS_DB_NON_COMPARABLE = [
  'fragmentation', 'avg_leaf_density', 'bloom_filter_fp',
  'sstable_count', 'page_splits', 'index_size_mb',
];

// --- Formatting helpers ---

export function formatMetricName(metric) {
  if (METRIC_LABELS[metric]) return METRIC_LABELS[metric];
  return metric
    .split('_')
    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
}

export function formatScenarioName(scenario) {
  return scenario
    .split('_')
    .map(w => w.charAt(0).toUpperCase() + w.slice(1))
    .join(' ');
}

export function formatKeyTypeName(keyType) {
  return KEY_TYPE_LABELS[keyType] || keyType;
}

export function formatDatabaseName(db) {
  return DATABASE_LABELS[db] || db;
}

export function formatNumber(val) {
  if (val == null || isNaN(val)) return '\u2014';
  return Number(val.toFixed(2)).toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  });
}
