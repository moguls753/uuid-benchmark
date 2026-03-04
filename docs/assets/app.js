/* ==========================================================================
   UUID Benchmark Dashboard — Data Loading & Filter Logic
   ========================================================================== */

'use strict';

/* --------------------------------------------------------------------------
   Color constants
   -------------------------------------------------------------------------- */

const KEY_TYPE_COLORS = {
  SEQUENTIAL:     '#78716c',
  OBJECTID:       '#a16207',
  UUIDV1:         '#be123c',
  UUIDV4:         '#1d4ed8',
  UUIDV7:         '#047857',
  ULID:           '#7e22ce',
  ULID_MONOTONIC: '#a855f7',
};

/** Dash patterns per key type for line charts (academic style differentiation) */
const KEY_TYPE_DASH = {
  SEQUENTIAL:     [],              // solid
  OBJECTID:       [2, 3],          // dotted
  UUIDV1:         [8, 4],          // dashed
  UUIDV4:         [8, 4, 2, 4],   // dash-dot
  UUIDV7:         [12, 4],        // long dash
  ULID:           [4, 4],          // short dash
  ULID_MONOTONIC: [8, 4, 2, 4, 2, 4], // dash-dot-dot
};

/** Point styles per key type for line charts */
const KEY_TYPE_POINT_STYLE = {
  SEQUENTIAL:     'circle',
  OBJECTID:       'triangle',
  UUIDV1:         'rect',
  UUIDV4:         'rectRot',
  UUIDV7:         'star',
  ULID:           'crossRot',
  ULID_MONOTONIC: 'cross',
};

const DATABASE_COLORS = {
  postgres:  '#336791',
  mysql:     '#00758f',
  mongodb:   '#116149',
  cassandra: '#1287B1',
};

/* --------------------------------------------------------------------------
   Formatting helpers
   -------------------------------------------------------------------------- */

const METRIC_LABELS = {
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

/** Metric definitions and measurement notes for info popovers */
const METRIC_INFO = {
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

const DATABASE_LABELS = {
  postgres:  'PostgreSQL',
  mysql:     'MySQL',
  mongodb:   'MongoDB',
  cassandra: 'Cassandra',
};

const KEY_TYPE_LABELS = {
  SEQUENTIAL:     'Sequential',
  OBJECTID:       'ObjectId',
  UUIDV1:         'UUIDv1',
  UUIDV4:         'UUIDv4',
  UUIDV7:         'UUIDv7',
  ULID:           'ULID',
  ULID_MONOTONIC: 'ULID Monotonic',
};

function formatMetricName(metric) {
  if (METRIC_LABELS[metric]) return METRIC_LABELS[metric];
  // Fallback: convert snake_case to Title Case
  return metric
    .split('_')
    .map(function (w) { return w.charAt(0).toUpperCase() + w.slice(1); })
    .join(' ');
}

function formatScenarioName(scenario) {
  return scenario
    .split('_')
    .map(function (w) { return w.charAt(0).toUpperCase() + w.slice(1); })
    .join(' ');
}

function formatKeyTypeName(keyType) {
  if (KEY_TYPE_LABELS[keyType]) return KEY_TYPE_LABELS[keyType];
  return keyType;
}

function formatDatabaseName(db) {
  if (DATABASE_LABELS[db]) return DATABASE_LABELS[db];
  return db;
}

/* --------------------------------------------------------------------------
   Global state
   -------------------------------------------------------------------------- */

let allEntries = [];
let metadata = {};
let annotations = {};

/** Which tab is active: 'cross-uuid' | 'cross-db' | 'scale' | 'raw-data' */
let activeTab = 'cross-uuid';

/** Current filter selections */
const filterState = {
  database:    null,
  keyType:     null,
  scenario:    null,
  scale:       null,
  connections: null,
  metric:      null,
};

/** Which filters are visible per tab */
const TAB_FILTERS = {
  'cross-uuid': ['database', 'scenario', 'scale', 'connections', 'metric'],
  'cross-db':   ['scenario', 'scale', 'connections', 'metric'],
  'scale':      ['database', 'scenario', 'connections', 'metric'],
  'raw-data':   ['database', 'scenario', 'scale', 'connections'],
};

/* --------------------------------------------------------------------------
   DOM references
   -------------------------------------------------------------------------- */

const dom = {};

function cacheDom() {
  dom.tabs          = document.querySelectorAll('.tab-btn');
  dom.filterBar     = document.querySelector('.filter-bar');
  dom.chartPanel    = document.getElementById('panel-chart');
  dom.tablePanel    = document.getElementById('panel-table');
  dom.noData        = document.querySelector('.no-data');
  dom.canvas        = document.getElementById('main-chart');

  dom.filters = {
    database:    document.getElementById('filter-database'),
    keyType:     document.getElementById('filter-keyType'),
    scenario:    document.getElementById('filter-scenario'),
    scale:       document.getElementById('filter-scale'),
    connections: document.getElementById('filter-connections'),
    metric:      document.getElementById('filter-metric'),
  };

  dom.metricInfoBtn      = document.querySelector('.metric-info-btn');
  dom.metricInfoPopover  = document.querySelector('.metric-info-popover');
  dom.popoverDefinition  = document.querySelector('.popover-definition');
  dom.popoverMeasurement = document.querySelector('.popover-measurement');
  dom.popoverClose       = document.querySelector('.popover-close');

  dom.methodologyToggle = document.querySelector('.methodology-toggle');
  dom.methodologyDetail = document.getElementById('methodology-detail');

  dom.comparabilityWarning = document.querySelector('.comparability-warning');
  dom.comparabilityText    = document.querySelector('.comparability-text');

  dom.chartAnnotation       = document.querySelector('.chart-annotation');
  dom.annotationFinding     = document.querySelector('.annotation-finding');
  dom.annotationExplanation = document.querySelector('.annotation-explanation');
}

/* --------------------------------------------------------------------------
   Data loading
   -------------------------------------------------------------------------- */

document.addEventListener('DOMContentLoaded', function () {
  cacheDom();
  bindTabEvents();
  bindFilterEvents();
  bindMetricInfoPopover();
  bindMethodologyToggle();

  Promise.all([
    fetch('data/data.json').then(function (res) {
      if (!res.ok) throw new Error('Failed to load data.json: ' + res.status);
      return res.json();
    }),
    fetch('data/annotations.json').then(function (res) {
      if (!res.ok) return {};
      return res.json();
    }).catch(function () { return {}; }),
  ]).then(function (results) {
    allEntries = results[0].entries || [];
    metadata   = results[0].metadata || {};
    annotations = results[1] || {};
    initUI();
  }).catch(function (err) {
    console.error(err);
    showNoData(true);
  });
});

/* --------------------------------------------------------------------------
   UI initialization
   -------------------------------------------------------------------------- */

function initUI() {
  // Set up initial filter visibility for default tab
  updateFilterVisibility();

  // Populate filters with cascading logic
  populateFilters();

  // Render
  renderCurrentView();
}

/* --------------------------------------------------------------------------
   Tab switching
   -------------------------------------------------------------------------- */

function bindTabEvents() {
  dom.tabs.forEach(function (btn) {
    btn.addEventListener('click', function () {
      var tab = btn.getAttribute('data-tab');
      if (tab === activeTab) return;

      // Update active class
      dom.tabs.forEach(function (b) {
        b.classList.remove('active');
        b.setAttribute('aria-selected', 'false');
      });
      btn.classList.add('active');
      btn.setAttribute('aria-selected', 'true');

      activeTab = tab;

      // Show/hide chart vs table panel
      if (tab === 'raw-data') {
        dom.chartPanel.hidden = true;
        dom.tablePanel.hidden = false;
      } else {
        dom.chartPanel.hidden = false;
        dom.tablePanel.hidden = true;
      }

      updateFilterVisibility();
      populateFilters();
      renderCurrentView();
    });
  });
}

function updateFilterVisibility() {
  var visible = TAB_FILTERS[activeTab] || [];

  Object.keys(dom.filters).forEach(function (key) {
    var group = dom.filters[key].closest('.filter-group');
    if (visible.indexOf(key) >= 0) {
      group.hidden = false;
    } else {
      group.hidden = true;
    }
  });
}

/* --------------------------------------------------------------------------
   Filter events
   -------------------------------------------------------------------------- */

function bindFilterEvents() {
  Object.keys(dom.filters).forEach(function (key) {
    dom.filters[key].addEventListener('change', function () {
      filterState[key] = coerceFilterValue(key, dom.filters[key].value);
      if (dom.metricInfoPopover) dom.metricInfoPopover.hidden = true;
      cascadeFilters(key);
      renderCurrentView();
    });
  });
}

/* --------------------------------------------------------------------------
   Metric info popover
   -------------------------------------------------------------------------- */

function bindMetricInfoPopover() {
  if (!dom.metricInfoBtn) return;

  dom.metricInfoBtn.addEventListener('click', function (e) {
    e.stopPropagation();
    var metric = filterState.metric;
    var info = metric && METRIC_INFO[metric];

    if (!info) {
      dom.metricInfoPopover.hidden = true;
      return;
    }

    dom.popoverDefinition.textContent = info.definition;
    var measureText = info.measurement;
    if (info.comparability) measureText += '\n\n' + info.comparability;
    dom.popoverMeasurement.textContent = measureText;
    dom.metricInfoPopover.hidden = !dom.metricInfoPopover.hidden;
  });

  dom.popoverClose.addEventListener('click', function () {
    dom.metricInfoPopover.hidden = true;
  });

  document.addEventListener('click', function (e) {
    if (!dom.metricInfoPopover.hidden
        && !dom.metricInfoPopover.contains(e.target)
        && e.target !== dom.metricInfoBtn) {
      dom.metricInfoPopover.hidden = true;
    }
  });

  document.addEventListener('keydown', function (e) {
    if (e.key === 'Escape' && !dom.metricInfoPopover.hidden) {
      dom.metricInfoPopover.hidden = true;
    }
  });
}

/* --------------------------------------------------------------------------
   Methodology banner toggle
   -------------------------------------------------------------------------- */

function bindMethodologyToggle() {
  if (!dom.methodologyToggle || !dom.methodologyDetail) return;

  dom.methodologyToggle.addEventListener('click', function () {
    var expanded = dom.methodologyDetail.hidden;
    dom.methodologyDetail.hidden = !expanded;
    dom.methodologyToggle.setAttribute('aria-expanded', String(expanded));
    dom.methodologyToggle.textContent = expanded ? 'Hide' : 'Methodology';
  });
}

/* --------------------------------------------------------------------------
   Comparability warnings
   -------------------------------------------------------------------------- */

function updateComparabilityWarning() {
  if (!dom.comparabilityWarning) return;

  var metric = filterState.metric;
  var info = metric && METRIC_INFO[metric];

  // Show warning only on chart tabs when the metric has a comparability note
  if (info && info.comparability && activeTab !== 'raw-data') {
    dom.comparabilityText.textContent = info.comparability;
    dom.comparabilityWarning.hidden = false;
  } else {
    dom.comparabilityWarning.hidden = true;
  }
}

/* --------------------------------------------------------------------------
   Chart annotation rendering
   -------------------------------------------------------------------------- */

function buildAnnotationKey() {
  var keyOrder;
  if (activeTab === 'cross-uuid') {
    keyOrder = ['database', 'scenario', 'scale', 'connections', 'metric'];
  } else if (activeTab === 'cross-db') {
    keyOrder = ['scenario', 'scale', 'connections', 'metric'];
  } else if (activeTab === 'scale') {
    keyOrder = ['database', 'scenario', 'connections', 'metric'];
  } else {
    return null;
  }

  var parts = [];
  keyOrder.forEach(function (k) {
    if (filterState[k] != null) {
      parts.push(String(filterState[k]));
    }
  });

  return parts.join('|');
}

function updateAnnotation() {
  if (!dom.chartAnnotation) return;

  if (activeTab === 'raw-data') {
    dom.chartAnnotation.hidden = true;
    return;
  }

  var tabAnnotations = annotations[activeTab];
  if (!tabAnnotations) {
    dom.chartAnnotation.hidden = true;
    return;
  }

  var key = buildAnnotationKey();
  var entry = key && tabAnnotations[key];

  if (entry && entry.finding) {
    dom.annotationFinding.textContent = entry.finding;
    dom.annotationExplanation.textContent = entry.explanation || '';
    dom.annotationExplanation.hidden = !entry.explanation;
    dom.chartAnnotation.hidden = false;
  } else {
    dom.chartAnnotation.hidden = true;
  }
}

/**
 * Connections are numbers; everything else is a string.
 */
function coerceFilterValue(key, val) {
  if (key === 'connections') return Number(val);
  return val;
}

/* --------------------------------------------------------------------------
   Cascading filter logic
   -------------------------------------------------------------------------- */

/**
 * Populate all filter dropdowns from scratch, respecting the current tab
 * and attempting to preserve existing selections.
 */
function populateFilters() {
  var visible = TAB_FILTERS[activeTab] || [];

  // 1. Collect valid options for each visible filter given the OTHER filters.
  //    We iterate in a fixed order; each filter is constrained by all others.
  var validOptions = {};
  visible.forEach(function (key) {
    validOptions[key] = deriveValidOptions(key, visible);
  });

  // 2. For each visible filter: update the <select>, preserve selection if valid,
  //    otherwise fall back to first option.
  visible.forEach(function (key) {
    fillSelect(key, validOptions[key]);
  });
}

/**
 * Populate a single <select> element with options, using <optgroup> for metrics.
 * Preserves previous selection if still valid, otherwise defaults to first option.
 */
function fillSelect(key, opts) {
  var select = dom.filters[key];
  var prev = filterState[key];

  select.innerHTML = '';

  if (key === 'metric' && opts.length > 0) {
    // Group metrics using METRIC_GROUPS
    var optSet = {};
    opts.forEach(function (v) { optSet[v] = true; });

    METRIC_GROUPS.forEach(function (group) {
      var groupMetrics = group.metrics.filter(function (m) { return optSet[m]; });
      if (groupMetrics.length === 0) return;

      var optgroup = document.createElement('optgroup');
      optgroup.label = group.label;
      groupMetrics.forEach(function (val) {
        var option = document.createElement('option');
        option.value = val;
        option.textContent = formatOption(key, val);
        optgroup.appendChild(option);
        delete optSet[val];
      });
      select.appendChild(optgroup);
    });

    // Any ungrouped metrics
    var remaining = Object.keys(optSet);
    if (remaining.length > 0) {
      remaining.forEach(function (val) {
        var option = document.createElement('option');
        option.value = val;
        option.textContent = formatOption(key, val);
        select.appendChild(option);
      });
    }
  } else {
    opts.forEach(function (val) {
      var option = document.createElement('option');
      option.value = val;
      option.textContent = formatOption(key, val);
      select.appendChild(option);
    });
  }

  // Restore or default
  if (prev != null && opts.indexOf(coerceFilterValue(key, String(prev))) >= 0) {
    select.value = String(prev);
    filterState[key] = prev;
  } else if (opts.length > 0) {
    select.value = String(opts[0]);
    filterState[key] = opts[0];
  } else {
    filterState[key] = null;
  }
}

/**
 * After a single filter changes, recompute the OTHER filters' options
 * (cascade) without resetting the one that just changed.
 */
function cascadeFilters(changedKey) {
  var visible = TAB_FILTERS[activeTab] || [];

  // Recompute all filters except the one that just changed
  visible.forEach(function (key) {
    if (key === changedKey) return;
    var opts = deriveValidOptions(key, visible);
    fillSelect(key, opts);
  });
}

/**
 * Given a filter key and the set of visible filter keys, return the sorted
 * list of unique values for `key` that exist in entries matching ALL OTHER
 * currently-selected visible filters.
 */
function deriveValidOptions(key, visible) {
  // Build a partial filter from all OTHER visible filters' current selections
  var partial = {};
  visible.forEach(function (k) {
    if (k !== key && filterState[k] != null) {
      partial[k] = filterState[k];
    }
  });

  // Collect unique values of `key` from entries matching the partial filter
  var seen = {};
  allEntries.forEach(function (e) {
    if (matchesPartial(e, partial)) {
      var val = e[key];
      if (val != null) seen[val] = true;
    }
  });

  var opts = Object.keys(seen).map(function (v) {
    return coerceFilterValue(key, v);
  });

  // Cross-DB Impact: only show metrics that are comparable across databases
  // and have a SEQUENTIAL baseline in 2+ databases
  if (activeTab === 'cross-db' && key === 'metric') {
    // Metrics that use different definitions per database — not comparable
    var nonComparable = ['fragmentation', 'avg_leaf_density', 'bloom_filter_fp', 'sstable_count', 'page_splits', 'index_size_mb'];

    opts = opts.filter(function (metric) {
      if (nonComparable.indexOf(metric) >= 0) return false;

      var dbsWithBaseline = {};
      allEntries.forEach(function (e) {
        if (e.metric !== metric || e.keyType !== 'SEQUENTIAL') return;
        if (partial.scenario && e.scenario !== partial.scenario) return;
        if (partial.scale && e.scale !== partial.scale) return;
        if (partial.connections != null && String(e.connections) !== String(partial.connections)) return;
        dbsWithBaseline[e.database] = true;
      });
      return Object.keys(dbsWithBaseline).length >= 2;
    });
  }

  // Scale tab: only show option values that have data at 2+ different scales,
  // otherwise the line chart has <=1 point and is meaningless.
  if (activeTab === 'scale' && (key === 'connections' || key === 'database')) {
    opts = opts.filter(function (optVal) {
      var scales = {};
      allEntries.forEach(function (e) {
        if (String(e[key]) !== String(optVal)) return;
        // Check all other visible filters except the one we're evaluating
        var match = true;
        visible.forEach(function (k) {
          if (k === key) return;
          if (filterState[k] != null && String(e[k]) !== String(filterState[k])) {
            match = false;
          }
        });
        if (match) scales[e.scale] = true;
      });
      return Object.keys(scales).length >= 2;
    });
  }

  // Sort
  opts.sort(function (a, b) {
    return sortComparator(key, a, b);
  });

  return opts;
}

/**
 * Does an entry match a partial filter object?
 */
function matchesPartial(entry, partial) {
  var keys = Object.keys(partial);
  for (var i = 0; i < keys.length; i++) {
    var k = keys[i];
    // Allow loose comparison for connections (number vs string)
    if (String(entry[k]) !== String(partial[k])) return false;
  }
  return true;
}

/**
 * Custom sort orders for filter options.
 */
function sortComparator(key, a, b) {
  // Scale: numeric order
  if (key === 'scale') {
    var scaleOrder = { '100k': 0, '1m': 1, '10m': 2, '100m': 3 };
    return (scaleOrder[a] || 0) - (scaleOrder[b] || 0);
  }
  // Connections: numeric
  if (key === 'connections') {
    return Number(a) - Number(b);
  }
  // Key types: defined order
  if (key === 'keyType') {
    var ktOrder = ['SEQUENTIAL', 'OBJECTID', 'UUIDV1', 'UUIDV4', 'UUIDV7', 'ULID', 'ULID_MONOTONIC'];
    var ia = ktOrder.indexOf(a);
    var ib = ktOrder.indexOf(b);
    if (ia >= 0 && ib >= 0) return ia - ib;
    return String(a).localeCompare(String(b));
  }
  // Databases: defined order
  if (key === 'database') {
    var dbOrder = ['postgres', 'mysql', 'mongodb', 'cassandra'];
    var da = dbOrder.indexOf(a);
    var db2 = dbOrder.indexOf(b);
    if (da >= 0 && db2 >= 0) return da - db2;
    return String(a).localeCompare(String(b));
  }
  // Default: alphabetical
  return String(a).localeCompare(String(b));
}

/**
 * Format an option value for display in a dropdown.
 */
function formatOption(key, val) {
  switch (key) {
    case 'database':    return formatDatabaseName(val);
    case 'keyType':     return formatKeyTypeName(val);
    case 'scenario':    return formatScenarioName(val);
    case 'metric':      return formatMetricName(val);
    case 'scale':       return String(val).toUpperCase();
    case 'connections':
      return val === 1 ? '1 connection' : val + ' connections';
    default:            return String(val);
  }
}

/* --------------------------------------------------------------------------
   Filtered entries helper
   -------------------------------------------------------------------------- */

/**
 * Return all entries matching the current filter state for the active tab.
 * Only checks filters that are visible for the current tab.
 */
function getFilteredEntries() {
  var visible = TAB_FILTERS[activeTab] || [];
  var partial = {};
  visible.forEach(function (k) {
    if (filterState[k] != null) {
      partial[k] = filterState[k];
    }
  });

  return allEntries.filter(function (e) {
    return matchesPartial(e, partial);
  });
}

/* --------------------------------------------------------------------------
   No-data indicator
   -------------------------------------------------------------------------- */

function showNoData(show) {
  dom.noData.hidden = !show;
  if (show) {
    dom.chartPanel.hidden = true;
    dom.tablePanel.hidden = true;
  }
}

/* --------------------------------------------------------------------------
   Chart instance management
   -------------------------------------------------------------------------- */

let chartInstance = null;

function destroyChart() {
  if (chartInstance) {
    chartInstance.destroy();
    chartInstance = null;
  }
}

/* --------------------------------------------------------------------------
   Canonical orderings
   -------------------------------------------------------------------------- */

const KEY_TYPE_ORDER = ['SEQUENTIAL', 'OBJECTID', 'UUIDV1', 'UUIDV4', 'UUIDV7', 'ULID', 'ULID_MONOTONIC'];
const DATABASE_ORDER = ['postgres', 'mysql', 'mongodb', 'cassandra'];
const SCALE_ORDER    = ['100k', '1m', '10m', '100m'];

/** Metric groups for organized dropdowns */
const METRIC_GROUPS = [
  { label: 'Performance', metrics: ['throughput'] },
  { label: 'Latency',     metrics: ['p50_latency_us', 'p95_latency_us', 'p99_latency_us'] },
  { label: 'Storage',     metrics: ['table_size_mb', 'index_size_mb'] },
  { label: 'Cache',       metrics: ['cache_hit_ratio', 'index_hit_ratio'] },
  { label: 'I/O',         metrics: ['read_iops', 'write_iops', 'read_throughput_mb', 'write_throughput_mb'] },
  { label: 'B-tree',      metrics: ['page_splits', 'fragmentation', 'avg_leaf_density'] },
  { label: 'LSM-tree',    metrics: ['sstable_count', 'bloom_filter_fp'] },
];

/* --------------------------------------------------------------------------
   Shared Chart.js defaults
   -------------------------------------------------------------------------- */

const CHART_ANIMATION = { duration: 300 };

const CHART_GRID_STYLE = {
  color: 'rgba(28, 25, 23, 0.07)',
};

const CHART_BORDER_STYLE = {
  color: '#1c1917',
  display: true,
};

/**
 * Chart.js plugin: draw error bars (stddev range) on bar/line charts.
 * Expects each dataset to have an `errorBars` array of objects:
 *   { low: number, high: number } — one per data point.
 * Null/undefined entries are skipped.
 */
const errorBarPlugin = {
  id: 'errorBars',
  afterDraw: function (chart) {
    var ctx = chart.ctx;
    chart.data.datasets.forEach(function (dataset, dsIndex) {
      if (!dataset.errorBars) return;
      var meta = chart.getDatasetMeta(dsIndex);
      if (meta.hidden) return;

      dataset.errorBars.forEach(function (eb, index) {
        if (!eb || eb.low == null || eb.high == null) return;
        var element = meta.data[index];
        if (!element) return;

        var x = element.x;
        var yScale = chart.scales.y;
        var yLow  = yScale.getPixelForValue(eb.low);
        var yHigh = yScale.getPixelForValue(eb.high);

        var capWidth = 6;

        ctx.save();
        ctx.strokeStyle = '#1c1917';
        ctx.lineWidth = 1.5;
        ctx.beginPath();

        // Vertical line
        ctx.moveTo(x, yLow);
        ctx.lineTo(x, yHigh);

        // Top cap
        ctx.moveTo(x - capWidth, yHigh);
        ctx.lineTo(x + capWidth, yHigh);

        // Bottom cap
        ctx.moveTo(x - capWidth, yLow);
        ctx.lineTo(x + capWidth, yLow);

        ctx.stroke();
        ctx.restore();
      });
    });
  },
};

/* --------------------------------------------------------------------------
   Build chart title from current filter state
   -------------------------------------------------------------------------- */

function buildChartTitle(overrides) {
  var parts = [];
  var o = overrides || {};

  if (o.database !== false && filterState.database) {
    parts.push(formatDatabaseName(filterState.database));
  }
  if (o.keyType !== false && filterState.keyType) {
    parts.push(formatKeyTypeName(filterState.keyType));
  }
  if (filterState.scenario) {
    parts.push(formatScenarioName(filterState.scenario));
  }
  if (o.scale !== false && filterState.scale) {
    parts.push(String(filterState.scale).toUpperCase());
  }
  if (filterState.connections != null) {
    parts.push(filterState.connections === 1
      ? '1 Connection'
      : filterState.connections + ' Connections');
  }

  return parts.join(' \u2014 ');
}

/* --------------------------------------------------------------------------
   Number formatting helper
   -------------------------------------------------------------------------- */

function formatNumber(val) {
  if (val == null || isNaN(val)) return '\u2014';
  // For very large or very small numbers, use toLocaleString with 2 decimal places
  return Number(val.toFixed(2)).toLocaleString(undefined, {
    minimumFractionDigits: 0,
    maximumFractionDigits: 2,
  });
}

/* --------------------------------------------------------------------------
   Render dispatcher
   -------------------------------------------------------------------------- */

function renderCurrentView() {
  // Ensure correct panel is visible
  if (activeTab === 'raw-data') {
    dom.chartPanel.hidden = true;
    dom.tablePanel.hidden = false;
  } else {
    dom.chartPanel.hidden = false;
    dom.tablePanel.hidden = true;
  }

  updateComparabilityWarning();

  // Cross-DB Impact queries allEntries directly (no per-entry filtering)
  if (activeTab === 'cross-db') {
    showNoData(false);
    renderCrossDB();
    updateAnnotation();
    return;
  }

  var entries = getFilteredEntries();

  if (entries.length === 0) {
    showNoData(true);
    return;
  }

  showNoData(false);

  switch (activeTab) {
    case 'cross-uuid':
      renderCrossUUID(entries);
      break;
    case 'scale':
      renderScale(entries);
      break;
    case 'raw-data':
      renderRawData(entries);
      break;
  }

  updateAnnotation();
}

/* ==========================================================================
   Task 5: Cross-UUID Chart View (bar chart)
   ========================================================================== */

function renderCrossUUID(entries) {
  destroyChart();

  // Group entries by keyType — entries are already filtered to one metric
  var byKeyType = {};
  entries.forEach(function (e) {
    byKeyType[e.keyType] = e;
  });

  // Build labels and data in canonical order
  var labels = [];
  var data = [];
  var colors = [];
  var errorBars = [];

  KEY_TYPE_ORDER.forEach(function (kt) {
    if (!byKeyType[kt]) return;
    var e = byKeyType[kt];
    labels.push(formatKeyTypeName(kt));
    data.push(e.median);
    colors.push(KEY_TYPE_COLORS[kt] || '#999');
    errorBars.push({
      low:  Math.max(0, e.median - e.stddev),
      high: e.median + e.stddev,
    });
  });

  if (labels.length === 0) {
    showNoData(true);
    return;
  }

  var metricLabel = filterState.metric
    ? formatMetricName(filterState.metric)
    : 'Value';

  chartInstance = new Chart(dom.canvas, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: [{
        label: metricLabel,
        data: data,
        backgroundColor: colors.map(function (c) { return c + 'cc'; }),
        borderColor: colors,
        borderWidth: 1,
        errorBars: errorBars,
      }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: CHART_ANIMATION,
      plugins: {
        title: {
          display: true,
          text: buildChartTitle({ keyType: false }),
          font: { size: 14, weight: '600', family: '"Crimson Pro", Georgia, serif' },
          color: '#1c1917',
          padding: { bottom: 16 },
        },
        legend: {
          display: false,
        },
        tooltip: {
          callbacks: {
            label: function (ctx) {
              var e = entries.find(function (en) {
                return formatKeyTypeName(en.keyType) === ctx.label;
              });
              var lines = [metricLabel + ': ' + formatNumber(ctx.parsed.y)];
              if (e) {
                lines.push('Mean: ' + formatNumber(e.mean));
                lines.push('StdDev: ' + formatNumber(e.stddev));
                lines.push('CV: ' + formatNumber(e.cv) + '%');
              }
              return lines;
            },
          },
        },
      },
      scales: {
        x: {
          grid: { display: false },
          border: CHART_BORDER_STYLE,
          ticks: {
            font: { size: 11, family: '"DM Sans", "Helvetica Neue", Arial, sans-serif' },
            color: '#78716c',
          },
        },
        y: {
          beginAtZero: true,
          grid: CHART_GRID_STYLE,
          border: CHART_BORDER_STYLE,
          title: {
            display: true,
            text: metricLabel,
            font: { size: 11, weight: '600', family: '"DM Sans", "Helvetica Neue", Arial, sans-serif' },
            color: '#78716c',
          },
          ticks: {
            font: { size: 10, family: '"JetBrains Mono", "SF Mono", "Consolas", monospace' },
            color: '#78716c',
          },
        },
      },
    },
    plugins: [errorBarPlugin],
  });
}

/* ==========================================================================
   Task 6: Cross-DB Impact View (grouped bar chart — % vs Sequential baseline)
   ========================================================================== */

function renderCrossDB(/* entries arg unused — we query allEntries directly */) {
  destroyChart();

  var scenario    = filterState.scenario;
  var scale       = filterState.scale;
  var connections = filterState.connections;
  var metric      = filterState.metric;

  if (!scenario || !scale || connections == null || !metric) {
    showNoData(true);
    return;
  }

  // Get all entries matching the selected scenario/scale/connections/metric
  var matching = allEntries.filter(function (e) {
    return e.scenario === scenario
      && e.scale === scale
      && String(e.connections) === String(connections)
      && e.metric === metric;
  });

  // Group: database -> keyType -> entry
  var byDB = {};
  matching.forEach(function (e) {
    if (!byDB[e.database]) byDB[e.database] = {};
    byDB[e.database][e.keyType] = e;
  });

  // Determine which databases have a SEQUENTIAL baseline
  var dbsWithBaseline = [];
  DATABASE_ORDER.forEach(function (db) {
    if (byDB[db] && byDB[db].SEQUENTIAL) {
      dbsWithBaseline.push(db);
    }
  });

  if (dbsWithBaseline.length === 0) {
    showNoData(true);
    return;
  }

  // Determine which non-baseline key types appear in at least one database
  var keyTypesPresent = [];
  KEY_TYPE_ORDER.forEach(function (kt) {
    if (kt === 'SEQUENTIAL') return;
    var found = dbsWithBaseline.some(function (db) {
      return byDB[db][kt] != null;
    });
    if (found) keyTypesPresent.push(kt);
  });

  if (keyTypesPresent.length === 0) {
    showNoData(true);
    return;
  }

  // X-axis labels = database names
  var labels = dbsWithBaseline.map(function (db) {
    return formatDatabaseName(db);
  });

  var metricLabel = filterState.metric
    ? formatMetricName(filterState.metric)
    : 'Value';

  // One dataset per key type, each bar = % difference vs SEQUENTIAL for that database
  var datasets = keyTypesPresent.map(function (kt) {
    var data = [];
    dbsWithBaseline.forEach(function (db) {
      var baseline = byDB[db].SEQUENTIAL.median;
      var entry    = byDB[db][kt];
      if (entry && baseline !== 0) {
        data.push(Math.round(((entry.median - baseline) / Math.abs(baseline)) * 10000) / 100);
      } else {
        data.push(null);
      }
    });

    var color = KEY_TYPE_COLORS[kt] || '#999';
    return {
      label: formatKeyTypeName(kt),
      data: data,
      backgroundColor: color + 'cc',
      borderColor: color,
      borderWidth: 1,
    };
  });

  chartInstance = new Chart(dom.canvas, {
    type: 'bar',
    data: {
      labels: labels,
      datasets: datasets,
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: CHART_ANIMATION,
      plugins: {
        title: {
          display: true,
          text: buildChartTitle({ database: false, keyType: false })
            + ' \u2014 % vs Sequential',
          font: { size: 14, weight: '600', family: '"Crimson Pro", Georgia, serif' },
          color: '#1c1917',
          padding: { bottom: 16 },
        },
        legend: {
          display: true,
          position: 'bottom',
          labels: {
            usePointStyle: true,
            pointStyle: 'rect',
            padding: 16,
            font: { size: 11, family: '"DM Sans", "Helvetica Neue", Arial, sans-serif' },
            color: '#1c1917',
          },
        },
        tooltip: {
          callbacks: {
            label: function (ctx) {
              var pct = ctx.parsed.y;
              var sign = pct >= 0 ? '+' : '';
              var db = dbsWithBaseline[ctx.dataIndex];
              var kt = keyTypesPresent[ctx.datasetIndex];
              var entry = byDB[db] && byDB[db][kt];
              var baseline = byDB[db] && byDB[db].SEQUENTIAL;
              var lines = [ctx.dataset.label + ': ' + sign + formatNumber(pct) + '%'];
              if (entry) lines.push('Actual: ' + formatNumber(entry.median));
              if (baseline) lines.push('Baseline (Sequential): ' + formatNumber(baseline.median));
              return lines;
            },
          },
        },
      },
      scales: {
        x: {
          grid: { display: false },
          border: CHART_BORDER_STYLE,
          ticks: {
            font: { size: 11, family: '"DM Sans", "Helvetica Neue", Arial, sans-serif' },
            color: '#78716c',
          },
        },
        y: {
          grid: CHART_GRID_STYLE,
          border: CHART_BORDER_STYLE,
          title: {
            display: true,
            text: '% Difference vs Sequential (' + metricLabel + ')',
            font: { size: 11, weight: '600', family: '"DM Sans", "Helvetica Neue", Arial, sans-serif' },
            color: '#78716c',
          },
          ticks: {
            font: { size: 10, family: '"JetBrains Mono", "SF Mono", "Consolas", monospace' },
            color: '#78716c',
            callback: function (value) {
              return (value >= 0 ? '+' : '') + value + '%';
            },
          },
        },
      },
    },
  });
}

/* ==========================================================================
   Task 7: Scale Effect Chart View (line chart)
   ========================================================================== */

function renderScale(entries) {
  destroyChart();

  // Determine which scales are present, in canonical order
  var scalesPresent = {};
  entries.forEach(function (e) {
    scalesPresent[e.scale] = true;
  });
  var scaleLabels = [];
  SCALE_ORDER.forEach(function (s) {
    if (scalesPresent[s]) scaleLabels.push(s);
  });

  if (scaleLabels.length === 0) {
    showNoData(true);
    return;
  }

  // Group entries: keyType -> scale -> entry
  var byKeyType = {};
  entries.forEach(function (e) {
    if (!byKeyType[e.keyType]) byKeyType[e.keyType] = {};
    byKeyType[e.keyType][e.scale] = e;
  });

  // Build one dataset per key type
  var datasets = [];
  KEY_TYPE_ORDER.forEach(function (kt) {
    if (!byKeyType[kt]) return;
    var ktData = byKeyType[kt];
    var data = [];
    var errBars = [];

    scaleLabels.forEach(function (s) {
      if (ktData[s]) {
        data.push(ktData[s].median);
        errBars.push({
          low:  Math.max(0, ktData[s].median - ktData[s].stddev),
          high: ktData[s].median + ktData[s].stddev,
        });
      } else {
        data.push(null);
        errBars.push(null);
      }
    });

    var color = KEY_TYPE_COLORS[kt] || '#999';
    datasets.push({
      label: formatKeyTypeName(kt),
      data: data,
      borderColor: color,
      backgroundColor: color,
      borderWidth: 2.5,
      borderDash: KEY_TYPE_DASH[kt] || [],
      pointRadius: 5,
      pointHoverRadius: 8,
      pointStyle: KEY_TYPE_POINT_STYLE[kt] || 'circle',
      pointBackgroundColor: color,
      pointBorderColor: '#fff',
      pointBorderWidth: 1.5,
      fill: false,
      spanGaps: false,
      tension: 0,
      errorBars: errBars,
    });
  });

  if (datasets.length === 0) {
    showNoData(true);
    return;
  }

  var metricLabel = filterState.metric
    ? formatMetricName(filterState.metric)
    : 'Value';

  chartInstance = new Chart(dom.canvas, {
    type: 'line',
    data: {
      labels: scaleLabels.map(function (s) { return s.toUpperCase(); }),
      datasets: datasets,
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      animation: CHART_ANIMATION,
      interaction: {
        mode: 'index',
        intersect: false,
      },
      plugins: {
        title: {
          display: true,
          text: buildChartTitle({ scale: false, keyType: false }),
          font: { size: 14, weight: '600', family: '"Crimson Pro", Georgia, serif' },
          color: '#1c1917',
          padding: { bottom: 16 },
        },
        legend: {
          display: true,
          position: 'bottom',
          labels: {
            usePointStyle: true,
            padding: 16,
            font: { size: 11, family: '"DM Sans", "Helvetica Neue", Arial, sans-serif' },
            color: '#1c1917',
          },
        },
        tooltip: {
          callbacks: {
            label: function (ctx) {
              var kt = Object.keys(byKeyType).find(function (k) {
                return formatKeyTypeName(k) === ctx.dataset.label;
              });
              var scaleKey = scaleLabels[ctx.dataIndex];
              var e = kt && byKeyType[kt] && byKeyType[kt][scaleKey];
              var lines = [ctx.dataset.label + ': ' + formatNumber(ctx.parsed.y)];
              if (e) {
                lines.push('StdDev: ' + formatNumber(e.stddev));
              }
              return lines;
            },
          },
        },
      },
      scales: {
        x: {
          type: 'category',
          grid: { display: false },
          border: CHART_BORDER_STYLE,
          title: {
            display: true,
            text: 'Dataset Scale',
            font: { size: 11, weight: '600', family: '"DM Sans", "Helvetica Neue", Arial, sans-serif' },
            color: '#78716c',
          },
          ticks: {
            font: { size: 11, family: '"DM Sans", "Helvetica Neue", Arial, sans-serif' },
            color: '#78716c',
          },
        },
        y: {
          beginAtZero: true,
          grid: CHART_GRID_STYLE,
          border: CHART_BORDER_STYLE,
          title: {
            display: true,
            text: metricLabel,
            font: { size: 11, weight: '600', family: '"DM Sans", "Helvetica Neue", Arial, sans-serif' },
            color: '#78716c',
          },
          ticks: {
            maxTicksLimit: 8,
            font: { size: 10, family: '"JetBrains Mono", "SF Mono", "Consolas", monospace' },
            color: '#78716c',
          },
        },
      },
    },
    plugins: [errorBarPlugin],
  });
}

/* ==========================================================================
   Task 8: Raw Data Table View
   ========================================================================== */

/** Current sort state for the raw data table */
var tableSortState = { column: 'keyType', direction: 'asc' };

/** Column definitions for the raw data table */
var TABLE_COLUMNS = [
  { key: 'keyType', label: 'Key Type',  numeric: false },
  { key: 'metric',  label: 'Metric',    numeric: false },
  { key: 'median',  label: 'Median',    numeric: true  },
  { key: 'mean',    label: 'Mean',      numeric: true  },
  { key: 'stddev',  label: 'StdDev',    numeric: true  },
  { key: 'cv',      label: 'CV%',       numeric: true  },
  { key: 'min',     label: 'Min',       numeric: true  },
  { key: 'max',     label: 'Max',       numeric: true  },
];

function renderRawData(entries) {
  destroyChart();

  var thead = document.querySelector('.data-table thead tr');
  var tbody = document.querySelector('.data-table tbody');

  // Build header
  thead.innerHTML = '';
  TABLE_COLUMNS.forEach(function (col) {
    var th = document.createElement('th');
    th.className = col.numeric ? 'num' : '';
    th.setAttribute('data-col', col.key);

    var labelSpan = document.createTextNode(col.label + ' ');
    th.appendChild(labelSpan);

    var indicator = document.createElement('span');
    indicator.className = 'sort-indicator';
    if (tableSortState.column === col.key) {
      th.classList.add('sort-active');
      th.setAttribute('aria-sort', tableSortState.direction === 'asc' ? 'ascending' : 'descending');
      indicator.textContent = tableSortState.direction === 'asc' ? '\u25B2' : '\u25BC';
    } else {
      th.removeAttribute('aria-sort');
      indicator.textContent = '\u25B2';
    }
    th.appendChild(indicator);

    th.addEventListener('click', function () {
      if (tableSortState.column === col.key) {
        tableSortState.direction = tableSortState.direction === 'asc' ? 'desc' : 'asc';
      } else {
        tableSortState.column = col.key;
        tableSortState.direction = 'asc';
      }
      renderRawData(entries);
    });

    thead.appendChild(th);
  });

  // Sort entries
  var sorted = entries.slice().sort(function (a, b) {
    var col = tableSortState.column;
    var dir = tableSortState.direction === 'asc' ? 1 : -1;
    var va = a[col];
    var vb = b[col];

    // keyType: use canonical order
    if (col === 'keyType') {
      var ia = KEY_TYPE_ORDER.indexOf(va);
      var ib = KEY_TYPE_ORDER.indexOf(vb);
      if (ia >= 0 && ib >= 0) {
        var cmp = ia - ib;
        if (cmp !== 0) return cmp * dir;
      } else {
        var cmp2 = String(va).localeCompare(String(vb));
        if (cmp2 !== 0) return cmp2 * dir;
      }
      // Secondary sort by metric name
      return String(a.metric).localeCompare(String(b.metric)) * dir;
    }

    // metric: alphabetical string sort
    if (col === 'metric') {
      var mc = String(va).localeCompare(String(vb));
      if (mc !== 0) return mc * dir;
      // Secondary sort by keyType canonical order
      var ika = KEY_TYPE_ORDER.indexOf(a.keyType);
      var ikb = KEY_TYPE_ORDER.indexOf(b.keyType);
      return ((ika >= 0 ? ika : 99) - (ikb >= 0 ? ikb : 99)) * dir;
    }

    // Numeric sort
    if (va == null && vb == null) return 0;
    if (va == null) return 1;
    if (vb == null) return -1;
    return (Number(va) - Number(vb)) * dir;
  });

  // Compute best/worst median per metric for conditional formatting
  var LOWER_IS_BETTER = ['p50_latency_us', 'p95_latency_us', 'p99_latency_us',
    'fragmentation', 'page_splits', 'sstable_count', 'bloom_filter_fp'];

  var metricBestWorst = {};
  sorted.forEach(function (e) {
    if (e.median == null) return;
    if (!metricBestWorst[e.metric]) {
      metricBestWorst[e.metric] = { best: e.median, worst: e.median };
    }
    var mw = metricBestWorst[e.metric];
    var lowerBetter = LOWER_IS_BETTER.indexOf(e.metric) >= 0;
    if (lowerBetter) {
      if (e.median < mw.best) mw.best = e.median;
      if (e.median > mw.worst) mw.worst = e.median;
    } else {
      if (e.median > mw.best) mw.best = e.median;
      if (e.median < mw.worst) mw.worst = e.median;
    }
  });

  // Build rows
  tbody.innerHTML = '';
  sorted.forEach(function (e) {
    var tr = document.createElement('tr');

    TABLE_COLUMNS.forEach(function (col) {
      var td = document.createElement('td');
      td.className = col.numeric ? 'num' : '';

      var val = e[col.key];
      if (col.key === 'keyType') {
        td.textContent = formatKeyTypeName(val);
      } else if (col.key === 'metric') {
        td.textContent = formatMetricName(val);
      } else if (col.key === 'cv') {
        td.textContent = val != null ? formatNumber(val) + '%' : '\u2014';
      } else {
        td.textContent = formatNumber(val);
      }

      if (col.key === 'median' && e.median != null && metricBestWorst[e.metric]) {
        var mw = metricBestWorst[e.metric];
        if (mw.best !== mw.worst) {
          if (e.median === mw.best) td.classList.add('cell-best');
          if (e.median === mw.worst) td.classList.add('cell-worst');
        }
      }

      tr.appendChild(td);
    });

    tbody.appendChild(tr);
  });
}
