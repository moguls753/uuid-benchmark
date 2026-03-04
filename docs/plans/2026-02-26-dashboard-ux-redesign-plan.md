# Dashboard UX Redesign Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add educational context (methodology, metric definitions, hand-written findings) and visual polish to the UUID benchmark dashboard.

**Architecture:** Vanilla JS, no framework, no build step. Four files touched: `index.html` (structure), `style.css` (visual polish), `app.js` (metric info, annotations, warnings), plus one new `annotations.json` data file. All annotation content is hand-curated, not auto-computed.

**Tech Stack:** HTML5, CSS3 (custom properties, flexbox), vanilla JS, Chart.js 4.x (CDN)

---

### Task 1: HTML Structure — Add new containers

Add the methodology banner, annotation container, comparability warning bar, and metric info popover structure to index.html. No JS wiring yet — just the DOM elements.

**Files:**
- Modify: `docs/index.html`

**Step 1: Add methodology banner after header**

Insert between `</header>` (line 18) and `<nav class="tab-bar">` (line 20):

```html
  <div class="methodology-banner">
    <p class="methodology-summary">
      <span class="methodology-item">5 runs per configuration</span>
      <span class="methodology-sep" aria-hidden="true">&middot;</span>
      <span class="methodology-item">Median values shown</span>
      <span class="methodology-sep" aria-hidden="true">&middot;</span>
      <span class="methodology-item">Error bars = &plusmn;1 stddev</span>
      <span class="methodology-sep" aria-hidden="true">&middot;</span>
      <span class="methodology-item">Fresh container per UUID type</span>
      <button class="methodology-toggle" aria-expanded="false" aria-controls="methodology-detail">Methodology</button>
    </p>
    <div id="methodology-detail" class="methodology-detail" hidden>
      <div class="methodology-detail-inner">
        <h3>Measurement Tools</h3>
        <p><strong>PostgreSQL:</strong> pgbench (built-in benchmarking tool) running inside the container via Unix socket. Latency percentiles from pgbench <code>--log</code>.</p>
        <p><strong>MySQL, MongoDB, Cassandra:</strong> Custom Go workload binary (<code>cmd/workload/main.go</code>), compiled statically, copied into each container, executed via <code>docker exec</code>. Measures per-operation latency and calculates percentiles.</p>
        <h3>I/O Measurement</h3>
        <p>All databases use Linux cgroup v2 kernel accounting (<code>io.stat</code>) for container-isolated I/O metrics. Zero overhead — kernel-level, no sampling.</p>
        <h3>Isolation</h3>
        <p>Each UUID type runs in a fresh Docker container with clean volumes. No state carries over between key types. Containers: 4 CPUs, 8 GB memory.</p>
      </div>
    </div>
  </div>
```

**Step 2: Add comparability warning bar inside main, before filter-bar**

Insert at line 28, as first child of `<main class="main-content">`:

```html
    <div class="comparability-warning" hidden>
      <span class="comparability-icon" aria-hidden="true">&#9432;</span>
      <p class="comparability-text"></p>
    </div>
```

**Step 3: Add annotation container inside chart-container, after canvas**

Replace the chart-container section (line 55-57) with:

```html
    <section id="panel-chart" class="chart-container" role="tabpanel">
      <canvas id="main-chart"></canvas>
      <div class="chart-annotation" hidden>
        <p class="annotation-finding"></p>
        <p class="annotation-explanation"></p>
      </div>
    </section>
```

**Step 4: Add info icon structure to metric filter group**

Modify the metric filter group (line 49-52) to include an info button:

```html
      <div class="filter-group" data-filter="metric">
        <label for="filter-metric">Metric <button class="metric-info-btn" aria-label="Metric information" type="button">&#9432;</button></label>
        <select id="filter-metric"></select>
        <div class="metric-info-popover" hidden>
          <div class="popover-arrow"></div>
          <p class="popover-definition"></p>
          <p class="popover-measurement"></p>
          <button class="popover-close" aria-label="Close">&times;</button>
        </div>
      </div>
```

**Step 5: Update footer**

Replace footer content (line 75-77) with:

```html
  <footer class="site-footer">
    <p>Companion to bachelor thesis at FernUniversit&auml;t in Hagen. Data from automated benchmark runs &mdash; each value is the median of 5 runs.</p>
  </footer>
```

**Step 6: Verify**

Open `docs/index.html` in a browser. New elements should be present but hidden (no visual change yet except the methodology banner text and updated footer).

**Step 7: Commit**

```bash
git add docs/index.html
git commit -m "feat(dashboard): add HTML structure for methodology banner, annotations, metric info, and comparability warnings"
```

---

### Task 2: Visual Polish — CSS Overhaul

Elevate the visual design from "default HTML with good fonts" to "editorial data journalism." Update header, tabs, filter bar, chart area, and add new component styles.

**Files:**
- Modify: `docs/assets/style.css`

**Step 1: Add new CSS custom properties**

Add to the `:root` block (after line 44, before the closing `}`):

```css
  /* New — warm header tint, warning colors, annotation accent */
  --color-header-bg:     #f5f0e8;
  --color-warning-bg:    #fefce8;
  --color-warning-border:#facc15;
  --color-warning-text:  #854d0e;
  --color-best:          #f0fdf4;
  --color-worst:         #fef2f2;
  --color-annotation-border: #d6d3cd;
```

**Step 2: Update header styles**

Replace `.site-header` block (lines 73-77) with:

```css
.site-header {
  padding: 1.5rem 1rem 1rem;
  background: var(--color-header-bg);
  border-bottom: 1px solid var(--color-border);
}
```

**Step 3: Update tab active state**

Replace `.tab-btn.active` (lines 135-138) with:

```css
.tab-btn.active {
  color: var(--color-text);
  border-bottom-color: var(--color-text);
  background: rgba(28, 25, 23, 0.04);
}
```

**Step 4: Update chart container**

Replace `.chart-container` block (lines 203-210) with:

```css
.chart-container {
  background: var(--color-surface);
  border: 1px solid var(--color-border-light);
  border-radius: var(--radius);
  padding: 1.25rem 1rem;
  min-height: 400px;
  position: relative;
  box-shadow: 0 1px 3px rgba(28, 25, 23, 0.06), 0 1px 2px rgba(28, 25, 23, 0.04);
}
```

**Step 5: Add methodology banner styles**

Append after the header section (after line 95):

```css
/* --- Methodology banner -------------------------------------------------- */
.methodology-banner {
  background: var(--color-bg);
  border-bottom: 1px solid var(--color-border-light);
  padding: 0.5rem 1rem;
}

.methodology-summary {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  gap: 0.25rem 0;
  font-family: var(--font-body);
  font-size: 0.6875rem;
  color: var(--color-text-muted);
  letter-spacing: 0.02em;
}

.methodology-item {
  font-variant: small-caps;
  text-transform: lowercase;
  font-size: 0.75rem;
  letter-spacing: 0.04em;
}

.methodology-sep {
  margin: 0 0.5rem;
  color: var(--color-text-light);
}

.methodology-toggle {
  font-family: var(--font-body);
  font-size: 0.6875rem;
  font-weight: 600;
  color: var(--color-text-muted);
  background: none;
  border: none;
  border-bottom: 1px dashed var(--color-text-light);
  cursor: pointer;
  padding: 0;
  margin-left: 0.75rem;
  transition: color 0.15s ease;
}

.methodology-toggle:hover {
  color: var(--color-text);
}

.methodology-detail {
  padding: 0;
  overflow: hidden;
}

.methodology-detail[hidden] {
  display: none;
}

.methodology-detail-inner {
  padding: 0.75rem 0 0.5rem;
  border-top: 1px solid var(--color-border-light);
  margin-top: 0.5rem;
}

.methodology-detail-inner h3 {
  font-family: var(--font-body);
  font-size: 0.6875rem;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 0.08em;
  color: var(--color-text-muted);
  margin-top: 0.625rem;
  margin-bottom: 0.25rem;
}

.methodology-detail-inner h3:first-child {
  margin-top: 0;
}

.methodology-detail-inner p {
  font-family: var(--font-body);
  font-size: 0.75rem;
  line-height: 1.5;
  color: var(--color-text);
  margin-bottom: 0.375rem;
}

.methodology-detail-inner code {
  font-family: var(--font-mono);
  font-size: 0.6875rem;
  background: rgba(28, 25, 23, 0.05);
  padding: 0.1em 0.3em;
  border-radius: 2px;
}
```

**Step 6: Add comparability warning styles**

Append after methodology banner styles:

```css
/* --- Comparability warning ----------------------------------------------- */
.comparability-warning {
  display: flex;
  align-items: flex-start;
  gap: 0.5rem;
  background: var(--color-warning-bg);
  border: 1px solid var(--color-warning-border);
  border-radius: var(--radius);
  padding: 0.5rem 0.75rem;
  margin-bottom: 0.75rem;
}

.comparability-warning[hidden] {
  display: none;
}

.comparability-icon {
  font-size: 0.875rem;
  color: var(--color-warning-text);
  flex-shrink: 0;
  line-height: 1.4;
}

.comparability-text {
  font-family: var(--font-body);
  font-size: 0.75rem;
  line-height: 1.4;
  color: var(--color-warning-text);
  margin: 0;
}
```

**Step 7: Add chart annotation styles**

Append after chart container styles:

```css
/* --- Chart annotation ---------------------------------------------------- */
.chart-annotation {
  border-top: 1px solid var(--color-border-light);
  margin-top: 1rem;
  padding-top: 0.75rem;
  border-left: 2px solid var(--color-annotation-border);
  padding-left: 0.75rem;
}

.chart-annotation[hidden] {
  display: none;
}

.annotation-finding {
  font-family: var(--font-body);
  font-size: 0.8125rem;
  font-weight: 600;
  color: var(--color-text);
  line-height: 1.45;
  margin-bottom: 0.25rem;
}

.annotation-explanation {
  font-family: var(--font-display);
  font-style: italic;
  font-size: 0.8125rem;
  font-weight: 400;
  color: var(--color-text-muted);
  line-height: 1.5;
}
```

**Step 8: Add metric info popover styles**

Append after annotation styles:

```css
/* --- Metric info popover ------------------------------------------------- */
.metric-info-btn {
  font-size: 0.6875rem;
  background: none;
  border: none;
  color: var(--color-text-light);
  cursor: pointer;
  padding: 0 0.15rem;
  vertical-align: middle;
  transition: color 0.15s ease;
}

.metric-info-btn:hover {
  color: var(--color-text);
}

.filter-group[data-filter="metric"] {
  position: relative;
}

.metric-info-popover {
  position: absolute;
  top: calc(100% + 0.5rem);
  left: 0;
  z-index: 10;
  width: 320px;
  background: var(--color-surface);
  border: 1px solid var(--color-border);
  border-radius: 4px;
  padding: 0.875rem 1rem;
  box-shadow: 0 4px 12px rgba(28, 25, 23, 0.12);
}

.metric-info-popover[hidden] {
  display: none;
}

.popover-arrow {
  position: absolute;
  top: -6px;
  left: 1rem;
  width: 10px;
  height: 10px;
  background: var(--color-surface);
  border-left: 1px solid var(--color-border);
  border-top: 1px solid var(--color-border);
  transform: rotate(45deg);
}

.popover-definition {
  font-family: var(--font-display);
  font-style: italic;
  font-size: 0.8125rem;
  line-height: 1.5;
  color: var(--color-text);
  margin-bottom: 0.5rem;
}

.popover-measurement {
  font-family: var(--font-body);
  font-size: 0.6875rem;
  line-height: 1.45;
  color: var(--color-text-muted);
}

.popover-close {
  position: absolute;
  top: 0.375rem;
  right: 0.5rem;
  font-size: 1rem;
  background: none;
  border: none;
  color: var(--color-text-light);
  cursor: pointer;
  padding: 0.125rem 0.25rem;
  line-height: 1;
}

.popover-close:hover {
  color: var(--color-text);
}
```

**Step 9: Add table conditional formatting classes**

Append after existing table styles (after line 322):

```css
/* Conditional formatting — best/worst per metric */
.data-table td.cell-best {
  background: var(--color-best);
}

.data-table td.cell-worst {
  background: var(--color-worst);
}

.data-table tbody tr:hover td.cell-best,
.data-table tbody tr:hover td.cell-worst {
  background: #f0eeea;
}
```

**Step 10: Update desktop breakpoint for methodology banner**

Add inside the `@media (min-width: 768px)` block:

```css
  .methodology-banner {
    padding: 0.5rem 2rem;
  }

  .methodology-summary {
    gap: 0;
  }

  .chart-container {
    padding: 2rem 1.75rem;
    min-height: 480px;
  }

  .chart-annotation {
    margin-top: 1.25rem;
    padding-top: 1rem;
  }

  .site-title {
    font-size: 2rem;
  }
```

**Step 11: Verify**

Open in browser. The header should have a warm parchment tint, active tab has subtle background, chart has soft shadow, methodology banner is visible with expandable section (not wired yet).

**Step 12: Commit**

```bash
git add docs/assets/style.css
git commit -m "feat(dashboard): visual polish — editorial header, tabs, chart shadow, new component styles"
```

---

### Task 3: Metric Definitions and Info Popover Logic

Add the METRIC_INFO data object and wire up the info icon popover.

**Files:**
- Modify: `docs/assets/app.js`

**Step 1: Add METRIC_INFO object after METRIC_LABELS (after line 50)**

```js
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
```

**Step 2: Add popover DOM references to cacheDom() (after line 144)**

Add inside `cacheDom()`:

```js
  dom.metricInfoBtn    = document.querySelector('.metric-info-btn');
  dom.metricInfoPopover = document.querySelector('.metric-info-popover');
  dom.popoverDefinition  = document.querySelector('.popover-definition');
  dom.popoverMeasurement = document.querySelector('.popover-measurement');
  dom.popoverClose       = document.querySelector('.popover-close');
```

**Step 3: Add popover binding function (after bindFilterEvents)**

```js
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
    dom.popoverMeasurement.textContent = info.comparability || info.measurement;
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
```

**Step 4: Call bindMetricInfoPopover in DOMContentLoaded (after bindFilterEvents call, line 154)**

Add after `bindFilterEvents();`:

```js
  bindMetricInfoPopover();
```

**Step 5: Verify**

Open in browser. Click the info icon next to Metric dropdown. Popover should appear with definition and measurement note for current metric. Click outside or Escape to dismiss.

**Step 6: Commit**

```bash
git add docs/assets/app.js
git commit -m "feat(dashboard): add metric definitions and info popover"
```

---

### Task 4: Methodology Banner Toggle

Wire the methodology expand/collapse button.

**Files:**
- Modify: `docs/assets/app.js`

**Step 1: Add DOM references for methodology in cacheDom()**

```js
  dom.methodologyToggle = document.querySelector('.methodology-toggle');
  dom.methodologyDetail = document.getElementById('methodology-detail');
```

**Step 2: Add toggle binding function**

```js
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
```

**Step 3: Call in DOMContentLoaded (after bindMetricInfoPopover)**

```js
  bindMethodologyToggle();
```

**Step 4: Verify**

Click "Methodology" link in banner. Panel expands showing measurement tools, I/O methodology, and isolation details. Click again to collapse. Button text toggles between "Methodology" and "Hide".

**Step 5: Commit**

```bash
git add docs/assets/app.js
git commit -m "feat(dashboard): wire methodology banner expand/collapse"
```

---

### Task 5: Comparability Warnings

Show an inline warning when the selected metric has different definitions per database.

**Files:**
- Modify: `docs/assets/app.js`

**Step 1: Add DOM reference in cacheDom()**

```js
  dom.comparabilityWarning = document.querySelector('.comparability-warning');
  dom.comparabilityText    = document.querySelector('.comparability-text');
```

**Step 2: Add warning update function**

```js
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
```

**Step 3: Call updateComparabilityWarning in renderCurrentView()**

Add at the beginning of `renderCurrentView()` function, after the panel visibility logic (around line 664):

```js
  updateComparabilityWarning();
```

**Step 4: Verify**

Select "Fragmentation" metric on Cross-UUID tab. Yellow warning bar should appear: "Different definitions per database..." Select "Throughput" — warning hides. Switch to Raw Data tab — warning always hidden.

**Step 5: Commit**

```bash
git add docs/assets/app.js
git commit -m "feat(dashboard): show comparability warnings for non-comparable metrics"
```

---

### Task 6: Annotations JSON and Rendering

Create the annotations data file and wire annotation display below charts.

**Files:**
- Create: `docs/data/annotations.json`
- Modify: `docs/assets/app.js`

**Step 1: Create annotations.json with placeholder structure**

Create `docs/data/annotations.json`. Start with a few example entries — the user will fill in the rest aligned with thesis findings:

```json
{
  "cross-uuid": {},
  "cross-db": {},
  "scale": {}
}
```

This starts empty. The user populates it with entries like:
```json
{
  "cross-uuid": {
    "postgres|insert_performance|1m|1|throughput": {
      "finding": "UUIDv7 matches Sequential within 3%.",
      "explanation": "UUIDv7 embeds a Unix timestamp in the most significant bytes. Inserts arrive in roughly monotonic order, so the B-tree appends to the rightmost leaf page instead of splitting random pages across the tree."
    }
  }
}
```

**Step 2: Add global annotation state and DOM refs**

In `app.js`, add after the `metadata` variable declaration (around line 100):

```js
let annotations = {};
```

Add in `cacheDom()`:

```js
  dom.chartAnnotation       = document.querySelector('.chart-annotation');
  dom.annotationFinding     = document.querySelector('.annotation-finding');
  dom.annotationExplanation = document.querySelector('.annotation-explanation');
```

**Step 3: Load annotations.json alongside data.json**

Replace the data loading in DOMContentLoaded (lines 156-169) with:

```js
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
```

**Step 4: Add annotation rendering function**

```js
/* --------------------------------------------------------------------------
   Chart annotation rendering
   -------------------------------------------------------------------------- */

function buildAnnotationKey() {
  var parts = [];
  var visible = TAB_FILTERS[activeTab] || [];

  // Build key from visible filters, in fixed order, excluding the dimension
  // that varies within the chart (keyType for cross-uuid, database for cross-db, scale for scale)
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
```

**Step 5: Call updateAnnotation in renderCurrentView()**

Add at the end of `renderCurrentView()`, after the switch statement (around line 693):

```js
  updateAnnotation();
```

Also add after `renderCrossDB()` call (around line 670):

```js
    renderCrossDB();
    updateAnnotation();
    return;
```

**Step 6: Verify**

Open in browser. No annotations visible yet (JSON is empty). Add a test entry to `annotations.json` matching your current filter state, reload, verify the annotation appears below the chart with finding in semi-bold and explanation in italic serif.

**Step 7: Commit**

```bash
git add docs/data/annotations.json docs/assets/app.js
git commit -m "feat(dashboard): add annotation system with JSON data file and chart caption rendering"
```

---

### Task 7: Table Conditional Formatting

Add green/rose tinting for best/worst values in the Raw Data table.

**Files:**
- Modify: `docs/assets/app.js`

**Step 1: Modify renderRawData to compute best/worst per metric**

In the `renderRawData` function, after sorting entries and before building rows (around line 1244), add best/worst computation:

```js
  // Compute best/worst median per metric for conditional formatting
  // "best" depends on metric: lower is better for latency, higher for throughput/cache
  var LOWER_IS_BETTER = ['p50_latency_us', 'p95_latency_us', 'p99_latency_us',
    'fragmentation', 'sstable_count', 'bloom_filter_fp'];

  var metricBestWorst = {};
  sorted.forEach(function (e) {
    if (e.median == null) return;
    if (!metricBestWorst[e.metric]) {
      metricBestWorst[e.metric] = { best: e.median, worst: e.median, bestKt: e.keyType, worstKt: e.keyType };
    }
    var mw = metricBestWorst[e.metric];
    var lowerBetter = LOWER_IS_BETTER.indexOf(e.metric) >= 0;
    if (lowerBetter) {
      if (e.median < mw.best) { mw.best = e.median; mw.bestKt = e.keyType; }
      if (e.median > mw.worst) { mw.worst = e.median; mw.worstKt = e.keyType; }
    } else {
      if (e.median > mw.best) { mw.best = e.median; mw.bestKt = e.keyType; }
      if (e.median < mw.worst) { mw.worst = e.median; mw.worstKt = e.keyType; }
    }
  });
```

**Step 2: Apply cell-best/cell-worst classes in the row builder**

In the existing row builder loop (where `td` elements are created), for the `median` column, add class logic:

Replace the generic numeric cell handling with conditional formatting for the median column. After `td.textContent = formatNumber(val);` for the median column, add:

```js
      if (col.key === 'median' && e.median != null && metricBestWorst[e.metric]) {
        var mw = metricBestWorst[e.metric];
        if (mw.best !== mw.worst) {
          if (e.median === mw.best) td.classList.add('cell-best');
          if (e.median === mw.worst) td.classList.add('cell-worst');
        }
      }
```

**Step 3: Verify**

Open Raw Data tab. Best median per metric row should have subtle green tint, worst should have subtle rose tint. When all values are identical (e.g., single entry), no coloring applied.

**Step 4: Commit**

```bash
git add docs/assets/app.js
git commit -m "feat(dashboard): conditional formatting for best/worst values in raw data table"
```

---

### Task 8: Chart Title in Crimson Pro

Update Chart.js title rendering to use the display font for a journal figure feel.

**Files:**
- Modify: `docs/assets/app.js`

**Step 1: Replace all chart title font families**

In `renderCrossUUID`, `renderCrossDB`, and `renderScale`, replace the chart title `font.family` from:

```js
family: '"DM Sans", "Helvetica Neue", Arial, sans-serif'
```

to:

```js
family: '"Crimson Pro", Georgia, serif'
```

This applies to the `plugins.title.font` object in all three chart render functions. There are 3 occurrences total (one per function).

**Step 2: Verify**

Open each chart tab. The chart title should now render in Crimson Pro serif font, matching the page header aesthetic.

**Step 3: Commit**

```bash
git add docs/assets/app.js
git commit -m "style(dashboard): use Crimson Pro for chart titles (journal figure aesthetic)"
```

---

### Task 9: Final Integration Verify

Verify everything works together across all tabs and filter combinations.

**Step 1: Test Cross-UUID tab**
- Select different databases, scenarios, metrics
- Verify: methodology banner, filter bar, chart renders, annotation area (hidden if no annotation), comparability warning appears for fragmentation/leaf_density

**Step 2: Test Cross-DB tab**
- Verify: chart renders, legend visible, comparability warnings hidden (non-comparable metrics already filtered out)

**Step 3: Test Scale tab**
- Verify: line chart with multiple series, legend, error bars

**Step 4: Test Raw Data tab**
- Verify: conditional formatting (green best, rose worst per metric), sorting works, comparability warning hidden, annotation hidden

**Step 5: Test methodology banner**
- Click toggle: expands/collapses
- Content readable and formatted

**Step 6: Test metric info popover**
- Click info icon: popover appears with correct metric info
- Switch metric via dropdown: popover closes (next click shows new metric)
- Click outside / Escape: dismisses popover

**Step 7: Commit any fixes, then final commit if needed**

No commit needed if all checks pass.
