# Dashboard Redesign Implementation Plan

> **For Claude:** REQUIRED SUB-SKILLS:
> - Use `frontend-design` skill before writing any code (monochrome terminal aesthetic, Courier New, observatory-inspired)
> - Use `superpowers:executing-plans` to implement this plan task-by-task.

**Goal:** Rewrite the docs/ GitHub Pages dashboard from a single-chart academic journal style to a multi-panel "research terminal" aesthetic with 3-view navigation (Summary / Explorer / Raw Data), ES module architecture, and guided findings.

**Architecture:** Clean rewrite of `docs/` static site. Vanilla JS + Chart.js (no framework, no build step). 9 ES module files replace the monolithic `app.js`. Data files (`data.json`, `annotations.json`) are unchanged. Monochrome terminal palette where the only color comes from benchmark data.

**Tech Stack:** Vanilla JavaScript (ES modules), Chart.js 4.4.7 (CDN), Courier New system font (no external fonts), CSS custom properties, GitHub Pages static hosting.

**Design document:** `docs/plans/2026-03-04-dashboard-redesign-design.md` — contains full visual spec, color tokens, type scale, layout diagrams, and component descriptions. Read this FIRST before implementing.

---

## Reference: Current File Inventory

**Files being REPLACED (do not modify — rewrite from scratch):**
- `docs/index.html` (120 lines)
- `docs/assets/app.js` (1,585 lines — monolithic)
- `docs/assets/style.css` (711 lines)

**Files UNCHANGED (do not modify):**
- `docs/data/data.json` (63,729 lines — benchmark results)
- `docs/data/annotations.json` (156 lines — curated findings)

**New file structure after rewrite:**
```
docs/
├── index.html                     # Single HTML document
├── assets/
│   ├── style.css                  # All styles
│   ├── constants.js               # Colors, labels, metric info
│   ├── data.js                    # Data loading, filter state, cascading
│   ├── charts.js                  # Chart.js builders, error bar plugin
│   ├── annotations.js             # Finding display, next/prev navigation
│   ├── summary.js                 # Summary view rendering
│   ├── explorer.js                # Explorer view (4-panel grid)
│   ├── rawdata.js                 # Raw Data table
│   └── app.js                     # Entry: init, routing, URL state
└── data/
    ├── data.json                  # (unchanged)
    └── annotations.json           # (unchanged)
```

---

## Reference: Design Tokens

These are defined in the design doc and must be used consistently:

```css
/* Monochrome base — only color comes from data */
--bg: #ffffff;
--bg-off: #f7f7f7;
--bg-dark: #eeeeee;
--border: #cccccc;
--border-strong: #333333;
--text: #111111;
--text-mid: #444444;
--text-muted: #888888;
--text-faint: #bbbbbb;

/* Font: system monospace only */
--font-mono: "Courier New", Courier, monospace;

/* Key type colors */
--key-sequential: #78716c;
--key-objectid: #a16207;
--key-uuidv1: #be123c;
--key-uuidv4: #1d4ed8;
--key-uuidv7: #047857;
--key-ulid: #7e22ce;
--key-ulid-monotonic: #a855f7;

/* Database colors */
--db-postgres: #336791;
--db-mysql: #00758f;
--db-mongodb: #116149;
--db-cassandra: #1287B1;
```

**Typography rules:**
- Minimum font size: 10px (never 8-9px)
- Section labels: `uppercase`, `letter-spacing: 2-2.5px`
- Tab labels: `uppercase`, `letter-spacing: 1.5px`
- All numbers: `font-variant-numeric: tabular-nums`
- No border-radius anywhere (square corners)
- No box-shadows (1px borders only)
- No external fonts (Courier New is system font)

---

## Task 1: Create `constants.js` — Configuration Module

**Files:**
- Create: `docs/assets/constants.js`

**What:** Extract ALL constants from current `app.js` (lines 11-822) into a clean ES module. This is the single source of truth for colors, labels, metric info, orderings, and metric groups.

**Step 1: Write `constants.js`**

Export the following (copy exact values from current `app.js`):

```javascript
// From app.js lines 11-18
export const KEY_TYPE_COLORS = { SEQUENTIAL: '#78716c', OBJECTID: '#a16207', ... };

// From app.js lines 22-30
export const KEY_TYPE_DASH = { SEQUENTIAL: [], OBJECTID: [2, 3], ... };

// From app.js lines 33-41
export const KEY_TYPE_POINT_STYLE = { SEQUENTIAL: 'circle', OBJECTID: 'triangle', ... };

// From app.js lines 43-48
export const DATABASE_COLORS = { postgres: '#336791', mysql: '#00758f', ... };

// From app.js lines 54-72
export const METRIC_LABELS = { throughput: 'Throughput (ops/s)', ... };

// From app.js lines 75-150 (FULL object with all 17 metrics)
export const METRIC_INFO = { throughput: { definition: '...', measurement: '...' }, ... };

// From app.js lines 152-167
export const DATABASE_LABELS = { postgres: 'PostgreSQL', ... };
export const KEY_TYPE_LABELS = { SEQUENTIAL: 'Sequential', ... };

// From app.js lines 809-822
export const KEY_TYPE_ORDER = ['SEQUENTIAL', 'OBJECTID', 'UUIDV1', 'UUIDV4', 'UUIDV7', 'ULID', 'ULID_MONOTONIC'];
export const DATABASE_ORDER = ['postgres', 'mysql', 'mongodb', 'cassandra'];
export const SCALE_ORDER = ['100k', '1m', '10m', '100m'];
export const METRIC_GROUPS = [ { label: 'Performance', metrics: ['throughput'] }, ... ];

// From app.js lines 169-193 (formatting functions)
export function formatMetricName(metric) { ... }
export function formatScenarioName(scenario) { ... }
export function formatKeyTypeName(keyType) { ... }
export function formatDatabaseName(db) { ... }

// From app.js lines 923-930
export function formatNumber(val) { ... }

// From app.js lines 1532-1533
export const LOWER_IS_BETTER = ['p50_latency_us', 'p95_latency_us', 'p99_latency_us',
  'fragmentation', 'page_splits', 'sstable_count', 'bloom_filter_fp'];

// NEW: Default panels for the 4-panel Explorer grid
export const EXPLORER_PANELS = {
  default: ['throughput', 'p50_latency_us', 'page_splits', 'cache_hit_ratio'],
  cassandra: ['throughput', 'p50_latency_us', 'sstable_count', 'cache_hit_ratio'],
};

// NEW: Panel display config
export const PANEL_CONFIG = {
  throughput:       { label: 'THROUGHPUT',       unit: 'ops/sec' },
  p50_latency_us:   { label: 'LATENCY',          unit: 'μs' },
  p95_latency_us:   { label: 'P95 LATENCY',      unit: 'μs' },
  p99_latency_us:   { label: 'P99 LATENCY',      unit: 'μs' },
  page_splits:      { label: 'PAGE SPLITS',       unit: 'count' },
  cache_hit_ratio:  { label: 'CACHE HIT RATIO',   unit: '0-1' },
  sstable_count:    { label: 'SSTABLE COUNT',      unit: 'count' },
  fragmentation:    { label: 'FRAGMENTATION',      unit: '%' },
  table_size_mb:    { label: 'TABLE SIZE',         unit: 'MB' },
  index_size_mb:    { label: 'INDEX SIZE',         unit: 'MB' },
};
```

**Step 2: Verify**

Open browser dev tools, `import('./assets/constants.js')` from console — confirm all exports are accessible.

**Step 3: Commit**

```
feat(docs): extract constants into ES module
```

---

## Task 2: Create `data.js` — Data Loading & Filter State

**Files:**
- Create: `docs/assets/data.js`

**What:** Extract data loading, global filter state, cascading filter logic, and filtered entry queries from current `app.js` (lines 195-778). This module owns all state and is imported by every view module.

**Step 1: Write `data.js`**

Extract and export:

```javascript
import { KEY_TYPE_ORDER, DATABASE_ORDER, SCALE_ORDER, METRIC_GROUPS, METRIC_INFO,
         formatDatabaseName, formatKeyTypeName, formatScenarioName, formatMetricName }
  from './constants.js';

// --- Global state (module-scoped, exported for views to read) ---
export let allEntries = [];
export let metadata = {};
export let annotations = {};

// --- Filter state ---
export const filterState = {
  database: null, keyType: null, scenario: null,
  scale: null, connections: null, metric: null,
};

// --- Which filters are visible per view mode ---
// (Adapted from app.js TAB_FILTERS, renamed for new view structure)
export const VIEW_FILTERS = {
  'cross-uuid': ['database', 'scenario', 'scale', 'connections'],
  'cross-db':   ['keyType', 'scenario', 'scale', 'connections'],
  'scale':      ['database', 'scenario', 'connections'],
  'raw-data':   ['database', 'scenario', 'scale', 'connections'],
};

// --- Data loading ---
export async function loadData() { /* fetch data.json + annotations.json */ }

// --- Filter logic (extracted from app.js lines 507-756) ---
export function coerceFilterValue(key, val) { ... }
export function populateFilters(activeMode, domFilters) { ... }
export function cascadeFilters(changedKey, activeMode, domFilters) { ... }
export function deriveValidOptions(key, visible) { ... }
export function matchesPartial(entry, partial) { ... }
export function sortComparator(key, a, b) { ... }
export function formatOption(key, val) { ... }
export function getFilteredEntries(activeMode) { ... }

// --- NEW: Get entries for a specific metric (for multi-panel) ---
export function getEntriesForMetric(activeMode, metric) {
  // Like getFilteredEntries but overrides filterState.metric with the given metric
}
```

**Key changes from current app.js:**
- `TAB_FILTERS` → `VIEW_FILTERS` with updated keys matching new sub-tab names
- `populateFilters` and `cascadeFilters` accept DOM filter references as parameters (no global `dom` object)
- `getEntriesForMetric()` is NEW — needed for 4-panel grid where each panel queries a different metric
- Remove `metric` from visible filters in VIEW_FILTERS — metric is no longer a dropdown; it's determined by the 4-panel grid
- The cascading logic for Cross-DB (lines 648-665) and Scale (lines 669-686) filters is preserved exactly

**Step 2: Verify**

Console test: `import { loadData, allEntries } from './assets/data.js'; await loadData(); console.log(allEntries.length);` — should output a large number.

**Step 3: Commit**

```
feat(docs): extract data loading and filter state into ES module
```

---

## Task 3: Create `charts.js` — Chart.js Builders

**Files:**
- Create: `docs/assets/charts.js`

**What:** Extract Chart.js configuration, error bar plugin, and chart-building functions from current `app.js` (lines 828-1430). Adapt for the new monochrome aesthetic and multi-panel usage.

**Step 1: Write `charts.js`**

```javascript
import { KEY_TYPE_COLORS, KEY_TYPE_DASH, KEY_TYPE_POINT_STYLE, KEY_TYPE_ORDER,
         DATABASE_COLORS, DATABASE_ORDER, SCALE_ORDER,
         formatKeyTypeName, formatDatabaseName, formatNumber, METRIC_LABELS }
  from './constants.js';

// --- Chart.js shared config (updated for monochrome terminal aesthetic) ---
const CHART_FONT = '"Courier New", Courier, monospace';
const CHART_ANIMATION = { duration: 300 };
const CHART_GRID_STYLE = { color: 'rgba(0, 0, 0, 0.06)' };
const CHART_BORDER_STYLE = { color: '#333333', display: true };

// --- Error bar plugin (from app.js lines 845-888, adapt stroke color to #333) ---
export const errorBarPlugin = { ... };

// --- Tooltip config (monochrome: border, no colored bg) ---
const TOOLTIP_CONFIG = {
  backgroundColor: '#ffffff',
  borderColor: '#cccccc',
  borderWidth: 1,
  titleColor: '#111111',
  bodyColor: '#444444',
  titleFont: { family: CHART_FONT, size: 11 },
  bodyFont: { family: CHART_FONT, size: 10 },
};

// --- Chart builders ---

// Bar chart for Cross-UUID view (from app.js renderCrossUUID, lines 984-1093)
// Returns Chart.js config object (caller creates the Chart instance)
export function buildCrossUUIDChart(entries, metric) { ... }

// Grouped bar chart for Cross-DB view (from app.js renderCrossDB, lines 1099-1263)
export function buildCrossDBChart(allEntries, filterState, metric) { ... }

// Line chart for Scale view (from app.js renderScale, lines 1269-1429)
export function buildScaleChart(entries, metric) { ... }

// NEW: Mini bar chart for KPI cards (3 bars: 100K, 1M, 10M)
// Returns a tiny Chart.js config for a 80x30px canvas
export function buildMiniBarChart(values, colors) { ... }
```

**Key changes from current app.js:**
- All font families changed from `"Crimson Pro"` / `"DM Sans"` / `"JetBrains Mono"` → `"Courier New", Courier, monospace`
- All colors changed to monochrome tokens (`#111111`, `#888888`, `#cccccc`)
- Chart title removed from Chart.js config (panel label replaces it)
- Legend removed from individual charts (persistent legend strip handles this)
- `maintainAspectRatio: false` on all charts (panels control sizing)
- Functions return config objects, not Chart instances — the view module handles instantiation
- Tooltip styled monochrome (white bg, border, monospace font)
- NEW `buildMiniBarChart` for KPI cards on Summary view

**Step 2: Verify**

Open a minimal test HTML page that imports `charts.js` and creates one bar chart with dummy data. Confirm it renders with monospace fonts and monochrome styling.

**Step 3: Commit**

```
feat(docs): extract Chart.js builders into ES module
```

---

## Task 4: Create `annotations.js` — Finding Navigation

**Files:**
- Create: `docs/assets/annotations.js`

**What:** Extract annotation logic from current `app.js` (lines 455-502) and add next/prev navigation with progress tracking.

**Step 1: Write `annotations.js`**

```javascript
import { annotations } from './data.js';
import { filterState } from './data.js';

// --- State ---
let currentFindingIndex = -1;  // -1 = showing annotation for current filter, not navigating
let allFindingKeys = [];       // Ordered list of all annotation keys for current view mode

// --- Build annotation key from filter state (from app.js lines 455-475) ---
export function buildAnnotationKey(viewMode) { ... }

// --- Get annotation for current filter state ---
export function getAnnotation(viewMode) {
  const tabAnnotations = annotations[viewMode];
  if (!tabAnnotations) return null;
  const key = buildAnnotationKey(viewMode);
  return key ? tabAnnotations[key] || null : null;
}

// --- NEW: Enumerate all annotations for a view mode ---
export function getAllAnnotationKeys(viewMode) {
  const tabAnnotations = annotations[viewMode];
  if (!tabAnnotations) return [];
  return Object.keys(tabAnnotations);
}

// --- NEW: Navigate to next/prev finding ---
export function initFindingNavigation(viewMode) {
  allFindingKeys = getAllAnnotationKeys(viewMode);
  // Try to find current filter state in the list
  const currentKey = buildAnnotationKey(viewMode);
  currentFindingIndex = allFindingKeys.indexOf(currentKey);
  if (currentFindingIndex === -1) currentFindingIndex = 0;
}

export function nextFinding(viewMode) {
  if (allFindingKeys.length === 0) return null;
  currentFindingIndex = (currentFindingIndex + 1) % allFindingKeys.length;
  return parseFindingKey(viewMode, allFindingKeys[currentFindingIndex]);
}

export function prevFinding(viewMode) {
  if (allFindingKeys.length === 0) return null;
  currentFindingIndex = (currentFindingIndex - 1 + allFindingKeys.length) % allFindingKeys.length;
  return parseFindingKey(viewMode, allFindingKeys[currentFindingIndex]);
}

export function getFindingProgress() {
  return { current: currentFindingIndex + 1, total: allFindingKeys.length };
}

// --- Parse annotation key back into filter values ---
// Key format: "database|scenario|scale|connections|metric" (for cross-uuid)
function parseFindingKey(viewMode, key) {
  const parts = key.split('|');
  // Returns object with filter values to apply
  // Depends on viewMode determining which filters the parts map to
}

// --- Render annotation into DOM ---
export function renderAnnotation(container, viewMode) {
  // container has: .annotation-finding, .annotation-explanation,
  //                .annotation-progress, .annotation-prev, .annotation-next
}
```

**Step 2: Verify**

Console test: import and call `getAllAnnotationKeys('cross-uuid')` — should return array of annotation keys.

**Step 3: Commit**

```
feat(docs): add annotation navigation with next/prev and progress
```

---

## Task 5: Create `style.css` — Complete Monochrome Terminal Stylesheet

**Files:**
- Create: `docs/assets/style.css` (replaces existing entirely)

**What:** Complete rewrite of CSS for the monochrome terminal aesthetic. This is the largest single file and defines the entire visual identity.

**Step 1: Write `style.css`**

Structure the CSS in this order:

```css
/* 1. Custom properties (design tokens from Section 4 of design doc) */
/* 2. Reset & base (box-sizing, body with Courier New) */
/* 3. Page shell: header, tab bar, footer */
/* 4. Summary view: methodology banner, KPI cards, DB cards, legend */
/* 5. Explorer view: filter bar, sub-tabs, chart grid, panels, expand */
/* 6. Raw Data view: table styles */
/* 7. Annotations section */
/* 8. Comparability warning */
/* 9. Metric info popover */
/* 10. No-data state */
/* 11. Animations (fade-up for KPI cards, crossfade for views) */
/* 12. Responsive: tablet (768px) */
/* 13. Responsive: desktop (1024px) */
/* 14. Reduced motion */
/* 15. Print */
```

**Critical style rules to implement:**

```css
/* Body — Courier New, white bg */
body {
  font-family: "Courier New", Courier, monospace;
  font-size: 11px;
  color: #111111;
  background: #ffffff;
}

/* ALL borders: 1px solid #cccccc, no radius, no shadow */
/* Section labels: uppercase, letter-spacing 2.5px, 10px, font-weight 700 */
/* Tab underline: border-bottom 2px solid #333333 */
/* KPI hero numbers: 24-28px, font-weight 700 */
/* Panels: border 1px solid #cccccc, no radius */
/* Chart grid: display grid, grid-template-columns 1fr 1fr, gap 1px (border between) */
/* Panel expand: grid-column span 2, height transition 250ms */

/* Animations */
@keyframes fade-up {
  from { opacity: 0; transform: translateY(12px); }
  to { opacity: 1; transform: translateY(0); }
}
.kpi-card { animation: fade-up 500ms cubic-bezier(0.25, 0.1, 0.25, 1) both; }
.kpi-card:nth-child(1) { animation-delay: 0ms; }
.kpi-card:nth-child(2) { animation-delay: 50ms; }
.kpi-card:nth-child(3) { animation-delay: 100ms; }
.kpi-card:nth-child(4) { animation-delay: 150ms; }

/* Reduced motion */
@media (prefers-reduced-motion: reduce) {
  *, *::before, *::after {
    animation-duration: 0.01ms !important;
    transition-duration: 0.01ms !important;
  }
}
```

**Responsive breakpoints:**
- Mobile-first (default): single column everything, vertical filter stack
- `768px`: horizontal filter bar, 2×2 chart grid, KPI cards 2×2
- `1024px`: taller charts, wider filters

**Mobile Explorer:** Show 2 panels by default, `[SHOW ALL METRICS ▸]` toggle reveals other 2.

**Step 2: Verify**

Open `index.html` in browser — should see the monochrome terminal aesthetic. Check: no border-radius anywhere, no shadows, Courier New renders, colors are monochrome.

**Step 3: Commit**

```
feat(docs): rewrite stylesheet for monochrome terminal aesthetic
```

---

## Task 6: Create `index.html` — Page Shell

**Files:**
- Create: `docs/index.html` (replaces existing entirely)

**What:** The single HTML document with semantic structure for all 3 views. No external font loading (Courier New is system font). Chart.js from CDN. All JS via `<script type="module">`.

**Step 1: Write `index.html`**

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>UUID Benchmark</title>
  <link rel="stylesheet" href="assets/style.css">
  <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.7" defer></script>
  <script type="module" src="assets/app.js"></script>
</head>
<body>
  <!-- HEADER -->
  <header class="site-header">
    <span class="site-brand">UUID BENCHMARK</span>
    <span class="site-divider">|</span>
    <span class="site-subtitle">Cross-database performance comparison of identifier strategies</span>
  </header>

  <!-- MAIN NAVIGATION -->
  <nav class="main-nav" role="tablist" aria-label="View selection">
    <button class="nav-tab active" role="tab" aria-selected="true" data-view="summary">▸ SUMMARY</button>
    <button class="nav-tab" role="tab" aria-selected="false" data-view="explorer">EXPLORER</button>
    <button class="nav-tab" role="tab" aria-selected="false" data-view="raw-data">RAW DATA</button>
  </nav>

  <!-- SUMMARY VIEW -->
  <main id="view-summary" class="view-panel" role="tabpanel">
    <!-- Methodology banner -->
    <section class="methodology-banner">
      <p class="methodology-line">
        5 runs per configuration · Median values · ±1 stddev · Fresh container per UUID type · 100K / 1M / 10M records
        <button class="methodology-toggle" aria-expanded="false">▸ Details</button>
      </p>
      <div class="methodology-detail" hidden>
        <!-- Measurement tools, I/O measurement, isolation details -->
        <!-- (Same content as current index.html methodology-detail-inner) -->
      </div>
    </section>

    <!-- KPI Finding Cards -->
    <section class="kpi-section">
      <h2 class="section-label">KEY FINDINGS</h2>
      <div class="kpi-grid">
        <div class="kpi-card" data-finding="insert">
          <div class="kpi-icon">◎</div>
          <div class="kpi-label">INSERT PENALTY</div>
          <div class="kpi-hero">-13.6% to -30%</div>
          <div class="kpi-context">UUIDv4 vs sequential</div>
          <canvas class="kpi-chart" width="80" height="30"></canvas>
        </div>
        <div class="kpi-card" data-finding="structural">
          <div class="kpi-icon">◎</div>
          <div class="kpi-label">STRUCTURAL DAMAGE</div>
          <div class="kpi-hero">50%</div>
          <div class="kpi-context">leaf fragmentation, PG B-tree</div>
          <canvas class="kpi-chart" width="80" height="30"></canvas>
        </div>
        <div class="kpi-card" data-finding="scale">
          <div class="kpi-icon">◎</div>
          <div class="kpi-label">SCALE EFFECT</div>
          <div class="kpi-hero">-73%</div>
          <div class="kpi-context">UUIDv4 at 10M, MySQL</div>
          <canvas class="kpi-chart" width="80" height="30"></canvas>
        </div>
        <div class="kpi-card" data-finding="engine">
          <div class="kpi-icon">◎</div>
          <div class="kpi-label">BEST BALANCE</div>
          <div class="kpi-hero">UUIDv7</div>
          <div class="kpi-context">across all databases</div>
          <canvas class="kpi-chart" width="80" height="30"></canvas>
        </div>
      </div>
    </section>

    <!-- Database Overview -->
    <section class="db-section">
      <h2 class="section-label">DATABASES TESTED</h2>
      <div class="db-grid">
        <div class="db-card" data-db="postgres">
          <div class="db-name">POSTGRESQL</div>
          <div class="db-arch">B-tree / heap-organized</div>
          <div class="db-tool">pgbench</div>
          <button class="db-explore" data-db="postgres">▸ explore</button>
        </div>
        <div class="db-card" data-db="mysql">
          <div class="db-name">MYSQL</div>
          <div class="db-arch">Clustered B-tree (InnoDB)</div>
          <div class="db-tool">Go workload binary</div>
          <button class="db-explore" data-db="mysql">▸ explore</button>
        </div>
        <div class="db-card" data-db="mongodb">
          <div class="db-name">MONGODB</div>
          <div class="db-arch">WiredTiger B-tree index</div>
          <div class="db-tool">Go workload binary</div>
          <button class="db-explore" data-db="mongodb">▸ explore</button>
        </div>
        <div class="db-card" data-db="cassandra">
          <div class="db-name">CASSANDRA</div>
          <div class="db-arch">LSM-tree / SSTables</div>
          <div class="db-tool">Go workload binary</div>
          <button class="db-explore" data-db="cassandra">▸ explore</button>
        </div>
      </div>
    </section>

    <!-- UUID Types Legend -->
    <section class="legend-section">
      <h2 class="section-label">UUID TYPES TESTED</h2>
      <div class="legend-strip" id="summary-legend">
        <!-- Populated by summary.js -->
      </div>
    </section>
  </main>

  <!-- EXPLORER VIEW -->
  <main id="view-explorer" class="view-panel" role="tabpanel" hidden>
    <!-- Compact methodology line -->
    <div class="explorer-methodology">
      5 runs per configuration · Median values · ±1 stddev · Fresh container per UUID type
    </div>

    <!-- Filter bar -->
    <section class="filter-bar" aria-label="Filters">
      <div class="filter-group" data-filter="database">
        <label for="filter-database">DATABASE</label>
        <select id="filter-database"></select>
      </div>
      <div class="filter-group" data-filter="keyType">
        <label for="filter-keyType">KEY TYPE</label>
        <select id="filter-keyType"></select>
      </div>
      <div class="filter-group" data-filter="scenario">
        <label for="filter-scenario">SCENARIO</label>
        <select id="filter-scenario"></select>
      </div>
      <div class="filter-group" data-filter="scale">
        <label for="filter-scale">SCALE</label>
        <select id="filter-scale"></select>
      </div>
      <div class="filter-group" data-filter="connections">
        <label for="filter-connections">CONNECTIONS</label>
        <select id="filter-connections"></select>
      </div>
    </section>

    <!-- Explorer sub-tabs (view modes) -->
    <nav class="explorer-tabs" role="tablist" aria-label="Analysis mode">
      <button class="explorer-tab active" role="tab" aria-selected="true" data-mode="cross-uuid">CROSS-UUID</button>
      <button class="explorer-tab" role="tab" aria-selected="false" data-mode="cross-db">CROSS-DB</button>
      <button class="explorer-tab" role="tab" aria-selected="false" data-mode="scale">SCALE</button>
    </nav>

    <!-- Legend strip -->
    <div class="legend-strip" id="explorer-legend">
      <!-- Populated by explorer.js: colored squares + labels -->
    </div>

    <!-- Comparability warning -->
    <div class="comparability-warning" hidden>
      <span class="comparability-icon">ⓘ</span>
      <p class="comparability-text"></p>
    </div>

    <!-- 4-panel chart grid -->
    <div class="chart-grid">
      <div class="chart-panel" data-panel="0">
        <div class="panel-header">
          <span class="panel-label">THROUGHPUT</span>
          <span class="panel-unit">ops/sec</span>
          <button class="panel-expand" aria-label="Expand panel">⤢</button>
        </div>
        <div class="panel-chart"><canvas></canvas></div>
      </div>
      <div class="chart-panel" data-panel="1">
        <div class="panel-header">
          <span class="panel-label">LATENCY</span>
          <span class="panel-unit">μs</span>
          <button class="panel-expand" aria-label="Expand panel">⤢</button>
        </div>
        <div class="panel-chart"><canvas></canvas></div>
      </div>
      <div class="chart-panel" data-panel="2">
        <div class="panel-header">
          <span class="panel-label">PAGE SPLITS</span>
          <span class="panel-unit">count</span>
          <button class="panel-expand" aria-label="Expand panel">⤢</button>
        </div>
        <div class="panel-chart"><canvas></canvas></div>
      </div>
      <div class="chart-panel" data-panel="3">
        <div class="panel-header">
          <span class="panel-label">CACHE HIT RATIO</span>
          <span class="panel-unit">0-1</span>
          <button class="panel-expand" aria-label="Expand panel">⤢</button>
        </div>
        <div class="panel-chart"><canvas></canvas></div>
      </div>
    </div>

    <!-- Mobile: show all metrics toggle -->
    <button class="show-all-metrics" hidden>SHOW ALL METRICS ▸</button>

    <!-- Annotation section -->
    <section class="annotation-section" aria-live="polite">
      <div class="annotation-header">
        <span class="annotation-icon">◎</span>
        <span class="annotation-title">FINDING</span>
        <span class="annotation-progress"></span>
        <div class="annotation-nav">
          <button class="annotation-prev" aria-label="Previous finding">◂ prev</button>
          <button class="annotation-next" aria-label="Next finding">next ▸</button>
        </div>
      </div>
      <p class="annotation-finding"></p>
      <p class="annotation-explanation"></p>
    </section>

    <!-- No data state -->
    <div class="no-data" hidden>
      <p>— No data available for this filter combination —</p>
    </div>
  </main>

  <!-- RAW DATA VIEW -->
  <main id="view-raw-data" class="view-panel" role="tabpanel" hidden>
    <!-- Filter bar (same structure, allows "All" option) -->
    <section class="filter-bar raw-filter-bar" aria-label="Filters">
      <div class="filter-group" data-filter="database">
        <label for="raw-filter-database">DATABASE</label>
        <select id="raw-filter-database"></select>
      </div>
      <div class="filter-group" data-filter="scenario">
        <label for="raw-filter-scenario">SCENARIO</label>
        <select id="raw-filter-scenario"></select>
      </div>
      <div class="filter-group" data-filter="scale">
        <label for="raw-filter-scale">SCALE</label>
        <select id="raw-filter-scale"></select>
      </div>
      <div class="filter-group" data-filter="connections">
        <label for="raw-filter-connections">CONNECTIONS</label>
        <select id="raw-filter-connections"></select>
      </div>
    </section>

    <!-- Table -->
    <div class="table-scroll">
      <table class="data-table">
        <thead><tr></tr></thead>
        <tbody></tbody>
      </table>
    </div>
    <div class="table-footer">
      <span class="table-sort-info"></span>
      <span class="table-count"></span>
    </div>
  </main>

  <!-- FOOTER -->
  <footer class="site-footer">
    <span>UUID Benchmark · Eike Rackwitz · 2026</span>
    <span class="footer-sep">·</span>
    <a href="https://github.com/erackwitz/uuid-benchmark" class="footer-link">GitHub</a>
  </footer>
</body>
</html>
```

**Step 2: Verify**

Open in browser — should see the page shell with header, tabs, and empty views. Clicking tabs should show/hide views (once app.js is wired up).

**Step 3: Commit**

```
feat(docs): rewrite index.html with 3-view structure
```

---

## Task 7: Create `summary.js` — Summary View

**Files:**
- Create: `docs/assets/summary.js`

**What:** Renders the Summary/landing view: populates the UUID types legend with colored squares, renders mini bar charts in KPI cards, and handles "explore" button clicks that navigate to Explorer with pre-set filters.

**Step 1: Write `summary.js`**

```javascript
import { KEY_TYPE_COLORS, KEY_TYPE_ORDER, KEY_TYPE_LABELS, DATABASE_COLORS } from './constants.js';
import { allEntries, filterState } from './data.js';
import { buildMiniBarChart } from './charts.js';

export function initSummary() {
  renderLegend();
  renderKPICharts();
  bindExploreButtons();
  bindMethodologyToggle();
}

function renderLegend() {
  // Populate #summary-legend with colored ■ squares + labels
  // One item per key type: <span class="legend-item"><span class="legend-swatch" style="background: #color"></span> LABEL</span>
}

function renderKPICharts() {
  // For each .kpi-chart canvas, create a tiny 3-bar chart
  // INSERT card: UUIDv4 throughput at 100K, 1M, 10M for postgres
  // STRUCTURAL card: UUIDv4 fragmentation at 100K, 1M, 10M for postgres
  // SCALE card: UUIDv4 throughput penalty % at 100K, 1M, 10M for mysql
  // ENGINE card: UUIDv7 throughput as % of baseline across 4 databases at 1M
  // Query allEntries directly for these specific values
}

function bindExploreButtons() {
  // Each .db-explore button: set filterState.database, switch to Explorer view
  // Each .kpi-card: clickable, navigates to Explorer with relevant pre-set filters
}

function bindMethodologyToggle() {
  // Toggle methodology-detail hidden/shown
  // (Same logic as current app.js lines 421-430)
}

export function destroySummary() {
  // Destroy any mini chart instances
}
```

**Step 2: Verify**

Navigate to Summary view in browser — KPI cards should show mini bar charts, legend should show colored squares, "explore" buttons should switch to Explorer.

**Step 3: Commit**

```
feat(docs): implement Summary view with KPI cards and navigation
```

---

## Task 8: Create `explorer.js` — Explorer View (4-Panel Grid)

**Files:**
- Create: `docs/assets/explorer.js`

**What:** The core analysis view. Manages the 4-panel chart grid, sub-tab switching (Cross-UUID / Cross-DB / Scale), filter bar, panel expand/collapse, legend strip, comparability warnings, and metric info popovers.

This is the most complex module. It replaces the current `renderCrossUUID`, `renderCrossDB`, `renderScale` functions plus all the associated UI logic.

**Step 1: Write `explorer.js`**

```javascript
import { KEY_TYPE_COLORS, KEY_TYPE_ORDER, DATABASE_COLORS, DATABASE_ORDER,
         EXPLORER_PANELS, PANEL_CONFIG, METRIC_INFO,
         formatKeyTypeName, formatDatabaseName } from './constants.js';
import { filterState, allEntries, populateFilters, cascadeFilters,
         getFilteredEntries, getEntriesForMetric, VIEW_FILTERS } from './data.js';
import { buildCrossUUIDChart, buildCrossDBChart, buildScaleChart, errorBarPlugin } from './charts.js';
import { renderAnnotation, initFindingNavigation, nextFinding, prevFinding,
         getFindingProgress } from './annotations.js';

// --- State ---
let activeMode = 'cross-uuid';    // 'cross-uuid' | 'cross-db' | 'scale'
let chartInstances = [null, null, null, null];  // 4 Chart.js instances
let expandedPanel = -1;            // -1 = none expanded

// --- DOM refs (cached on init) ---
let dom = {};

export function initExplorer(initialFilters) {
  cacheDom();
  bindSubTabs();
  bindFilterEvents();
  bindPanelExpand();
  bindAnnotationNav();
  bindMobileToggle();

  // Apply initial filters if coming from Summary "explore" click
  if (initialFilters) {
    Object.assign(filterState, initialFilters);
  }

  updateFilterVisibility();
  populateFilters(activeMode, dom.filters);
  renderExplorer();
}

function cacheDom() {
  // Cache all Explorer-specific DOM elements
  dom.filters = { database, keyType, scenario, scale, connections };
  dom.panels = document.querySelectorAll('.chart-panel');
  dom.legend = document.getElementById('explorer-legend');
  dom.comparabilityWarning = ...;
  dom.annotationSection = ...;
  dom.noData = ...;
}

function bindSubTabs() {
  // On click: update activeMode, update filter visibility
  // (Cross-DB shows keyType filter, hides database; Cross-UUID shows database, hides keyType; Scale hides scale)
  // Re-populate filters, re-render
}

function bindFilterEvents() {
  // On change: cascade filters, re-render all 4 panels
  // (Same cascading logic as current, but no metric dropdown — panels handle metrics)
}

function bindPanelExpand() {
  // On click .panel-expand: toggle expanded state
  // Expanded: add class 'expanded' (CSS: grid-column: span 2, taller)
  // Collapsed: remove class
  // Re-render chart in expanded panel (larger canvas)
}

function bindAnnotationNav() {
  // .annotation-next → nextFinding() → apply returned filters → re-render
  // .annotation-prev → prevFinding() → apply returned filters → re-render
  // Update progress display
}

function updateFilterVisibility() {
  // Show/hide filter groups based on activeMode
  // Cross-UUID: show database, scenario, scale, connections
  // Cross-DB: show keyType, scenario, scale, connections
  // Scale: show database, scenario, connections (scale becomes X-axis)
}

function renderExplorer() {
  // 1. Determine which 4 metrics to show (EXPLORER_PANELS.default or .cassandra)
  // 2. For each panel (0-3):
  //    a. Get the metric for this panel
  //    b. Update panel label and unit from PANEL_CONFIG
  //    c. Get entries for this metric
  //    d. Build chart config based on activeMode (bar/line)
  //    e. Destroy old chart instance, create new one
  // 3. Update legend strip (key types for cross-uuid/scale, databases for cross-db)
  // 4. Update comparability warnings
  // 5. Update annotation
  destroyCharts();
  const panels = getPanelMetrics();
  panels.forEach((metric, i) => renderPanel(i, metric));
  updateLegend();
  updateComparabilityWarning();
  updateAnnotation();
}

function getPanelMetrics() {
  const db = filterState.database;
  if (db === 'cassandra') return EXPLORER_PANELS.cassandra;
  return EXPLORER_PANELS.default;
}

function renderPanel(index, metric) {
  const panel = dom.panels[index];
  const label = panel.querySelector('.panel-label');
  const unit = panel.querySelector('.panel-unit');
  const canvas = panel.querySelector('canvas');
  const config = PANEL_CONFIG[metric];

  label.textContent = config ? config.label : metric.toUpperCase();
  unit.textContent = config ? config.unit : '';

  let chartConfig;
  if (activeMode === 'cross-uuid') {
    const entries = getEntriesForMetric('cross-uuid', metric);
    chartConfig = buildCrossUUIDChart(entries, metric);
  } else if (activeMode === 'cross-db') {
    chartConfig = buildCrossDBChart(allEntries, filterState, metric);
  } else if (activeMode === 'scale') {
    const entries = getEntriesForMetric('scale', metric);
    chartConfig = buildScaleChart(entries, metric);
  }

  if (chartConfig) {
    chartInstances[index] = new Chart(canvas, {
      ...chartConfig,
      plugins: [errorBarPlugin],
    });
    panel.classList.remove('panel-empty');
  } else {
    // Show N/A state
    panel.classList.add('panel-empty');
  }
}

function updateLegend() {
  // Cross-UUID & Scale: show key type colors
  // Cross-DB: show database colors
}

function updateComparabilityWarning() {
  // Check if any of the 4 panel metrics have comparability notes
  // Show warning if so (especially for cross-db mode)
}

function updateAnnotation() {
  // Get annotation for current filter state + activeMode
  // Update annotation section or hide if no annotation
  // Update progress counter
}

function destroyCharts() {
  chartInstances.forEach((c, i) => {
    if (c) { c.destroy(); chartInstances[i] = null; }
  });
}

export function destroyExplorer() {
  destroyCharts();
}
```

**Step 2: Verify**

Navigate to Explorer — should see 4 charts in a 2×2 grid. Changing database dropdown should update all 4 charts. Sub-tabs should switch between Cross-UUID/Cross-DB/Scale. Panel expand should work. Annotations should appear below charts.

**Step 3: Commit**

```
feat(docs): implement Explorer view with 4-panel chart grid
```

---

## Task 9: Create `rawdata.js` — Raw Data Table

**Files:**
- Create: `docs/assets/rawdata.js`

**What:** Extract and adapt the raw data table from current `app.js` (lines 1436-1585). Update styling for monochrome aesthetic.

**Step 1: Write `rawdata.js`**

```javascript
import { KEY_TYPE_ORDER, LOWER_IS_BETTER,
         formatKeyTypeName, formatMetricName, formatNumber } from './constants.js';
import { allEntries, filterState, getFilteredEntries } from './data.js';

// Same column definitions as current app.js lines 1440-1449
const TABLE_COLUMNS = [ ... ];

let tableSortState = { column: 'keyType', direction: 'asc' };

export function initRawData() {
  bindRawFilters();
  renderRawData();
}

function bindRawFilters() {
  // Bind raw-filter-* dropdowns with cascading logic
  // These dropdowns have "All" as first option
}

export function renderRawData() {
  const entries = getFilteredEntries('raw-data');
  // ... (adapted from current app.js lines 1451-1585)
  // Key changes:
  // - Conditional formatting: monochrome (bold for best, muted for worst) instead of colored bg
  // - Use .cell-best { font-weight: 700; color: #333333; } and .cell-worst { color: #888888; }
  // - Update row count display
}

export function destroyRawData() {
  // Clean up event listeners if needed
}
```

**Step 2: Verify**

Navigate to Raw Data tab — should see sortable table with monochrome conditional formatting.

**Step 3: Commit**

```
feat(docs): implement Raw Data view with monochrome table
```

---

## Task 10: Create `app.js` — Entry Point & Router

**Files:**
- Create: `docs/assets/app.js` (replaces existing entirely)

**What:** Application entry point. Handles data loading, view switching, URL hash state management, and initialization.

**Step 1: Write `app.js`**

```javascript
import { loadData } from './data.js';
import { initSummary, destroySummary } from './summary.js';
import { initExplorer, destroyExplorer } from './explorer.js';
import { initRawData, destroyRawData } from './rawdata.js';

let activeView = 'summary';

document.addEventListener('DOMContentLoaded', async () => {
  try {
    await loadData();
    parseURLHash();
    initActiveView();
    bindMainNav();
    bindHashChange();
  } catch (err) {
    console.error('Failed to load data:', err);
    // Show error state
  }
});

function bindMainNav() {
  document.querySelectorAll('.nav-tab').forEach(btn => {
    btn.addEventListener('click', () => {
      const view = btn.dataset.view;
      if (view === activeView) return;
      switchView(view);
    });
  });
}

function switchView(view, initialFilters) {
  // 1. Destroy current view
  // 2. Update active tab styling
  // 3. Hide all view-panels, show the target one
  // 4. Init new view
  // 5. Update URL hash
  destroyCurrentView();

  document.querySelectorAll('.nav-tab').forEach(btn => {
    const isActive = btn.dataset.view === view;
    btn.classList.toggle('active', isActive);
    btn.setAttribute('aria-selected', String(isActive));
    // Add ▸ marker to active tab text
  });

  document.querySelectorAll('.view-panel').forEach(panel => {
    panel.hidden = panel.id !== `view-${view}`;
  });

  activeView = view;
  initActiveView(initialFilters);
  updateURLHash();
}

function initActiveView(initialFilters) {
  switch (activeView) {
    case 'summary': initSummary(); break;
    case 'explorer': initExplorer(initialFilters); break;
    case 'raw-data': initRawData(); break;
  }
}

function destroyCurrentView() {
  switch (activeView) {
    case 'summary': destroySummary(); break;
    case 'explorer': destroyExplorer(); break;
    case 'raw-data': destroyRawData(); break;
  }
}

// --- URL Hash State ---

function parseURLHash() {
  // Parse: #view=explorer&mode=cross-uuid&db=postgres&scenario=insert_performance&scale=1m&conn=1
  // Set activeView and filterState accordingly
  // If no hash, default to summary
}

function updateURLHash() {
  // Build hash from current view + filterState
  // Use history.replaceState to avoid polluting browser history
}

function bindHashChange() {
  window.addEventListener('hashchange', () => {
    parseURLHash();
    switchView(activeView);
  });
}

// --- Public API for cross-view navigation ---
// (Called by summary.js when user clicks "explore" buttons)
window.navigateToExplorer = function(filters) {
  switchView('explorer', filters);
};
```

**Step 2: Verify**

Full end-to-end test:
1. Open `docs/index.html` — Summary view loads with KPI cards
2. Click "EXPLORER" tab — switches to 4-panel chart grid
3. Change filters — charts update
4. Click sub-tabs — Cross-UUID/Cross-DB/Scale switch
5. Click "RAW DATA" tab — table loads
6. Copy URL hash, open in new tab — same view loads
7. Back button works

**Step 3: Commit**

```
feat(docs): implement app entry point with routing and URL state
```

---

## Task 11: Integration Testing & Polish

**Files:**
- Modify: All files as needed for bug fixes

**Step 1: Cross-browser testing**

Test in Chrome, Firefox, Safari:
- [ ] All 3 views render correctly
- [ ] Filter cascading works (no "no results" states from invalid combos)
- [ ] 4-panel chart grid renders with correct data
- [ ] Panel expand/collapse works
- [ ] Sub-tab switching (Cross-UUID/Cross-DB/Scale) works
- [ ] Annotations display for known filter combos
- [ ] Next/prev finding navigation works
- [ ] URL hash state persists across page reload
- [ ] Raw data table sorts correctly
- [ ] Comparability warnings show for non-comparable metrics

**Step 2: Responsive testing**

- [ ] Mobile (<768px): single column charts, stacked filters, 2-panel default + toggle
- [ ] Tablet (768-1024px): 2×2 grid, wrapped filters
- [ ] Desktop (>1024px): full layout

**Step 3: Accessibility audit**

- [ ] Tab through all interactive elements — logical order
- [ ] Screen reader reads tab roles correctly
- [ ] `prefers-reduced-motion: reduce` disables animations
- [ ] Keyboard: Enter/Space activates buttons, Escape closes popovers, arrows navigate tabs

**Step 4: Performance check**

- [ ] Data loads fast (single fetch, ~1.6MB JSON)
- [ ] Chart rendering is smooth (4 charts at once)
- [ ] No memory leaks from chart instances (destroyed on view switch)
- [ ] ES module loading doesn't cause FOUC

**Step 5: Final commit**

```
fix(docs): integration testing and polish
```

---

## Execution Notes

- **Read the design document first:** `docs/plans/2026-03-04-dashboard-redesign-design.md` has the full visual spec
- **Use `frontend-design` skill** when implementing — it ensures the monochrome terminal aesthetic is executed with precision (Courier New, 1px borders, no radius, no shadows, uppercase letter-spacing labels)
- **Keep data files unchanged** — `data.json` and `annotations.json` are the source of truth
- **Test incrementally** — after each task, open the browser and verify
- **The user handles all git operations** — do NOT run git commands
- **Current `app.js` is the reference** for all business logic (filter cascading, chart building, annotation lookup) — the logic is correct, it just needs to be modularized and restyled
