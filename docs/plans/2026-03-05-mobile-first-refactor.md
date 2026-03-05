# Mobile-First Dashboard Refactor — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Also use frontend-design skill for all CSS/HTML work to maintain the monochrome terminal aesthetic.

**Goal:** Make the UUID Benchmark dashboard fully mobile-usable: proper touch targets, readable typography, collapsible filter drawer, card-based raw data view, consistent spacing scale.

**Architecture:** CSS-first approach. Most changes are in `style.css` (new custom properties, mobile media queries, `:active` states). HTML changes add filter drawer toggle/chips markup. JS changes add filter drawer logic in `explorer.js` and card rendering in `rawdata.js`.

**Tech Stack:** Vanilla HTML/CSS/JS (no framework). JetBrains Mono. Chart.js for charts.

**Design doc:** `docs/plans/2026-03-05-mobile-first-refactor-design.md`

---

### Task 1: Add Spacing Scale Custom Properties

**Files:**
- Modify: `docs/assets/style.css:8-42`

**Step 1: Add spacing scale tokens to `:root`**

In `docs/assets/style.css`, add after line 21 (`--touch-target: 44px;`):

```css
  /* Spacing scale (4px increments) */
  --sp-1: 4px;
  --sp-2: 8px;
  --sp-3: 12px;
  --sp-4: 16px;
  --sp-5: 20px;
  --sp-6: 24px;
  --sp-7: 32px;
  --sp-8: 40px;
```

**Step 2: Verify no regressions**

Open `docs/index.html` in browser. Confirm nothing has changed visually — this is additive only.

**Step 3: Commit**

```bash
git add docs/assets/style.css
git commit -m "feat(dashboard): add spacing scale custom properties"
```

---

### Task 2: Mobile Typography Overrides

**Files:**
- Modify: `docs/assets/style.css` (add new media query block)

**Step 1: Add mobile typography media query**

Add a new section after the existing `/* --- 11. Responsive: Small Mobile (<=400px) */` block (line ~1293), or insert a new mobile-first block. Place it just before the `@media (max-width: 400px)` block:

```css
/* --- 12. Responsive: Mobile (<768px) — Typography & Touch Targets -------- */

@media (max-width: 767px) {
  body {
    font-size: 13px;
  }

  /* Interactive UI text: 14px minimum for tappable elements */
  .nav-tab,
  .explorer-tab,
  .methodology-toggle,
  .db-explore,
  .show-all-metrics,
  .annotation-prev,
  .annotation-next,
  .filter-group label {
    font-size: 14px;
  }

  select {
    font-size: 14px;
  }

  /* Section labels */
  .section-label {
    font-size: 16px;
  }

  /* KPI context text */
  .kpi-context {
    font-size: 13px;
  }

  /* KPI label */
  .kpi-label {
    font-size: 13px;
  }

  /* Spark chart labels — minimum readable */
  .kpi-spark-labels span {
    font-size: 10px;
  }

  /* Legend strip */
  .legend-strip {
    font-size: 13px;
  }

  /* Annotation text */
  .annotation-finding {
    font-size: 14px;
  }

  .annotation-explanation {
    font-size: 13px;
  }

  /* Footer */
  .site-footer {
    font-size: 12px;
  }

  /* Methodology detail */
  .methodology-line {
    font-size: 13px;
  }

  .methodology-toggle {
    font-size: 13px;
  }

  .method-dl dt,
  .method-dl dd {
    font-size: 13px;
  }

  /* Panel metric select */
  .panel-metric-select {
    font-size: 13px;
  }

  .panel-unit {
    font-size: 13px;
  }

  /* DB card text */
  .db-name {
    font-size: 14px;
  }

  .db-arch,
  .db-tool {
    font-size: 13px;
  }
}
```

**Step 2: Verify in browser**

Open `docs/index.html` in Chrome DevTools responsive mode at 375px width. Confirm:
- Body text is 13px
- Tab labels, buttons, filter labels are 14px
- Section headings are 16px
- Nothing overflows horizontally

**Step 3: Commit**

```bash
git add docs/assets/style.css
git commit -m "feat(dashboard): add mobile typography overrides (13px body, 14px interactive)"
```

---

### Task 3: Mobile Touch Targets

**Files:**
- Modify: `docs/assets/style.css` (extend the mobile media query from Task 2)

**Step 1: Add touch target rules inside the `@media (max-width: 767px)` block**

Append to the media query added in Task 2:

```css
  /* Touch targets: 44px minimum height */
  .nav-tab {
    min-height: var(--touch-target);
    padding: var(--sp-3) var(--sp-4);
  }

  .explorer-tab {
    min-height: var(--touch-target);
    padding: var(--sp-3) var(--sp-4);
  }

  .methodology-toggle {
    min-height: var(--touch-target);
    padding: var(--sp-2) var(--sp-3);
  }

  .db-explore {
    min-height: var(--touch-target);
    padding: var(--sp-2) 0;
  }

  select {
    min-height: var(--touch-target);
    padding: var(--sp-2) var(--sp-3);
    padding-right: var(--sp-5);
  }

  .annotation-prev,
  .annotation-next {
    min-height: var(--touch-target);
    padding: var(--sp-2) var(--sp-3);
  }

  .annotation-progress {
    min-height: var(--touch-target);
    display: flex;
    align-items: center;
  }

  .panel-expand {
    min-height: var(--touch-target);
    min-width: var(--touch-target);
    display: flex;
    align-items: center;
    justify-content: center;
  }

  .panel-metric-select {
    min-height: var(--touch-target);
  }

  .show-all-metrics {
    min-height: var(--touch-target);
  }

  .chart-modal-close {
    min-height: var(--touch-target);
    padding: var(--sp-2) var(--sp-3);
  }

  .kpi-card {
    padding: var(--sp-4);
  }
```

**Step 2: Verify in browser**

At 375px width, use Chrome DevTools element inspector to verify all interactive elements are >= 44px in computed height.

**Step 3: Commit**

```bash
git add docs/assets/style.css
git commit -m "feat(dashboard): enforce 44px minimum touch targets on mobile"
```

---

### Task 4: Interaction States (`:active` and `:focus-visible`)

**Files:**
- Modify: `docs/assets/style.css` (modify existing hover rules)

**Step 1: Add `:active` alongside every `:hover` rule**

Find and update each hover rule. The pattern is to add `:active` as a comma-separated selector:

```css
/* KPI card — around line 367 */
.kpi-card:hover,
.kpi-card:active {
  background: var(--bg-dark);
}

/* DB explore — around line 483 */
.db-explore:hover,
.db-explore:active {
  color: var(--text);
}

/* Nav tab — around line 167 */
.nav-tab:hover,
.nav-tab:active {
  color: var(--text-mid);
}

/* Explorer tab — around line 569 */
.explorer-tab:hover,
.explorer-tab:active {
  color: var(--text-mid);
}

/* Methodology toggle — around line 262 */
.methodology-toggle:hover,
.methodology-toggle:active {
  color: var(--text);
}

/* Annotation prev/next — around line 900 */
.annotation-prev:hover,
.annotation-prev:active,
.annotation-next:hover,
.annotation-next:active {
  color: var(--text);
  background: var(--bg-off);
}

/* Panel expand — around line 669 */
.panel-expand:hover,
.panel-expand:active {
  color: var(--text-muted);
}

/* Panel metric select — around line 647 */
.panel-metric-select:hover,
.panel-metric-select:active {
  color: var(--text-mid);
}

/* Show all metrics — around line 818 */
.show-all-metrics:hover,
.show-all-metrics:active {
  color: var(--text);
  background: var(--bg-off);
}

/* Links — around line 100 */
a:hover,
a:active { color: var(--text); }

/* Footer link — around line 203 */
.footer-link:hover,
.footer-link:active {
  color: var(--text-muted);
}

/* Chart modal close — around line 755 */
.chart-modal-close:hover,
.chart-modal-close:active {
  color: var(--text);
  border-color: var(--border-strong);
}

/* Table row hover — around line 1077 */
.data-table tbody tr:hover,
.data-table tbody tr:active {
  background: var(--bg-dark);
}

/* Metric info button — around line 997 */
.metric-info-btn:hover,
.metric-info-btn:active {
  color: var(--text);
}
```

**Step 2: Verify tap feedback**

Open in mobile mode, tap KPI cards, tabs, buttons — confirm visual feedback on tap.

**Step 3: Commit**

```bash
git add docs/assets/style.css
git commit -m "feat(dashboard): add :active states for mobile tap feedback"
```

---

### Task 5: Summary View — DB Cards Single-Column Below 480px

**Files:**
- Modify: `docs/assets/style.css` (update the existing `@media (max-width: 400px)` block or add a new one)

**Step 1: Add 480px breakpoint for DB grid**

Add this new media query (can go near the existing 400px block around line 1295):

```css
@media (max-width: 480px) {
  .db-grid {
    grid-template-columns: 1fr;
  }
}
```

**Step 2: Verify**

At 375px width: DB cards stack vertically, one per row. At 500px: still 2-column.

**Step 3: Commit**

```bash
git add docs/assets/style.css
git commit -m "feat(dashboard): single-column DB cards below 480px"
```

---

### Task 6: Explorer Filter Drawer — HTML Markup

**Files:**
- Modify: `docs/index.html:177-198`

**Step 1: Add filter toggle button and chips container**

Replace the Explorer filter bar section (lines 177-198) with:

```html
    <!-- Filter bar -->
    <section class="filter-bar" aria-label="Filters">
      <button class="filter-toggle" aria-expanded="false" aria-controls="explorer-filter-drawer">
        <span class="filter-toggle-icon"></span>
        <span class="filter-toggle-label">FILTERS</span>
        <span class="filter-toggle-count"></span>
      </button>
      <div class="filter-chips" id="explorer-filter-chips"></div>
      <div class="filter-drawer" id="explorer-filter-drawer">
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
      </div>
    </section>
```

**Step 2: Add same pattern to Raw Data filter bar**

Replace lines 279-296 with:

```html
    <!-- Filter bar -->
    <section class="filter-bar raw-filter-bar" aria-label="Filters">
      <button class="filter-toggle raw-filter-toggle" aria-expanded="false" aria-controls="raw-filter-drawer">
        <span class="filter-toggle-icon"></span>
        <span class="filter-toggle-label">FILTERS</span>
        <span class="filter-toggle-count"></span>
      </button>
      <div class="filter-chips" id="raw-filter-chips"></div>
      <div class="filter-drawer" id="raw-filter-drawer">
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
      </div>
    </section>
```

**Step 3: Commit**

```bash
git add docs/index.html
git commit -m "feat(dashboard): add filter drawer toggle and chips markup"
```

---

### Task 7: Explorer Filter Drawer — CSS

**Files:**
- Modify: `docs/assets/style.css`

**Step 1: Add filter drawer styles**

Add after the existing `.filter-group label` rule (~line 544):

```css
/* Filter toggle (mobile only) */
.filter-toggle {
  display: none; /* Hidden on desktop */
  align-items: center;
  gap: var(--sp-2);
  width: 100%;
  min-height: var(--touch-target);
  padding: var(--sp-3) 0;
  font-size: 14px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 1.5px;
  color: var(--text-muted);
  border-bottom: 1px solid var(--border);
}

.filter-toggle-icon {
  display: inline-block;
  width: 4px;
  height: 6px;
  background: currentColor;
  -webkit-mask: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 4 6'%3E%3Cpath d='M0 0L4 3L0 6Z'/%3E%3C/svg%3E") no-repeat center / contain;
  mask: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 4 6'%3E%3Cpath d='M0 0L4 3L0 6Z'/%3E%3C/svg%3E") no-repeat center / contain;
  flex-shrink: 0;
  transition: transform 0.15s;
}

.filter-toggle[aria-expanded="true"] .filter-toggle-icon {
  transform: rotate(90deg);
}

.filter-toggle:hover,
.filter-toggle:active {
  color: var(--text);
}

.filter-toggle-count {
  font-weight: 500;
  color: var(--text-faint);
}

/* Filter chips (mobile only) */
.filter-chips {
  display: none; /* Hidden on desktop */
  font-size: 13px;
  color: var(--text-muted);
  padding: var(--sp-2) 0;
  line-height: 1.5;
}

.filter-chips:empty {
  display: none;
}

/* Filter drawer wrapper */
.filter-drawer {
  display: contents; /* On desktop, children flow normally in the flex parent */
}
```

**Step 2: Add mobile overrides inside the `@media (max-width: 767px)` block**

Append to the mobile media query from Task 2:

```css
  /* Filter drawer: toggle visible, drawer collapsed */
  .filter-toggle {
    display: flex;
  }

  .filter-chips {
    display: block;
  }

  .filter-drawer {
    display: none;
    flex-direction: column;
    gap: var(--sp-3);
    padding: var(--sp-3) 0;
  }

  .filter-drawer.open {
    display: flex;
  }

  .filter-drawer .filter-group {
    width: 100%;
  }

  .filter-drawer .filter-group select {
    width: 100%;
  }

  /* Hide the horizontal filter flex layout cues */
  .filter-bar {
    flex-direction: column;
    gap: 0;
  }
```

**Step 3: Verify**

- Desktop (1024px+): filter toggle hidden, selects display inline as before
- Mobile (375px): toggle button visible, filters hidden, chips show current selection

**Step 4: Commit**

```bash
git add docs/assets/style.css
git commit -m "feat(dashboard): add filter drawer CSS (collapsible on mobile)"
```

---

### Task 8: Explorer Filter Drawer — JavaScript

**Files:**
- Modify: `docs/assets/explorer.js`
- Reference: `docs/assets/constants.js` (for `formatDatabaseName`, `formatScenarioName`)

**Step 1: Add filter drawer DOM caching and logic**

In `explorer.js`, update the `cacheDom()` function (around line 70) to add:

```javascript
  dom.filterToggle = document.querySelector('#view-explorer .filter-toggle');
  dom.filterChips = document.getElementById('explorer-filter-chips');
  dom.filterDrawer = document.getElementById('explorer-filter-drawer');
```

**Step 2: Add `bindFilterDrawer()` function**

Add after the `bindFilterEvents()` function:

```javascript
function bindFilterDrawer() {
  if (!dom.filterToggle || !dom.filterDrawer) return;
  dom.filterToggle.addEventListener('click', () => {
    const isOpen = dom.filterDrawer.classList.toggle('open');
    dom.filterToggle.setAttribute('aria-expanded', String(isOpen));
  });
}
```

**Step 3: Add `updateFilterChips()` function**

Add after `bindFilterDrawer`:

```javascript
function updateFilterChips() {
  if (!dom.filterChips) return;
  const visible = VIEW_FILTERS[activeMode] || [];
  const parts = [];

  visible.forEach(key => {
    const val = filterState[key];
    if (val == null) return;
    switch (key) {
      case 'database': parts.push(formatDatabaseName(val)); break;
      case 'keyType': parts.push(formatKeyTypeName(val)); break;
      case 'scenario': parts.push(formatScenarioName(val.replace(/_/g, ' '))); break;
      case 'scale': parts.push(String(val).toUpperCase()); break;
      case 'connections': parts.push(val + ' conn'); break;
    }
  });

  dom.filterChips.textContent = parts.join(' \u00b7 ');

  // Update count on toggle button
  const countEl = dom.filterToggle?.querySelector('.filter-toggle-count');
  if (countEl) {
    countEl.textContent = parts.length > 0 ? `(${parts.length})` : '';
  }
}
```

**Step 4: Wire into `initExplorer` and `renderExplorer`**

In `initExplorer()`, add `bindFilterDrawer();` after `bindFilterEvents();` (around line 57).

In `renderExplorer()`, add `updateFilterChips();` after `updateAnnotation();` (around line 383).

Also import `formatScenarioName` in the imports at the top of `explorer.js`:

```javascript
import {
  KEY_TYPE_COLORS, KEY_TYPE_ORDER, KEY_TYPE_SHORT, KEY_TYPE_LABELS,
  DATABASE_COLORS, DATABASE_ORDER, DATABASE_LABELS,
  EXPLORER_PANELS, PANEL_CONFIG, METRIC_GROUPS, METRIC_INFO, VIEW_FILTERS,
  formatKeyTypeName, formatDatabaseName, formatScenarioName,
} from './constants.js';
```

**Step 5: Verify**

- Mobile: toggle opens/closes drawer, chips show current filter values
- Desktop: toggle hidden, filters display normally

**Step 6: Commit**

```bash
git add docs/assets/explorer.js
git commit -m "feat(dashboard): add filter drawer toggle and chip logic for explorer"
```

---

### Task 9: Raw Data Filter Drawer — JavaScript

**Files:**
- Modify: `docs/assets/rawdata.js`

**Step 1: Add DOM caching for raw filter drawer**

In `cacheRawDom()` (around line 44), add:

```javascript
  rawDom.filterToggle = document.querySelector('#view-raw-data .raw-filter-toggle');
  rawDom.filterChips = document.getElementById('raw-filter-chips');
  rawDom.filterDrawer = document.getElementById('raw-filter-drawer');
```

**Step 2: Add `bindRawFilterDrawer()` function**

```javascript
function bindRawFilterDrawer() {
  if (!rawDom.filterToggle || !rawDom.filterDrawer) return;
  rawDom.filterToggle.addEventListener('click', () => {
    const isOpen = rawDom.filterDrawer.classList.toggle('open');
    rawDom.filterToggle.setAttribute('aria-expanded', String(isOpen));
  });
}
```

**Step 3: Add `updateRawFilterChips()` function**

```javascript
function updateRawFilterChips() {
  if (!rawDom.filterChips) return;
  const parts = [];

  RAW_FILTERS.forEach(key => {
    const val = rawFilterState[key];
    if (val == null) return;
    parts.push(formatRawOption(key, val));
  });

  rawDom.filterChips.textContent = parts.join(' \u00b7 ');

  const countEl = rawDom.filterToggle?.querySelector('.filter-toggle-count');
  if (countEl) {
    countEl.textContent = parts.length > 0 ? `(${parts.length})` : '';
  }
}
```

**Step 4: Wire in**

In `initRawData()`, add `bindRawFilterDrawer();` after `bindRawFilters();`.

In `renderRawData()`, add `updateRawFilterChips();` as the last line of the function (after the footer update).

**Step 5: Commit**

```bash
git add docs/assets/rawdata.js
git commit -m "feat(dashboard): add filter drawer toggle and chips for raw data view"
```

---

### Task 10: Raw Data Card Layout — CSS

**Files:**
- Modify: `docs/assets/style.css`

**Step 1: Add card layout styles**

Add after the existing `.table-footer` rule (~line 1098):

```css
/* --- Raw Data Card Layout (Mobile) --------------------------------------- */

.raw-data-cards {
  display: none; /* Hidden on desktop */
}

.raw-card {
  border: 1px solid var(--border);
  margin-top: -1px; /* Collapse borders */
}

.raw-card-header {
  display: flex;
  align-items: center;
  gap: var(--sp-2);
  padding: var(--sp-3) var(--sp-4);
  background: var(--bg-off);
  font-size: 14px;
  font-weight: 700;
  border-bottom: 1px solid var(--border);
}

.raw-card-swatch {
  display: inline-block;
  width: 10px;
  height: 10px;
  flex-shrink: 0;
}

.raw-card-body {
  padding: 0;
}

.raw-card-row {
  display: flex;
  justify-content: space-between;
  align-items: baseline;
  padding: var(--sp-2) var(--sp-4);
  font-size: 13px;
  min-height: var(--touch-target);
  border-bottom: 1px solid var(--bg-dark);
}

.raw-card-row:last-child {
  border-bottom: none;
}

.raw-card-label {
  color: var(--text-muted);
}

.raw-card-value {
  text-align: right;
  font-variant-numeric: tabular-nums;
}

.raw-card-value.cell-best {
  font-weight: 700;
  color: var(--border-strong);
}

.raw-card-value.cell-worst {
  color: var(--text-muted);
}
```

**Step 2: Add mobile overrides**

Inside the `@media (max-width: 767px)` block:

```css
  /* Raw data: cards instead of table */
  .table-scroll {
    display: none;
  }

  .table-footer {
    display: none;
  }

  .raw-data-cards {
    display: block;
  }
```

**Step 3: Commit**

```bash
git add docs/assets/style.css
git commit -m "feat(dashboard): add raw data card layout CSS for mobile"
```

---

### Task 11: Raw Data Card Layout — HTML Container

**Files:**
- Modify: `docs/index.html`

**Step 1: Add cards container after the table**

After the `</div>` closing `.table-scroll` (around line 304), and before `<div class="table-footer">` (line 305), insert:

```html
    <!-- Mobile card layout -->
    <div class="raw-data-cards" id="raw-data-cards"></div>
```

**Step 2: Commit**

```bash
git add docs/index.html
git commit -m "feat(dashboard): add raw data cards container element"
```

---

### Task 12: Raw Data Card Layout — JavaScript Rendering

**Files:**
- Modify: `docs/assets/rawdata.js`
- Reference: `docs/assets/constants.js` (for `KEY_TYPE_COLORS`)

**Step 1: Add import for KEY_TYPE_COLORS**

Update the import at the top of `rawdata.js`:

```javascript
import {
  KEY_TYPE_ORDER, KEY_TYPE_COLORS, LOWER_IS_BETTER,
  formatKeyTypeName, formatMetricName, formatNumber,
  formatDatabaseName, formatScenarioName,
} from './constants.js';
```

**Step 2: Add DOM caching for cards container**

In `cacheRawDom()`, add:

```javascript
  rawDom.cardsContainer = document.getElementById('raw-data-cards');
```

**Step 3: Add `renderRawCards()` function**

Add after `renderRawData()`:

```javascript
function renderRawCards(sorted, metricBestWorst) {
  if (!rawDom.cardsContainer) return;
  rawDom.cardsContainer.innerHTML = '';

  // Group entries by keyType + database + scenario + scale
  const groups = new Map();
  sorted.forEach(e => {
    const groupKey = `${e.keyType}|${e.database}|${e.scenario}|${e.scale}`;
    if (!groups.has(groupKey)) {
      groups.set(groupKey, { keyType: e.keyType, database: e.database, scenario: e.scenario, scale: e.scale, metrics: [] });
    }
    groups.get(groupKey).metrics.push(e);
  });

  groups.forEach(group => {
    const card = document.createElement('div');
    card.className = 'raw-card';

    // Header
    const header = document.createElement('div');
    header.className = 'raw-card-header';

    const swatch = document.createElement('span');
    swatch.className = 'raw-card-swatch';
    swatch.style.background = KEY_TYPE_COLORS[group.keyType] || '#999';
    header.appendChild(swatch);

    const title = document.createTextNode(
      formatKeyTypeName(group.keyType) +
      ' \u00b7 ' + formatDatabaseName(group.database) +
      ' \u00b7 ' + formatScenarioName(group.scenario) +
      ' \u00b7 ' + String(group.scale).toUpperCase()
    );
    header.appendChild(title);
    card.appendChild(header);

    // Body
    const body = document.createElement('div');
    body.className = 'raw-card-body';

    group.metrics.forEach(e => {
      const row = document.createElement('div');
      row.className = 'raw-card-row';

      const label = document.createElement('span');
      label.className = 'raw-card-label';
      label.textContent = formatMetricName(e.metric);
      row.appendChild(label);

      const value = document.createElement('span');
      value.className = 'raw-card-value';
      value.textContent = formatNumber(e.median);

      // Conditional formatting
      if (e.median != null && metricBestWorst[e.metric]) {
        const mw = metricBestWorst[e.metric];
        if (mw.best !== mw.worst) {
          if (e.median === mw.best) value.classList.add('cell-best');
          if (e.median === mw.worst) value.classList.add('cell-worst');
        }
      }

      row.appendChild(value);
      body.appendChild(row);
    });

    card.appendChild(body);
    rawDom.cardsContainer.appendChild(card);
  });
}
```

**Step 4: Call `renderRawCards` from `renderRawData`**

In `renderRawData()`, after the table rows are built (after the `sorted.forEach` loop, around line 305) and before the footer update, add:

```javascript
  // Render mobile card layout
  renderRawCards(sorted, metricBestWorst);
```

**Step 5: Verify**

- Mobile (375px): table hidden, cards visible with grouped metrics
- Desktop: cards hidden, table visible

**Step 6: Commit**

```bash
git add docs/assets/rawdata.js
git commit -m "feat(dashboard): render raw data as cards on mobile"
```

---

### Task 13: Final Verification & Cleanup

**Files:**
- All modified files

**Step 1: Full mobile audit at 375px**

Open in Chrome DevTools responsive mode at 375px width. Walk through:

1. **Summary view:** KPI cards 2-column, DB cards 1-column, all text readable, tap feedback works on KPI cards and explore buttons
2. **Explorer view:** Filter toggle visible, chips show current selection, drawer opens/closes, charts 1-column, "Show all metrics" for panels 3-4
3. **Raw Data view:** Filter toggle visible, cards layout with grouped metrics, best/worst formatting

**Step 2: Full desktop audit at 1440px**

Verify nothing has changed on desktop:

1. Filter toggle hidden, inline selects visible
2. Cards container hidden, table visible
3. Font sizes unchanged

**Step 3: Test at 768px (tablet breakpoint)**

Verify the transition point works cleanly — filter toggle hides, inline layout takes over, table appears.

**Step 4: Check for horizontal overflow**

At 320px width (smallest common phone), verify no horizontal scrollbar appears on any view.

**Step 5: Commit any fixes found during verification**

```bash
git add -A
git commit -m "fix(dashboard): address mobile audit findings"
```

---

## File Change Summary

| File | Type | Tasks |
|------|------|-------|
| `docs/assets/style.css` | Modify | 1, 2, 3, 4, 5, 7, 10 |
| `docs/index.html` | Modify | 6, 11 |
| `docs/assets/explorer.js` | Modify | 8 |
| `docs/assets/rawdata.js` | Modify | 9, 12 |

No new files created. No files deleted. `app.js`, `data.js`, `constants.js`, `charts.js`, `annotations.js`, `summary.js` are untouched.

---

## Testing Strategy

This is a static HTML dashboard — no unit test framework. Testing is manual browser verification:

1. **Chrome DevTools responsive mode** — primary testing at 320px, 375px, 768px, 1024px, 1440px
2. **Touch simulation** — Chrome DevTools "toggle device toolbar" enables tap simulation
3. **Lighthouse audit** — run Lighthouse accessibility check to verify touch target compliance
4. **Real device** — if available, verify on an actual phone for font readability confirmation
