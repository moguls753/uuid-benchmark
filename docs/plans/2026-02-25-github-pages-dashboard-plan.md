# GitHub Pages Dashboard Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Use frontend-design skill for Tasks 3-8.

**Goal:** Build an interactive static dashboard for exploring UUID benchmark results across 4 databases, served via GitHub Pages.

**Architecture:** Single-page app with 4 tab views (Cross-UUID, Cross-DB, Scale Effect, Raw Data). Python script converts `results/*.csv` into `docs/data/data.json`. Vanilla JS + Chart.js 4.x from CDN renders charts client-side. Mobile-first CSS.

**Tech Stack:** Python 3 (stdlib), Chart.js 4.x (CDN), vanilla JS, CSS

---

### Task 1: Conversion Script

**Files:**
- Create: `scripts/convert_results.py`
- Read: `results/*.csv` (all non-raw summary CSVs)
- Create: `docs/data/data.json`

**Step 1: Write the conversion script**

The script must:
- Scan `results/*.csv`, skip `*_raw.csv` files
- Parse filename with regex: `{database}_{scale}_{scenario}_{connections}conn.csv`
  - Database: `postgres`, `mysql`, `mongodb`, `cassandra`
  - Scale: `100k`, `1m`, `10m` (from RecordCount column: 100000→100k, 1000000→1m, 10000000→10m)
  - Scenario: from filename OR from CSV `Scenario` column (for `_all_` files)
  - Connections: integer before `conn`
- Handle special filenames like `postgres_1m_insert-performance_batch1_8conn.csv` (extra segments)
- Handle `_all_` files that contain multiple scenarios in one CSV
- Read each CSV row: `Scenario,KeyType,Metric,Median,Mean,StdDev,Min,Max,CV_Percent,RecordCount,Connections`
- Skip rows where Median == -1 (non-applicable metrics)
- Build entries list and auto-derive metadata (unique databases, scales, scenarios, keyTypes, metrics)
- Sort metadata arrays for consistent dropdown order
- Write `docs/data/data.json` with entries + metadata + generated timestamp

```python
#!/usr/bin/env python3
"""Convert results/*.csv to docs/data/data.json for the GitHub Pages dashboard."""

import csv
import json
import os
import re
from datetime import datetime, timezone

RESULTS_DIR = os.path.join(os.path.dirname(__file__), '..', 'results')
OUTPUT_DIR = os.path.join(os.path.dirname(__file__), '..', 'docs', 'data')
OUTPUT_FILE = os.path.join(OUTPUT_DIR, 'data.json')

SCALE_MAP = {
    '100000': '100k',
    '1000000': '1m',
    '10000000': '10m',
}

SCALE_ORDER = ['100k', '1m', '10m']
KEY_TYPE_ORDER = ['SEQUENTIAL', 'UUIDV1', 'UUIDV4', 'UUIDV7', 'ULID', 'ULID_MONOTONIC']
SCENARIO_ORDER = [
    'insert_performance', 'read_performance', 'update_performance',
    'mixed_insert_heavy', 'mixed_read_update'
]
DATABASE_ORDER = ['postgres', 'mysql', 'mongodb', 'cassandra']


def parse_filename(filename):
    """Extract database and connections from filename. Scale/scenario come from CSV data."""
    base = filename.replace('.csv', '')
    # Match: {database}_{...}_{N}conn
    m = re.match(r'^(postgres|mysql|mongodb|cassandra)_(.+)_(\d+)conn$', base)
    if not m:
        return None
    return {
        'database': m.group(1),
        'connections': int(m.group(3)),
    }


def sorted_by_order(values, order):
    """Sort values according to a predefined order, unknown values at the end."""
    def key(v):
        try:
            return order.index(v)
        except ValueError:
            return len(order)
    return sorted(values, key=key)


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    entries = []
    all_databases = set()
    all_scales = set()
    all_scenarios = set()
    all_key_types = set()
    all_metrics = set()
    all_connections = set()

    csv_files = sorted(f for f in os.listdir(RESULTS_DIR)
                       if f.endswith('.csv') and '_raw' not in f)

    for csv_file in csv_files:
        parsed = parse_filename(csv_file)
        if not parsed:
            print(f"Skipping unrecognized filename: {csv_file}")
            continue

        filepath = os.path.join(RESULTS_DIR, csv_file)
        with open(filepath, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                median = float(row['Median'])
                if median == -1:
                    continue

                record_count = row['RecordCount'].split('.')[0]
                scale = SCALE_MAP.get(record_count)
                if not scale:
                    print(f"Unknown RecordCount {record_count} in {csv_file}")
                    continue

                entry = {
                    'database': parsed['database'],
                    'scale': scale,
                    'scenario': row['Scenario'],
                    'connections': int(row['Connections']),
                    'keyType': row['KeyType'],
                    'metric': row['Metric'],
                    'median': round(float(row['Median']), 2),
                    'mean': round(float(row['Mean']), 2),
                    'stddev': round(float(row['StdDev']), 2),
                    'min': round(float(row['Min']), 2),
                    'max': round(float(row['Max']), 2),
                    'cv': round(float(row['CV_Percent']), 2),
                }
                entries.append(entry)

                all_databases.add(entry['database'])
                all_scales.add(entry['scale'])
                all_scenarios.add(entry['scenario'])
                all_key_types.add(entry['keyType'])
                all_metrics.add(entry['metric'])
                all_connections.add(entry['connections'])

    data = {
        'entries': entries,
        'metadata': {
            'databases': sorted_by_order(list(all_databases), DATABASE_ORDER),
            'scales': sorted_by_order(list(all_scales), SCALE_ORDER),
            'scenarios': sorted_by_order(list(all_scenarios), SCENARIO_ORDER),
            'keyTypes': sorted_by_order(list(all_key_types), KEY_TYPE_ORDER),
            'metrics': sorted(list(all_metrics)),
            'connections': sorted(list(all_connections)),
            'generated': datetime.now(timezone.utc).isoformat(),
        },
    }

    with open(OUTPUT_FILE, 'w') as f:
        json.dump(data, f, indent=2)

    print(f"Wrote {len(entries)} entries to {OUTPUT_FILE}")
    print(f"Databases: {data['metadata']['databases']}")
    print(f"Scales: {data['metadata']['scales']}")
    print(f"Scenarios: {data['metadata']['scenarios']}")
    print(f"Key types: {data['metadata']['keyTypes']}")
    print(f"Metrics: {data['metadata']['metrics']}")
    print(f"Connections: {data['metadata']['connections']}")


if __name__ == '__main__':
    main()
```

**Step 2: Create output directory and run**

```bash
mkdir -p docs/data
python3 scripts/convert_results.py
```

Expected: Prints entry count, lists all discovered dimensions.

**Step 3: Verify output**

```bash
python3 -c "import json; d=json.load(open('docs/data/data.json')); print(len(d['entries']), 'entries'); print(json.dumps(d['metadata'], indent=2))"
```

Expected: Several hundred entries, all 4 databases listed, metrics include throughput/latency/page_splits etc.

**Step 4: Commit**

---

### Task 2: HTML Structure

**Files:**
- Create: `docs/index.html`

**Step 1: Write index.html**

Skeleton HTML with:
- Chart.js 4.x from CDN (`<script src="https://cdn.jsdelivr.net/npm/chart.js@4">`)
- 4 tab buttons: Cross-UUID, Cross-DB, Scale, Raw Data
- Filter bar container (dropdowns populated by JS)
- Chart canvas element
- Raw data table container (hidden by default)
- Links to `assets/style.css` and `assets/app.js`
- Mobile viewport meta tag
- Semantic HTML: `<header>`, `<nav>`, `<main>`, `<section>`

Use `frontend-design` skill for the actual HTML content and structure.

**Step 2: Verify**

Open `docs/index.html` in browser — should show tabs and empty filter area.

**Step 3: Commit**

---

### Task 3: CSS Styling

**Files:**
- Create: `docs/assets/style.css`

**Step 1: Write mobile-first CSS**

Use `frontend-design` skill. Requirements:
- Scientific/academic aesthetic: no gradients, muted palette, high contrast
- Mobile-first: base styles are mobile, `@media (min-width: 768px)` adds desktop layout
- Touch targets >= 44px for tabs, dropdowns, table headers
- Text >= 14px body, >= 18px headings
- Vertical stack layout for filters on mobile, horizontal row on desktop
- Tab bar with clear active state
- Chart container responsive (100% width, reasonable height)
- Table styling with alternating row colors, sticky first column on mobile
- "No data" message styling

**Step 2: Verify**

Open in browser at 375px width (mobile) and 1200px (desktop). Check touch target sizes, text readability, layout.

**Step 3: Commit**

---

### Task 4: JS — Data Loading & Filter Logic

**Files:**
- Create: `docs/assets/app.js`

**Step 1: Write data loading and core filter infrastructure**

```javascript
// Core responsibilities:
// 1. Fetch docs/data/data.json on page load
// 2. Populate dropdowns from metadata
// 3. Cascading filter logic: when one filter changes, update available
//    options in other dropdowns based on what data exists
// 4. Tab switching: show/hide correct filters per tab, trigger re-render
// 5. Export a function like getFilteredEntries() that other views call
```

Key functions:
- `loadData()` — fetch and parse data.json, store globally, init UI
- `populateDropdowns(tab)` — fill dropdowns with options relevant to current tab
- `updateAvailableOptions()` — cascading logic: filter entries by current selections, derive valid options for remaining dropdowns
- `onFilterChange()` — called on any dropdown change, updates cascading options, triggers chart render
- `onTabChange(tab)` — shows/hides relevant filters, triggers render
- `getFilteredEntries(filters)` — returns entries matching all active filters
- `formatMetricName(metric)` — converts `p50_latency_us` to `P50 Latency (μs)` etc.
- `formatScenarioName(scenario)` — converts `insert_performance` to `Insert Performance`

Use `frontend-design` skill for any UI interaction patterns.

**Step 2: Verify**

Open in browser, check console for data loaded message, verify dropdowns populate and cascade correctly.

**Step 3: Commit**

---

### Task 5: JS — Cross-UUID Chart View

**Files:**
- Modify: `docs/assets/app.js`

**Step 1: Implement Cross-UUID bar chart**

- Filter entries by selected database, scenario, scale, connections, metric
- X-axis: key types (SEQUENTIAL, UUIDv1, UUIDv4, UUIDv7, ULID, ULID_MONOTONIC)
- Y-axis: median value of selected metric
- Error bars: median ± stddev (use Chart.js error bar plugin or custom drawing)
- Consistent key type colors (scientific palette — muted, high contrast)
- Auto-generated chart title from current filters
- Responsive canvas

Key type color map (scientific palette — exact colors via frontend-design skill):
- SEQUENTIAL: grey (baseline)
- UUIDV1: muted warm
- UUIDV4: distinct color (the outlier readers look for)
- UUIDV7: cool tone
- ULID: adjacent to ULID_MONOTONIC
- ULID_MONOTONIC: adjacent to ULID

**Step 2: Verify**

Select PostgreSQL > insert_performance > 1m > 1 conn > throughput. Should see 6 bars. UUIDv4 should be notably lower throughput than SEQUENTIAL.

**Step 3: Commit**

---

### Task 6: JS — Cross-DB Chart View

**Files:**
- Modify: `docs/assets/app.js`

**Step 1: Implement Cross-DB bar chart**

- Filter entries by selected key type, scenario, scale, connections, metric
- X-axis: databases that have data for this metric (only applicable ones)
- Y-axis: median value
- Error bars: median ± stddev
- Same scientific color scheme but for databases (4 distinct colors)
- If a database doesn't have data for the selected combination, it's simply absent from the chart

**Step 2: Verify**

Select UUIDV4 > insert_performance > 1m > 1 conn > throughput. Should see bars for databases that have this data. MongoDB should show highest throughput.

**Step 3: Commit**

---

### Task 7: JS — Scale Effect Chart View

**Files:**
- Modify: `docs/assets/app.js`

**Step 1: Implement Scale Effect line chart**

- Filter entries by selected database, scenario, connections, metric
- X-axis: scale points (100k, 1M, 10M) — logarithmic scale
- Y-axis: median value of selected metric
- One line per key type, using same key type colors as Cross-UUID view
- Legend showing all key types
- Missing data points: line connects available points; single point = dot, no line
- Data points visible as circles on each line

**Step 2: Verify**

Select PostgreSQL > insert_performance > 1 conn > throughput. Should see 6 lines showing how throughput changes from 100k to 10M.

**Step 3: Commit**

---

### Task 8: JS — Raw Data Table View

**Files:**
- Modify: `docs/assets/app.js`

**Step 1: Implement sortable raw data table**

- Filter entries by selected database, scenario, scale, connections
- Show ALL metrics for the filtered set (no metric dropdown in this tab)
- Columns: Key Type, Metric, Median, Mean, StdDev, CV%, Min, Max
- Sortable: tap column header to sort asc/desc, visual indicator (▲/▼)
- Default sort: Key Type asc, then Metric asc
- Number formatting: round to 2 decimal places, add thousands separators for large values
- Mobile: table scrollable horizontally, Key Type column sticky on left
- Touch targets on sort headers >= 44px

Use `frontend-design` skill for table styling.

**Step 2: Verify**

Select PostgreSQL > insert_performance > 1m > 1 conn. Should see table with all metrics for all 6 key types. Tap "Median" header to sort.

**Step 3: Commit**

---

### Task 9: End-to-End Verification

**Step 1: Run conversion script fresh**

```bash
python3 scripts/convert_results.py
```

**Step 2: Serve locally and test**

```bash
cd docs && python3 -m http.server 8080
```

Open `http://localhost:8080` and verify:
- All 4 tabs work and switch correctly
- Filters cascade (selecting MySQL reduces available scales)
- Cross-UUID: bars render with error bars, title updates
- Cross-DB: only applicable databases shown
- Scale: lines connect available data points
- Raw Data: table sorts on column tap
- Mobile: resize to 375px width, verify stacked filters, readable text, 44px targets
- No console errors

**Step 3: Commit all remaining changes**
