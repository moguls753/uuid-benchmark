# GitHub Pages Dashboard Design

## Overview

Interactive static dashboard for exploring UUID benchmark results across 4 databases (PostgreSQL, MySQL, MongoDB, Cassandra). Served via GitHub Pages from `docs/` on `main` branch.

## Data Pipeline

**Conversion script:** `scripts/convert_results.py` (Python 3, stdlib only)
- Auto-discovers `results/*.csv` (excludes `*_raw.csv`)
- Parses filename convention: `{database}_{scale}_{scenario}_{connections}conn.csv`
- Handles `_all_` files containing multiple scenarios
- Outputs `docs/data/data.json`
- Idempotent — re-run after adding new CSVs, overwrites output
- Filters out `-1` metric values (non-applicable metrics)

**JSON structure:**
```json
{
  "entries": [
    {
      "database": "postgres",
      "scale": "1m",
      "scenario": "insert_performance",
      "connections": 1,
      "keyType": "UUIDV4",
      "metric": "throughput",
      "median": 33421.62,
      "mean": 33427.45,
      "stddev": 348.73,
      "min": 33063.00,
      "max": 33935.45,
      "cv": 1.04
    }
  ],
  "metadata": {
    "databases": [...],
    "scales": [...],
    "scenarios": [...],
    "keyTypes": [...],
    "metrics": [...],
    "generated": "ISO timestamp"
  }
}
```

## Architecture

Single-page app, Approach A (tab navigation).

**Technology:**
- Chart.js 4.x from CDN (cdn.jsdelivr.net)
- Vanilla JS, no framework, no build step
- Mobile-first CSS, no CSS framework
- Python 3 (stdlib only) for conversion script

**File structure:**
```
docs/
├── index.html
├── data/
│   └── data.json
└── assets/
    ├── app.js
    └── style.css

scripts/
└── convert_results.py
```

## UI Layout

### Tabs (4 views)

1. **Cross-UUID** — Bar chart comparing all key types for one database
2. **Cross-DB** — Bar chart comparing applicable databases for one key type
3. **Scale Effect** — Line chart showing metric across 100k/1M/10M
4. **Raw Data** — Sortable table with all metric values

### Filter bar per tab

| Tab | Filters |
|-----|---------|
| Cross-UUID | Database, Scenario, Scale, Connections, Metric |
| Cross-DB | Key Type, Scenario, Scale, Connections, Metric |
| Scale | Database, Scenario, Connections, Metric |
| Raw Data | Database, Scenario, Scale, Connections |

### Cascading filter logic

Dropdowns only show options that exist in the data for the current selection. Changing one filter updates the available options in all other filters. Prevents empty chart states.

## Chart Specifications

### Cross-UUID (bar chart)
- X-axis: 6 key types
- Y-axis: selected metric
- Stddev error bars (median +/- stddev)
- Auto-generated title from current filters

### Cross-DB (bar chart)
- X-axis: applicable databases only
- Y-axis: selected metric
- Stddev error bars
- Shows only databases that have data for the selected metric

### Scale Effect (line chart)
- X-axis: scale points (100k, 1M, 10M), logarithmic
- Y-axis: selected metric
- One line per key type, color-coded with legend
- Missing points: line connects available points; single point shows as dot

### Raw Data (table)
- Columns: Key Type, Metric, Median, Mean, StdDev, CV%, Min, Max
- Sortable by tapping column headers
- Mobile: horizontally scrollable with sticky Key Type column

## Visual Design

Scientific/academic aesthetic:
- High contrast, no gradients
- Muted color palette
- Clear labels, gridlines for readability
- Consistent colors per key type across all views
- Sequential key type visually distinct as baseline (muted/grey)

Exact colors and styling handled by `frontend-design` skill during implementation.

## Mobile-First Constraints

- Touch targets >= 44px (tabs, dropdowns, table headers)
- Primary action immediately visible without scrolling
- Clear vertical hierarchy: Title -> Tabs -> Filters -> Chart
- No horizontal scrolling (charts resize; bars go horizontal if needed on small screens)
- Text >= 14px body, >= 18px headings
- No hover-only affordances (tooltips on tap)
- Filters collapse to expandable section on mobile
- Raw data table: sticky first column or card layout

## Edge Cases

- **No data:** Text message "No data available for this combination"
- **Partial scale data:** Line chart connects available points only
- **Missing metrics:** `-1` values filtered out at conversion time
- **Database-specific metrics:** Only shown when applicable databases are selected
- **No URL state:** Filters reset on page reload

## End-to-End Flow

1. Run benchmarks -> CSVs in `results/`
2. `python scripts/convert_results.py` -> `docs/data/data.json`
3. Commit and push
4. GitHub Pages auto-deploys from `docs/` on `main`
5. Browser loads `data.json` via `fetch()`, renders charts

## Implementation Notes

- `frontend-design` skill is mandatory during HTML/CSS/JS implementation
- Conversion script must be implemented and tested before the frontend
