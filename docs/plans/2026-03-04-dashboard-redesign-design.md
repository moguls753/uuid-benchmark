# UUID Benchmark Dashboard — Redesign Design Document

**Date:** 2026-03-04
**Author:** Eike Rackwitz
**Status:** Approved
**Implementation skill:** `frontend-design` (must be invoked before writing any code)

---

## 1. Context & Purpose

The UUID Benchmark dashboard (`docs/`) is a static GitHub Pages site that serves as an interactive companion to a benchmark study evaluating UUID performance (UUIDv1, UUIDv4, UUIDv7, ULID, ULID-monotonic) vs sequential integer keys across PostgreSQL, MySQL, MongoDB, and Cassandra.

### Current State

- **Tech:** Vanilla JS + Chart.js, single `app.js` (1,585 lines), single `style.css` (711 lines), no build step
- **Design:** Academic journal aesthetic (Crimson Pro serif, DM Sans, warm parchment palette)
- **UX:** 4 tabs (Cross-UUID, Cross-DB, Scale, Raw Data), one chart at a time, 5-6 filter dropdowns, annotations below chart
- **Data:** `data.json` (63,729 lines of benchmark results), `annotations.json` (curated findings)

### Problems

1. **One chart at a time** — Cannot correlate throughput with page splits or fragmentation without re-filtering
2. **Filter fatigue** — 5-6 dropdowns per comparison, high cognitive load
3. **No summary landing** — Drops users into a single chart with no context
4. **No guided narrative** — Annotations exist but no way to walk through key findings
5. **Academic aesthetic doesn't match content** — This is a Go-programmed systems benchmark measuring cgroup v2 metrics and B-tree page splits; the serif/parchment design signals "literary" not "technical"

### Goals

1. **Multi-panel metric view** — See 4 metrics simultaneously for the same filter context
2. **Narrative-first landing** — Lead with key findings before exploration
3. **Terminal/monospace aesthetic** — Signal "precision instrument for data" not "journal article"
4. **Guided findings navigation** — Walk through curated annotations in logical order
5. **Keep zero-build-step vanilla JS** — No React, no Vite, no Tailwind. ES modules only.

---

## 2. Design Direction

### Aesthetic: "Research Terminal"

Inspired by the [ATE Observatory](https://github.com/florianbuetow/agent-economy) Bloomberg-terminal-style dashboard. The key insight: **the wireframe IS the product**. Monochrome base where the only color comes from the data itself.

**What makes it memorable:** Against a pure monochrome UI, the key type colors (UUIDv4 blue, UUIDv7 green, UUIDv1 red) explode off the page. The data becomes the only visual emphasis. Combined with dense monospace typography and 1px borders, it reads as a precision instrument.

**Tone:** CERN data monitor meets Tufte. Not Bloomberg dark-mode cosplay, not SaaS dashboard. A serious, utilitarian tool built for examining benchmark data.

---

## 3. Typography

### Font Choice: `Courier New, Courier, monospace`

One system font family, zero external font loading. Raw, deliberate, anti-polish. Matches the terminal/instrument aesthetic.

**Critical readability constraint:** Minimum font size is **10px** (not 8-9px as in the observatory reference). This is a scientific data site where users must accurately compare numbers like `33,422` vs `34,993` vs `37,891`. Courier New has poor glyph differentiation below 10px.

### Type Scale

| Role | Size | Weight | Treatment |
|------|------|--------|-----------|
| Hero KPI numbers | 24-28px | 700 | `font-variant-numeric: tabular-nums` |
| Section headers | 13px | 700 | `uppercase`, `letter-spacing: 2.5px` |
| Navigation tabs | 11px | 500 | `uppercase`, `letter-spacing: 1.5px` |
| Filter labels | 10px | 500 | `uppercase`, `letter-spacing: 1px` |
| Chart panel labels | 10px | 700 | `uppercase`, `letter-spacing: 2px` |
| Chart panel unit | 10px | 400 | `color: var(--text-muted)` |
| Table data, chart axes | 11px | 400 | Normal |
| KPI context lines | 10px | 400 | `color: var(--text-muted)` |
| Annotation findings | 12px | 700 | Normal case |
| Annotation explanations | 11px | 400 | `color: var(--text-mid)` |
| Footer, metadata | 10px | 400 | `color: var(--text-faint)` |

### Key Typographic Patterns

- **Instrument panel labels:** `uppercase letter-spacing: 2-2.5px` for section/panel headers (e.g., `THROUGHPUT`, `PAGE SPLITS`, `KEY FINDINGS`)
- **Tabular numbers everywhere:** `font-variant-numeric: tabular-nums` on all data
- **No serif, no sans-serif:** Pure monospace throughout including annotation prose (readable at 11-12px for the short 1-2 sentence findings)

---

## 4. Color System

### Monochrome Base

```css
:root {
  --bg:             #ffffff;    /* page background */
  --bg-off:         #f7f7f7;    /* inset surfaces, filter bar, KPI cards */
  --bg-dark:        #eeeeee;    /* hover states, active filters */
  --border:         #cccccc;    /* panel dividers, card borders */
  --border-strong:  #333333;    /* active tab underline, emphasis borders */
  --text:           #111111;    /* primary text */
  --text-mid:       #444444;    /* annotation explanations, secondary */
  --text-muted:     #888888;    /* labels, units, filter text */
  --text-faint:     #bbbbbb;    /* disabled, placeholders, footer */
}
```

**No accent color.** The only color in the entire interface comes from benchmark data:

### Key Type Colors (unchanged — already excellent)

```css
:root {
  --key-sequential:      #78716c;  /* muted grey — baseline, almost disappears into monochrome */
  --key-objectid:        #a16207;  /* warm brown — MongoDB native */
  --key-uuidv1:          #be123c;  /* dark red */
  --key-uuidv4:          #1d4ed8;  /* bright blue — the "villain" */
  --key-uuidv7:          #047857;  /* teal/green — the "winner" */
  --key-ulid:            #7e22ce;  /* purple */
  --key-ulid-monotonic:  #a855f7;  /* lighter purple */
}
```

### Database Colors (unchanged)

```css
:root {
  --db-postgres:   #336791;
  --db-mysql:      #00758f;
  --db-mongodb:    #116149;
  --db-cassandra:  #1287B1;
}
```

### Visual Rules

- **Borders:** `1px solid var(--border)` is the only spatial divider. No box shadows. No gradients.
- **Border radius:** `0` everywhere. Square corners. Instrument panel aesthetic.
- **Backgrounds:** Flat. `--bg` for page, `--bg-off` for inset cards/filters, `--bg-dark` for hover.
- **Icons:** Unicode symbols only (`◎`, `▸`, `■`, `▾`, `×`). No icon libraries.
- **Conditional formatting (tables):** Best value = `font-weight: 700; color: var(--border-strong)`. Worst value = `color: var(--text-muted)`. No colored backgrounds — stays monochrome.

---

## 5. Motion & Animation

Minimal, precise. The stillness is part of the identity.

- **KPI card entrance:** Staggered fade-up on page load. `opacity: 0 → 1`, `translateY: 12px → 0`. Duration: 500ms. Delay: 50ms between cards. Easing: `cubic-bezier(0.25, 0.1, 0.25, 1)`.
- **Tab/view transitions:** Crossfade, 200ms ease.
- **Chart panel entrance:** `scale(0.98) → scale(1)` + `opacity: 0 → 1`. Duration: 300ms.
- **Panel expand/collapse:** Height transition, 250ms ease-out.
- **No bounces, no spring physics, no hover animations on data elements.** Everything linear or ease-out.
- **Reduced motion:** Respect `prefers-reduced-motion: reduce` — disable all animations.

---

## 6. Layout & Navigation

### Three-View Structure

```
SUMMARY  →  EXPLORER  →  RAW DATA
(narrative)  (analysis)   (export)
```

Replaces the current 4-tab structure. Each view has a distinct purpose and user intent.

### Page Shell

```
┌─────────────────────────────────────────────────────────────┐
│  UUID BENCHMARK  │  Cross-database performance comparison   │
│                     of identifier strategies                │
├─────────────────────────────────────────────────────────────┤
│  ▸ SUMMARY          ▸ EXPLORER          ▸ RAW DATA         │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  (view content — full width, scrollable)                    │
│                                                             │
├─────────────────────────────────────────────────────────────┤
│  UUID Benchmark · Eike Rackwitz · 2026 · GitHub            │
└─────────────────────────────────────────────────────────────┘
```

**Header:**
- Left: `UUID BENCHMARK` — 11px, bold, `uppercase tracking-[2.5px]`
- Vertical pipe divider (`|`)
- Right: Subtitle in 10px, `--text-muted`
- Single compact line. No background color (unlike current parchment header).
- `border-bottom: 1px solid var(--border)`

**Tab bar:**
- 3 tabs, 10px, `uppercase tracking-[1.5px]`
- Active tab: `border-bottom: 2px solid var(--border-strong)`, `color: var(--text)`
- Inactive tabs: `color: var(--text-muted)`, no underline
- `border-bottom: 1px solid var(--border)` on the tab bar itself
- `▸` marker before active tab label

**Footer:**
- Single line: `UUID Benchmark · Eike Rackwitz · 2026 · GitHub`
- 10px, `--text-faint`
- `border-top: 1px solid var(--border)`
- GitHub links to the repo

### Max Width

`--max-width: 1200px` (wider than current 1120px to accommodate 2×2 chart grid).

---

## 7. Summary View (Landing)

The first thing visitors see. Narrative-first: leads with key findings, then invites exploration.

### Layout

```
┌─────────────────────────────────────────────────────────────┐
│  METHODOLOGY                                                │
│  5 runs per configuration · Median values · ±1 stddev ·    │
│  Fresh container per UUID type · 100K / 1M / 10M records   │
│  [▸ Details]                                                │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  KEY FINDINGS                                               │
│                                                             │
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐  │
│  │ ◎ INSERT   │  │ ◎ STRUCT   │  │ ◎ SCALE    │  │ ◎ ENGINE   │  │
│  │            │  │            │  │            │  │            │  │
│  │  -13.6%    │  │   50%      │  │   -73%     │  │  UUIDv7    │  │
│  │  to -30%   │  │ leaf frag  │  │  at 10M    │  │  best      │  │
│  │            │  │            │  │  MySQL     │  │  balance   │  │
│  │ UUIDv4     │  │ PG heap    │  │ UUIDv4     │  │ all DBs    │  │
│  │ vs seq.    │  │ B-tree     │  │ vs seq.    │  │            │  │
│  │  ▪▪▪ (3bar)│  │  ▪▪▪ (3bar)│  │  ▪▪▪ (3bar)│  │  ▪▪▪ (3bar)│  │
│  └────────────┘  └────────────┘  └────────────┘  └────────────┘  │
│                                                             │
│  DATABASES TESTED                                           │
│                                                             │
│  ┌────────────────┐  ┌────────────────┐                    │
│  │ POSTGRESQL     │  │ MYSQL          │                    │
│  │ B-tree / heap  │  │ Clustered      │                    │
│  │ pgbench        │  │ B-tree         │                    │
│  │ ▸ explore      │  │ ▸ explore      │                    │
│  ├────────────────┤  ├────────────────┤                    │
│  │ MONGODB        │  │ CASSANDRA      │                    │
│  │ WiredTiger     │  │ LSM-tree       │                    │
│  │ B-tree index   │  │ Compaction     │                    │
│  │ ▸ explore      │  │ ▸ explore      │                    │
│  └────────────────┘  └────────────────┘                    │
│                                                             │
│  UUID TYPES TESTED                                          │
│                                                             │
│  ■ SEQUENTIAL  ■ UUIDV1  ■ UUIDV4  ■ UUIDV7              │
│  ■ ULID  ■ ULID_MONO  ■ OBJECTID                          │
│  (colored squares matching key type colors)                 │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Components

**Methodology banner:**
- Compact single line with key facts, same as current
- `[▸ Details]` expands to show measurement tools, I/O measurement, isolation details
- Positioned at top of Summary view only
- 10px, `--text-muted`, `border-bottom: 1px solid var(--border)`

**KPI finding cards (4 cards):**
- `border: 1px solid var(--border)`, no radius, no shadow
- Background: `var(--bg-off)`
- Section label: `◎ INSERT` — 10px, uppercase, tracking-[2px], `--text-muted`
- Hero number: 24px, bold — the punchline stat (e.g., `-13.6% to -30%`)
- Context: 10px, `--text-muted` — what the number means (e.g., `UUIDv4 vs seq.`)
- Mini bar chart: 3 tiny bars (100K, 1M, 10M scale) instead of sparkline — more honest with 3 data points
- Staggered fade-up entrance animation (50ms delay between cards)
- Clickable: navigates to Explorer with relevant filters pre-set

**KPI card content (specific cards):**

1. **INSERT PENALTY** — Hero: `-13.6% to -30%`. Context: `UUIDv4 vs sequential`. Bars: throughput at 100K/1M/10M for UUIDv4 on PostgreSQL.
2. **STRUCTURAL DAMAGE** — Hero: `50%`. Context: `leaf fragmentation, PG heap B-tree`. Bars: fragmentation at 100K/1M/10M.
3. **SCALE EFFECT** — Hero: `-73%`. Context: `UUIDv4 at 10M, MySQL`. Bars: MySQL UUIDv4 throughput penalty at 3 scales.
4. **BEST BALANCE** — Hero: `UUIDv7`. Context: `across all databases`. Bars: UUIDv7 throughput as % of baseline across 4 databases.

**Database cards (2×2 grid):**
- `border: 1px solid var(--border)`, no radius
- Database name: 11px, bold, colored with database color (only place DB color appears)
- Architecture label: 10px, `--text-muted` (e.g., `B-tree / heap-organized`)
- Benchmark tool: 10px, `--text-muted` (e.g., `pgbench`, `Go workload binary`)
- `▸ explore` link: navigates to Explorer pre-filtered to that database

**UUID types legend:**
- Horizontal list of colored squares (`■`) with labels
- 10px, each square colored with key type color
- Serves as persistent color key for the entire site

---

## 8. Explorer View (Core Analysis)

Replaces the current 4-tab single-chart approach with a multi-panel layout.

### Layout

```
┌─────────────────────────────────────────────────────────────┐
│  METHODOLOGY  5 runs · Median · ±1 stddev · Fresh container│
├─────────────────────────────────────────────────────────────┤
│  DATABASE [PostgreSQL ▾]  SCENARIO [Insert ▾]              │
│  SCALE [1M ▾]  CONNECTIONS [1 ▾]                           │
├─────────────────────────────────────────────────────────────┤
│  CROSS-UUID          CROSS-DB          SCALE               │
│  ─────────                                                  │
├─────────────────────────────────────────────────────────────┤
│  ■ SEQ  ■ V1  ■ V4  ■ V7  ■ ULID  ■ ULID_M  ■ OID       │
├─────────────────────────────────────────────────────────────┤
│  ┌──────────────────────┐  ┌──────────────────────┐        │
│  │ THROUGHPUT    ops/sec│  │ LATENCY          μs  │        │
│  │                      │  │                      │        │
│  │  ████ ██ ████ ████   │  │  p50  p95  p99       │        │
│  │  ████ ██ ████ ████   │  │  ███  ███  ███       │        │
│  │  ████ ██ ████ ████   │  │  ███  ███  ███       │        │
│  │                      │  │                      │        │
│  └──────────────────────┘  └──────────────────────┘        │
│  ┌──────────────────────┐  ┌──────────────────────┐        │
│  │ PAGE SPLITS    count │  │ CACHE HIT RATIO  0-1 │        │
│  │                      │  │                      │        │
│  │  ████ ██ ████ ████   │  │  ████ ██ ████ ████   │        │
│  │  ████ ██ ████ ████   │  │  ████ ██ ████ ████   │        │
│  │  ████ ██ ████ ████   │  │  ████ ██ ████ ████   │        │
│  │                      │  │                      │        │
│  └──────────────────────┘  └──────────────────────┘        │
├─────────────────────────────────────────────────────────────┤
│  ◎ FINDING  (3 / 14)                        ◂ prev  next ▸│
│  UUIDv4 achieves 33,422 ops/sec, a 13.6% reduction...     │
│  Random key insertion causes mid-tree B-tree page splits   │
│  forcing costly rebalancing operations...                   │
└─────────────────────────────────────────────────────────────┘
```

### Sub-Components

**Methodology line:**
- Compact single line at top of Explorer (always visible)
- Same content as Summary methodology banner, no expandable detail
- 10px, `--text-muted`, `border-bottom: 1px solid var(--border)`

**Filter bar:**
- 4 dropdowns: DATABASE, SCENARIO, SCALE, CONNECTIONS
- Each: label in 10px uppercase tracking-[1px], `<select>` styled with monospace
- Same cascading filter logic as current implementation (only show valid combinations)
- `border-bottom: 1px solid var(--border)`
- Compact: single row on desktop, wraps to 2 rows on tablet

**View sub-tabs (replaces VIEW dropdown):**
- `CROSS-UUID` | `CROSS-DB` | `SCALE` — visible toggle row
- Same styling as main navigation tabs (10px uppercase, underline active)
- These control what the 4 panels display:
  - **CROSS-UUID:** X-axis = key types, comparing all UUID types for one database
  - **CROSS-DB:** X-axis = databases, comparing one key type across databases. Adds KEY TYPE dropdown to filter bar.
  - **SCALE:** Line charts with X-axis = 100K/1M/10M. Removes SCALE from filter bar (it becomes the X-axis).
- `border-bottom: 1px solid var(--border)`

**Legend strip:**
- Persistent horizontal line of colored squares with abbreviated labels
- `■ SEQ  ■ V1  ■ V4  ■ V7  ■ ULID  ■ ULID_M  ■ OID`
- 10px, colored squares use key type colors
- In CROSS-DB view, legend changes to database colors
- `border-bottom: 1px solid var(--border)`

**Chart panel grid (2×2):**
- 4 Chart.js canvases in a CSS grid: `grid-template-columns: 1fr 1fr`
- Each panel:
  - `border: 1px solid var(--border)`, no radius
  - Panel label: top-left, 10px, bold, uppercase, tracking-[2px] (e.g., `THROUGHPUT`)
  - Unit: top-right, 10px, `--text-muted` (e.g., `ops/sec`)
  - Click label to show metric info popover (definition + measurement methodology)
  - Chart.js canvas fills remaining space
  - Error bars (±1 stddev) on all bar charts
  - Hover tooltip shows exact value

**Default 4 panels per view mode:**

| Panel | Cross-UUID | Cross-DB | Scale |
|-------|-----------|---------|-------|
| Top-left | Throughput (bar) | Throughput (bar) | Throughput (line) |
| Top-right | Latency p50/p95/p99 (grouped bar) | Latency p50 (bar) | Latency p50 (line) |
| Bottom-left | Page splits (bar) | Page splits (bar) | Page splits (line) |
| Bottom-right | Cache hit ratio (bar) | Cache hit ratio (bar) | Cache hit ratio (line) |

**Panel adaptation per database:**
- **Cassandra:** Bottom-left swaps to `COMPACTION / SSTABLE COUNT`. Bottom-right swaps to `KEY CACHE HIT RATE`.
- **MongoDB:** Panels stay the same (WiredTiger uses B-tree, metrics map directly).
- If a metric returns no data for the current filter, panel shows `—` centered with `N/A for [database]` in `--text-faint`.

**Panel expand/collapse:**
- Click the panel label area (or a small `⤢` expand icon) to expand that panel to full width (spans both columns)
- Other 3 panels collapse to make room
- Click again (or `×` button) to return to 2×2 grid
- Essential for Scale line charts with 7 overlapping lines
- Smooth height/width transition, 250ms ease-out

**Comparability warning:**
- Shown below the chart grid when a non-comparable metric is displayed (e.g., fragmentation in Cross-DB view)
- Same content as current implementation
- Yellow/amber tint: `border-left: 3px solid #facc15`, `background: #fefce8` (only exception to monochrome rule — warnings should stand out)
- 10px monospace text

**Annotation section:**
- Below chart grid, `border-top: 1px solid var(--border)`
- Header: `◎ FINDING` left-aligned, `(3 / 14)` progress counter, `◂ prev  next ▸` right-aligned
- Finding text: 12px bold
- Explanation text: 11px, `--text-mid`
- `▸ next` auto-navigates filters to the next curated annotation and updates charts
- `◂ prev` goes back
- If current filter state matches a curated annotation, it auto-displays
- If no annotation exists for current filters, section shows `No curated finding for this combination` in `--text-faint`

### URL State / Deep Linking

Filter state is encoded in the URL hash for sharing:

```
#view=explorer&mode=cross-uuid&db=postgres&scenario=insert_performance&scale=1m&conn=1
#view=explorer&mode=cross-db&key=UUIDV4&scenario=insert_performance&scale=1m&conn=1
#view=explorer&mode=scale&db=mysql&scenario=insert_performance&conn=1
#view=raw&db=postgres&scenario=insert_performance
#view=summary
```

- On page load, parse hash and restore filter state
- On any filter change, update hash (using `replaceState` to avoid polluting browser history)
- Enables bookmarking and sharing specific views

---

## 9. Raw Data View

### Layout

```
┌─────────────────────────────────────────────────────────────┐
│  FILTER BAR                                                 │
│  DATABASE [All ▾]  SCENARIO [All ▾]  SCALE [All ▾]        │
│  CONNECTIONS [All ▾]                                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  KEY TYPE       METRIC          MEDIAN      MEAN     STDDEV│
│  ─────────────────────────────────────────────────────────  │
│  SEQUENTIAL     throughput      38,661     38,540      831 │
│  UUIDV1         throughput      36,220     36,100      642 │
│  UUIDV4         throughput      33,422     33,280      920 │
│  UUIDV7         throughput      37,891     37,750      510 │
│  ...                                                        │
│                                                             │
│  SORTED BY: KEY TYPE ▾           142 ENTRIES                │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Behavior

- **Filter bar:** Same 4 dropdowns but each has an `All` option to show broader data sets
- **Columns:** Key Type, Database, Scenario, Scale, Metric, Median, Mean, StdDev, CV%, Min, Max
- **Sortable:** Click column header to sort. Active sort column shows `▾` or `▴`
- **Conditional formatting:** Monochrome only. Best value per metric group: `font-weight: 700; color: var(--border-strong)`. Worst value: `color: var(--text-muted)`.
- **Sticky header row** on vertical scroll
- **Max height:** `70vh` with scrollable tbody
- **Row count:** Shown bottom-right, e.g., `142 ENTRIES`
- **Table styling:** 11px monospace, `border-bottom: 1px solid var(--border)` between rows, alternating `--bg` / `--bg-off` row backgrounds for readability

---

## 10. Responsive Behavior

### Breakpoints

| Breakpoint | Layout Changes |
|------------|---------------|
| Desktop (>1024px) | Full 2×2 chart grid, horizontal filter bar, 4 KPI cards in row |
| Tablet (768-1024px) | 2×2 grid with smaller charts, filter bar wraps to 2 rows, KPI cards 2×2 |
| Mobile (<768px) | Charts stack single column, filters stack vertically, KPI cards stack 2×2 |

### Mobile-Specific

- **Explorer:** Show 2 panels (throughput + latency) by default. `SHOW ALL METRICS ▸` toggle reveals the other 2 panels. Avoids excessive scrolling.
- **Filters:** Stack vertically, full-width dropdowns
- **KPI cards:** 2×2 grid (not 4-across)
- **Raw data table:** Horizontal scroll with sticky first column (Key Type)
- **Legend:** Wraps to 2 lines if needed
- **Touch targets:** Minimum 44px for all interactive elements

---

## 11. File Architecture

```
docs/
├── index.html                    # Single HTML document, 3 views
├── assets/
│   ├── style.css                 # All styles, CSS custom properties
│   ├── app.js                    # Entry: routing, view switching, init, URL state
│   ├── data.js                   # Data loading, filter state, cascading logic
│   ├── summary.js                # Summary view: KPIs, DB cards, legend
│   ├── explorer.js               # Explorer view: 4-panel grid, sub-tabs, expand
│   ├── rawdata.js                # Raw data table: sort, conditional formatting
│   ├── charts.js                 # Chart.js config, error bars, bar/line builders
│   ├── annotations.js            # Finding display, next/prev, progress tracking
│   └── constants.js              # Colors, labels, metric info, key type config
└── data/
    ├── data.json                 # Benchmark results (unchanged)
    └── annotations.json          # Curated findings (unchanged)
```

### Module Architecture

All JS files are ES modules loaded via `<script type="module">`.

```
index.html
  └── app.js (entry point)
        ├── constants.js (colors, labels, metric info)
        ├── data.js (fetch, filter state, cascading)
        ├── summary.js (renders Summary view)
        │     └── charts.js (mini bar charts for KPI cards)
        ├── explorer.js (renders Explorer view)
        │     ├── charts.js (4-panel Chart.js instances)
        │     └── annotations.js (finding display, navigation)
        └── rawdata.js (renders Raw Data view)
```

**Key design decisions:**
- `data.js` owns the global filter state and exports it. All view modules import from `data.js`.
- `charts.js` is a shared utility — builds Chart.js configurations for both Summary mini-charts and Explorer full panels.
- `constants.js` contains all color maps, metric labels, metric info definitions, key type configs — single source of truth.
- `annotations.js` handles finding lookup, next/prev navigation, and progress tracking.
- `app.js` handles URL hash parsing/updating, view switching, and initialization.

### Data Files (Unchanged)

- `data.json` — Same structure, same conversion pipeline (`scripts/convert_results.py`)
- `annotations.json` — Same structure, same curation process

### External Dependencies

- **Chart.js 4.4.7** — CDN (`cdn.jsdelivr.net`), loaded via `<script>` tag with `defer`
- **No other dependencies.** No Google Fonts (Courier New is a system font). No icon libraries. No CSS frameworks.

---

## 12. Accessibility

### ARIA

- `role="tablist"` on main navigation and Explorer sub-tabs
- `role="tab"`, `aria-selected` on tab buttons
- `aria-controls` linking tabs to view panels
- `role="tabpanel"` on each view container
- `aria-expanded` on expandable sections (methodology detail, panel expand)
- `aria-label` on icon-only buttons (expand, close, info)
- `aria-live="polite"` on annotation section (updates when finding changes)

### Motion

- Respect `prefers-reduced-motion: reduce` — disable all animations
- `@media (prefers-reduced-motion: reduce) { *, *::before, *::after { animation-duration: 0.01ms !important; transition-duration: 0.01ms !important; } }`

### Color

- Key type colors are distinguishable in greyscale (tested with current palette)
- Chart.js line charts use different dash patterns AND point styles per key type — color is not the sole differentiator
- Monochrome base has high contrast: `#111111` on `#ffffff` = ratio 17.4:1 (exceeds WCAG AAA)

### Keyboard

- Tab key navigates through interactive elements in logical order
- Enter/Space activates buttons and tabs
- Escape closes popovers and expanded panels
- Arrow keys navigate between tabs (left/right)

---

## 13. Chart Configuration

### Bar Charts (Cross-UUID, Cross-DB)

- Vertical bars
- Colors from key type palette (Cross-UUID) or database palette (Cross-DB)
- Error bars: ±1 stddev as whiskers (same Chart.js plugin as current)
- No data labels on bars (clean — values on hover)
- Grid lines: `color: var(--border)`, subtle
- Axis labels: 10px monospace
- No chart title (panel label serves as title)

### Grouped Bar Chart (Latency Panel)

- Groups of 3 bars per key type: p50, p95, p99
- Same color per key type, differentiated by opacity: p50 = 100%, p95 = 70%, p99 = 40%
- Legend shows `p50 / p95 / p99` within the panel

### Line Charts (Scale View)

- X-axis: logarithmic scale, labels `100K`, `1M`, `10M`
- One line per key type
- Dash patterns per key type (existing `KEY_TYPE_DASH` config — academic-style differentiation):
  - SEQUENTIAL: solid
  - OBJECTID: dotted `[2, 3]`
  - UUIDV1: dashed `[8, 4]`
  - UUIDV4: dash-dot `[8, 4, 2, 4]`
  - UUIDV7: long dash `[12, 4]`
  - ULID: short dash `[4, 4]`
  - ULID_MONOTONIC: dash-dot-dot `[8, 4, 2, 4, 2, 4]`
- Point styles per key type (existing `KEY_TYPE_POINT_STYLE` config):
  - SEQUENTIAL: circle, OBJECTID: triangle, UUIDV1: rect, UUIDV4: rectRot, UUIDV7: star, ULID: crossRot, ULID_MONOTONIC: cross
- Missing data points: line connects available points (skip nulls)
- Point radius: 4px, hover radius: 6px

### Tooltip

- Monospace font
- Shows: metric name, exact value, ±stddev
- No colored background — `border: 1px solid var(--border)`, `background: var(--bg)`

### Chart Sizing

- Each panel in 2×2 grid: approximately 580px × 280px on desktop (within 1200px max-width)
- Expanded panel: full width (~1160px) × 400px
- Mobile single-column: full width × 250px
- `maintainAspectRatio: false` with explicit container height

---

## 14. Implementation Notes

### Migration Strategy

This is a **clean rewrite** (Approach B). The current `app.js` and `style.css` are replaced entirely.

**What carries over:**
- `data.json` and `annotations.json` — unchanged
- Filter cascading logic — extracted from current app.js into `data.js`
- Chart.js configuration patterns — adapted into `charts.js`
- All color constants, metric labels, metric info — moved to `constants.js`
- Annotation lookup logic — moved to `annotations.js`

**What's new:**
- ES module architecture (8 files instead of 1 monolith)
- 3-view structure (Summary / Explorer / Raw Data)
- 4-panel chart grid with expand/collapse
- Sub-tab navigation within Explorer
- URL state management
- KPI cards with mini bar charts
- Legend strip
- Next/prev finding navigation with progress
- Monochrome terminal aesthetic

### Implementation Approach

**IMPORTANT: Use the `frontend-design` skill when implementing.** This skill ensures distinctive, production-grade aesthetics and avoids generic AI patterns. The monochrome terminal aesthetic must be executed with precision — every border weight, every letter-spacing value, every font size matters.

The implementation should proceed module by module:
1. `index.html` + `style.css` — page shell, CSS custom properties, all static styling
2. `constants.js` — all configuration extracted from current app.js
3. `data.js` — data loading + filter state (extracted from current app.js cascading logic)
4. `charts.js` — Chart.js builders (adapted from current app.js)
5. `summary.js` — Summary view with KPI cards and DB cards
6. `explorer.js` — Explorer view with 4-panel grid, sub-tabs, expand/collapse
7. `annotations.js` — Finding navigation
8. `rawdata.js` — Raw data table (adapted from current app.js)
9. `app.js` — Entry point, routing, URL state, init

Each module should be testable independently by loading it directly in the browser with `<script type="module">`.

---

## 15. What This Design Does NOT Include

Explicitly out of scope:

- **Dark mode** — not needed for this use case
- **Data export (CSV download)** — users can copy from Raw Data table
- **Print stylesheet** — not prioritized (the current chart-based approach doesn't print well anyway)
- **Search** — the filter system is sufficient for the data volume
- **User preferences / localStorage** — no state persistence between sessions (URL hash handles sharing)
- **Build step** — no bundling, no transpilation, no minification
- **Framework** — no React, Vue, or any framework dependency
- **Real-time data** — all data is static benchmark results
