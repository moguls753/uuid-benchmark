# Dashboard UX Redesign Design

## Problem

The benchmark dashboard displays data correctly but lacks educational context and visual polish. Visitors (thesis reviewers, GitHub users, the author) see charts without understanding:
- What each metric means or how it was measured
- Why the numbers are the way they are (the mechanism behind the result)
- The statistical methodology (5 runs, medians, stddev)
- Which metrics are comparable across databases and which aren't

The visual design undershoots its "academic journal" target — currently reads as "default HTML with good fonts" rather than Nature/Economist/FT data journalism quality.

## Audience

- Thesis reviewers at FernUniversitat in Hagen evaluating the bachelor thesis
- GitHub visitors exploring UUID performance research
- The author during thesis writing and data analysis

## Design Direction

**Aesthetic: Editorial data journalism.** Inspired by FT Visual & Data Journalism, The Economist data pages, and Tufte's principles — but executed with more typographic confidence and visual hierarchy than the current version. The dashboard should feel like a well-designed figure supplement to an academic paper, not a generic web app.

Key principles:
- Data is the hero — UI serves the data
- Every number should explain itself
- Bold typographic hierarchy (not everything at the same visual volume)
- Warm, scholarly palette (keep the existing off-white paper tone)
- Print-friendly — a thesis reviewer might print this

## Sections

### 1. Methodology Context Banner

A compact, always-visible strip below the header showing statistical rigor at a glance.

**Content:**
```
5 runs per configuration  ·  Median values shown  ·  Error bars = ±1 stddev  ·  Fresh container per UUID type
```

**Expandable detail:** A "Methodology" link/button expands a panel with:
- What tools are used per database (pgbench for PostgreSQL, custom Go binary for MySQL/MongoDB/Cassandra)
- Why workloads run inside containers (zero network overhead)
- What cgroup v2 I/O measurement means
- Link to full `METRICS_METHODOLOGY.md` on GitHub

**Visual treatment:** Small caps, muted text, subtle top border. Should feel like a journal's "Methods" footnote — present but not competing with the data.

### 2. Metric Info Tooltips

Each metric in the dropdown gets a small info icon (`ⓘ`). Clicking/hovering shows a popover with two parts:

**a) Definition** — one sentence explaining the metric:
- "P99 Latency: 99% of operations completed within this time. Only 1 in 100 operations was slower."
- "Throughput: Operations completed per second during the workload phase."
- "Page Splits: Number of B-tree leaf page splits during inserts. More splits = more random I/O and wasted space."
- "Cache Hit Ratio: Fraction of page requests served from memory. 1.0 = all from RAM, 0.0 = all from disk."

**b) Measurement & comparability note:**
- "Measured identically across all databases via cgroup v2 kernel accounting." (for I/O metrics)
- "PostgreSQL: WAL inspection (exact). MySQL: innodb_metrics counter. Comparable — both count actual B-tree splits." (for page splits)
- "Different definitions per database — compare within one database only, not across." (for fragmentation)

**Data source:** A JS object in `app.js` containing metric definitions. Content derived from `docs/METRICS_METHODOLOGY.md` but condensed to 1-2 sentences each.

**Visual treatment:** Popover with subtle shadow, off-white background, small serif text for the definition (Crimson Pro italic), sans-serif for the measurement note. Dismissible on click-outside or Escape.

### 3. Handwritten Annotations (Finding + Explanation)

Below each chart, a caption area shows a hand-curated annotation with two parts:

**a) Finding** — what the numbers show (bold, factual):
> "UUIDv7 matches Sequential within 3%."

**b) Explanation** — why it's that way (regular weight, the mechanism):
> "UUIDv7 embeds a Unix timestamp in the most significant bytes. Inserts arrive in roughly monotonic order, so the B-tree appends to the rightmost leaf page instead of splitting random pages across the tree."

**Storage:** `docs/data/annotations.json` — a separate data file, easy to edit without touching code.

**Structure:**
```json
{
  "cross-uuid": {
    "postgres|insert_performance|1m|1|throughput": {
      "finding": "UUIDv7 matches Sequential within 3%.",
      "explanation": "UUIDv7 embeds a Unix timestamp in the most significant bytes. Inserts arrive in roughly monotonic order, so the B-tree appends to the rightmost leaf page instead of splitting random pages across the tree."
    }
  },
  "cross-db": {
    "insert_performance|1m|1|throughput": {
      "finding": "MySQL shows the largest UUIDv4 penalty among B-tree databases.",
      "explanation": "InnoDB's clustered index architecture means the primary key IS the table data. Random UUIDv4 inserts force full-page splits and data movement, whereas PostgreSQL's heap-based storage only splits the separate index structure."
    }
  },
  "scale": {
    "postgres|insert_performance|1|throughput": {
      "finding": "The throughput gap between Sequential and UUIDv4 widens at larger scales.",
      "explanation": "At 100K records the B-tree fits mostly in the buffer pool. At 10M, UUIDv4's random access pattern causes cache misses — the working set exceeds available memory and every split triggers disk I/O."
    }
  }
}
```

**Key format:** `database|scenario|scale|connections|metric` for cross-uuid, `scenario|scale|connections|metric` for cross-db (no database filter), `database|scenario|connections|metric` for scale (no scale filter). Matches the visible filter keys per tab.

**Behavior:** When no annotation exists for the current filter combination, the caption area is hidden — no forced insight. Silence is fine for unremarkable results.

**Visual treatment:** Styled like a figure caption in Nature. Finding in semi-bold, explanation in regular weight with slightly smaller font. Subtle left border accent (2px, muted color). Placed directly below the chart, inside the chart container.

### 4. Visual Polish

Guided by the "editorial data journalism" direction. Changes to existing elements:

**Header:**
- Subtle warm-tinted background (`#f5f0e8` or similar parchment tone) instead of flat white
- Larger title with more weight contrast (2rem+ on desktop)
- Fine horizontal rule below subtitle for visual anchoring

**Tabs:**
- Active tab gets a subtle background fill (warm tint) in addition to the bottom border
- Slightly larger text, more horizontal padding
- Active state is unmistakable even at a glance

**Filter bar:**
- Tighter grouping with subtle separator dots between filter groups
- Labels rendered as small-caps with tighter letter spacing
- Select elements: slightly rounded (4px), subtle inner shadow for depth

**Chart area:**
- Subtle box-shadow for depth (not flat border)
- More generous padding (2rem on desktop)
- Chart title in Crimson Pro (display font) instead of DM Sans — makes it feel like a figure title in a journal

**Table (Raw Data tab):**
- Best value per metric row: subtle green-tinted background (`#f0fdf4`)
- Worst value: subtle rose-tinted background (`#fef2f2`)
- This immediately draws the eye to winners/losers without reading numbers

**Footer:**
- Add "Companion to bachelor thesis at FernUniversitat in Hagen" context
- Link to the GitHub repository

**Color refinements:**
- Keep the existing key-type and database palettes (they're well-chosen)
- Add a subtle warm paper texture via CSS (very faint noise pattern or gradient)
- Ensure all colors pass WCAG AA contrast on the background

### 5. Comparability Warnings

When viewing a metric that has different definitions per database, show an inline info bar:

**Trigger:** Metric is in the non-comparable set (`fragmentation`, `avg_leaf_density`, `page_splits` in Cross-UUID view where a single database is selected).

**Content example:**
> "Fragmentation is measured differently per database. PostgreSQL measures physical page ordering; MySQL measures B-tree structural overhead. Compare trends within one database, not absolute values across databases."

**Visual treatment:** Subtle yellow/amber tinted bar with info icon. Compact — one line when possible. Sits between the filter bar and the chart.

**Note:** The Cross-DB tab already filters out non-comparable metrics. This warning is for the Cross-UUID tab where a user might mentally compare "PostgreSQL fragmentation = 12%" with "MySQL fragmentation = 44%" without realizing they measure different things.

## Files Changed

| File | Changes |
|------|---------|
| `docs/index.html` | Add methodology banner, annotation container, comparability warning bar, metric info popover structure |
| `docs/assets/style.css` | Visual polish (header, tabs, filters, chart, table conditional formatting, annotation styles, popover styles, warning bar) |
| `docs/assets/app.js` | Metric definitions object, annotation loading from JSON, popover logic, comparability warning logic, table conditional formatting |
| `docs/data/annotations.json` | **New file** — hand-curated findings + explanations keyed by filter state |

## What's NOT Changing

- Tab structure (Cross-UUID, Cross-DB, Scale, Raw Data) — stays the same
- Cascading filter logic — already well-built
- Chart.js configuration and error bar plugin — working correctly
- Data pipeline (`scripts/convert_results.py`) — untouched
- `docs/data/data.json` — untouched

## Out of Scope

- Dark mode
- URL state / shareable links (nice-to-have, separate task)
- Responsive redesign (current mobile-first approach is adequate)
- Framework migration (staying vanilla JS)
