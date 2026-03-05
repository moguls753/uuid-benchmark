# Mobile-First Dashboard Refactor — Design Document

**Date:** 2026-03-05
**Audience:** Thesis reviewers, professors, and developers finding this via GitHub
**Aesthetic direction:** Preserve the monochrome terminal aesthetic (JetBrains Mono, 1px borders, no radius, no shadows). Mobile improvements should feel like the same terminal adapted for a smaller viewport, not a different design system.

---

## 1. Spacing Scale

Introduce CSS custom properties to replace ad-hoc pixel values:

```css
:root {
  --sp-1: 4px;
  --sp-2: 8px;
  --sp-3: 12px;
  --sp-4: 16px;
  --sp-5: 20px;
  --sp-6: 24px;
  --sp-7: 32px;
  --sp-8: 40px;
}
```

Migrate all padding, margin, and gap values to use these tokens. This gives the dashboard the visual rhythm of consistent spacing steps (like Tailwind) while keeping the raw terminal feel.

---

## 2. Typography — Mobile (< 768px)

Balance terminal density with mobile readability:

| Element | Current | Mobile target | Rationale |
|---------|---------|---------------|-----------|
| Body text | 12px | **13px** | Minimum readable monospace. Preserves terminal feel. |
| Interactive UI (tabs, buttons, filter labels) | 11px | **14px** | Must be scannable and tappable quickly |
| Section labels / headings | 14px | **16px** | Clear hierarchy on small screens |
| KPI hero numbers | 26px | 26px (unchanged) | Already large enough |
| KPI context / sublabels | 11px | **13px** | Readable supporting text |
| Spark chart axis labels | 8px | **10px** | Absolute minimum readable size |
| Legend items | 11px | **13px** | Need to distinguish color+label |
| Footer | 10px | **12px** | Low priority but still readable |
| Data table cells | 12px | N/A (cards on mobile) | See section 6 |

On tablet (768px+) and desktop (1024px+), sizes remain at current values. The compact 11-12px terminal aesthetic is preserved where screen real estate allows.

---

## 3. Touch Targets

Every interactive element gets `min-height: 44px` on mobile:

| Element | Current height (approx) | Fix |
|---------|------------------------|-----|
| `.methodology-toggle` | ~20px | Add `padding: var(--sp-2) var(--sp-3)`, ensure `min-height: 44px` |
| `.db-explore` | ~24px | Add `min-height: 44px`, increase padding |
| `select` dropdowns | ~28px | `min-height: 44px`, `padding: var(--sp-2) var(--sp-3)` |
| `.annotation-prev/next` | ~28px | `min-height: 44px` |
| `.panel-expand` | ~22px | `min-height: 44px`, `min-width: 44px` |
| `.panel-metric-select` | ~22px | `min-height: 44px` |
| `.show-all-metrics` | ~36px | `min-height: 44px` |

Implementation: a mobile media query block that sets `min-height: var(--touch-target)` on all button/select/interactive elements.

---

## 4. Summary View — Mobile Layout

**KPI Grid:** Keep 2-column at all sizes (user preference). The hero numbers and labels are compact enough.

**Database Cards:** Single-column below 480px.

```css
@media (max-width: 480px) {
  .db-grid {
    grid-template-columns: 1fr;
  }
}
```

**Methodology Banner:** Toggle button gets 44px touch target. Content is already single-column on mobile.

**Legend Strip:** Font-size bump to 13px. Already wraps with `flex-wrap`.

---

## 5. Explorer View — Collapsible Filter Drawer

### Problem
5 `<select>` dropdowns wrap awkwardly on mobile into 2-3 rows of tiny controls.

### Solution

**Mobile (< 768px):**

1. **Filter toggle button** — replaces the visible filter bar:
   ```
   ┌─────────────────────────────────────┐
   │ ▶ FILTERS (2)                       │
   └─────────────────────────────────────┘
   ```
   The count shows non-default active filters.

2. **Active filter chips** — always visible below the toggle:
   ```
   PostgreSQL · Insert Performance · 1M
   ```
   Rendered as inline text in `--text-muted`, dot-separated. Not interactive — just context.

3. **Expanded drawer** — when open, filters stack vertically:
   ```
   ┌─────────────────────────────────────┐
   │ ▼ FILTERS (2)                       │
   ├─────────────────────────────────────┤
   │ DATABASE                            │
   │ ┌─────────────────────────────────┐ │
   │ │ PostgreSQL                    ▼ │ │
   │ └─────────────────────────────────┘ │
   │ SCENARIO                            │
   │ ┌─────────────────────────────────┐ │
   │ │ Insert Performance            ▼ │ │
   │ └─────────────────────────────────┘ │
   │ SCALE                               │
   │ ┌─────────────────────────────────┐ │
   │ │ 1M                            ▼ │ │
   │ └─────────────────────────────────┘ │
   │ CONNECTIONS                          │
   │ ┌─────────────────────────────────┐ │
   │ │ 1                             ▼ │ │
   │ └─────────────────────────────────┘ │
   └─────────────────────────────────────┘
   ```
   Each select is full-width, 44px tall. Labels above each.

**Tablet/Desktop (768px+):** No change — current horizontal filter bar stays.

### Implementation
- Add a `.filter-toggle` button and `.filter-chips` div to `index.html` (inside the existing filter-bar section)
- CSS: hide toggle/chips on 768px+, hide horizontal filters on < 768px
- JS: minimal toggle logic (same pattern as `bindMethodologyToggle` in `summary.js`)
- Chip text generated from current filter state in `explorer.js`

### Sub-tabs
Stay visible at all sizes. Already 44px min-height (38px + padding). Bump font to 14px on mobile.

### Chart panels
Already 1-column on mobile. "Show all metrics" toggle already exists for panels 3-4. Just increase touch target on metric-select and expand button.

---

## 6. Raw Data View — Card Layout on Mobile

### Problem
Dense table with horizontal scroll, `white-space: nowrap`, 12px monospace text. Unusable on mobile.

### Solution

**Mobile (< 768px):** Each table row renders as a card:

```
┌─────────────────────────────────────┐
│ ■ UUIDv4                            │
├─────────────────────────────────────┤
│ Throughput         12,450 ops/s     │
│ P50 Latency            245 µs      │
│ P95 Latency          1,230 µs      │
│ P99 Latency          4,891 µs      │
│ Page Splits            4,521       │
│ Cache Hit               0.94       │
│ Table Size           156.2 MB      │
│ Index Size            48.3 MB      │
└─────────────────────────────────────┘
```

- Key type name as card header with color swatch
- Metrics as key-value pairs in a 2-column CSS grid (label left, value right-aligned)
- Best/worst conditional formatting preserved (`.cell-best` bold, `.cell-worst` muted)
- Cards separated by 1px `var(--border)` — matching the grid-gap pattern used elsewhere
- 13px font, 44px row height for tappable sort/filter controls

**Filters:** Same collapsible drawer pattern as Explorer (reuse the same CSS/JS pattern).

**Tablet/Desktop (768px+):** Current table unchanged.

### Implementation
- In `rawdata.js`, check viewport width at render time
- If < 768px, generate card HTML instead of `<table>` rows
- Listen for `resize` events to switch between card/table if viewport changes (e.g., rotation)
- Same data source, different presentation

---

## 7. Interaction States

### Problem
All interactive feedback is `:hover`-only. Mobile has no hover state.

### Solution
Mirror every `:hover` rule with `:active` for tap feedback:

```css
.kpi-card:hover,
.kpi-card:active {
  background: var(--bg-dark);
}

.db-explore:hover,
.db-explore:active {
  color: var(--text);
}

.nav-tab:hover,
.nav-tab:active {
  color: var(--text-mid);
}
```

Add `:focus-visible` where keyboard navigation matters (tabs, buttons, selects). Already partially in place for `select:focus`.

### Chart Modal
- "ESC" label on close button: keep as-is (it's a hint, not the only way to close)
- Backdrop tap-to-close already works
- Modal sizing (`min(92vw, 1360px)`) already works on mobile

---

## 8. No Horizontal Scroll

Verify these don't cause horizontal overflow on 320px viewport:

| Element | Risk | Fix |
|---------|------|-----|
| `.site-header` | `white-space: nowrap` on subtitle | Already has `overflow: hidden; text-overflow: ellipsis` — OK |
| `.methodology-line` | Long text | Already `flex-wrap` — OK |
| `.legend-strip` | Many items | Already `flex-wrap` — OK |
| `.filter-bar` | 5 selects | Fixed by collapsible drawer |
| `.data-table` | Wide table | Fixed by card layout |
| `.annotation-nav` | prev/progress/next | Compact enough — OK |
| `.kpi-grid` | 2-column | Hero numbers may clip at 320px — add `overflow-wrap: break-word` as safety |

---

## 9. Summary of Changes by File

| File | Changes |
|------|---------|
| `style.css` | Spacing scale, mobile font sizes, touch targets, `:active` states, card layout styles, filter drawer styles, 480px breakpoint for DB grid |
| `index.html` | Filter toggle button + chip container in Explorer and Raw Data filter bars |
| `explorer.js` | Filter drawer toggle logic, chip text generation |
| `rawdata.js` | Card layout rendering path for mobile, resize listener |

No changes to: `app.js`, `data.js`, `constants.js`, `charts.js`, `annotations.js`, `summary.js` (beyond binding the methodology toggle touch target which is CSS-only).

---

## 10. Design Principles Preserved

- **Monochrome terminal aesthetic** — no new colors, no rounded corners, no shadows
- **JetBrains Mono everywhere** — just slightly larger on mobile
- **1px borders as the primary visual language** — cards use the same border pattern
- **Data color is the only color** — key type and database colors remain the sole chromatic elements
- **The wireframe IS the product** — mobile layout is still structured, grid-based, no decorative fluff
