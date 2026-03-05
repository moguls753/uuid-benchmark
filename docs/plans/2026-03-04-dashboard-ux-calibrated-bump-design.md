# Dashboard UX Refinement — "Calibrated Bump" Design

**Date:** 2026-03-04
**Author:** Eike Rackwitz
**Status:** Approved
**Applies to:** Design doc `2026-03-04-dashboard-redesign-design.md`
**Implementation skill:** `frontend-design`

---

## 1. Context

The dashboard redesign (monochrome terminal aesthetic, 3-view structure, 2×2 chart grid) is aesthetically strong. This document refines two foundational UX issues — fixed 1200px width and small 10-11px Courier New font — and cascades those fixes through the full design.

**Audience:** Both academic thesis reviewers (1366-1440px laptops) and developers (1920px+ monitors).

---

## 2. Max Width

**Before:** `--max-width: 1200px` (fixed)

**After:**
```css
--max-width: min(1440px, calc(100vw - 48px));
```

| Viewport | Content width | Margin each side |
|----------|--------------|-----------------|
| 1366px   | 1318px       | 24px (natural)  |
| 1440px   | 1392px       | 24px            |
| 1920px   | 1440px (cap) | 240px           |
| 2560px   | 1440px (cap) | 560px           |

**Impact:** Each chart panel in the 2×2 grid goes from ~580px to ~700px. Each KPI card goes from ~290px to ~350px.

---

## 3. Font

**Before:** `"Courier New", Courier, monospace` (system font, zero dependencies)

**After:**
```css
--font-mono: "JetBrains Mono", "Courier New", Courier, monospace;
```

**Loading:**
```html
<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
```

Three weights: 400 (body), 500 (labels/tabs), 700 (headers/hero). ~30KB total (woff2). Courier New fallback if CDN unavailable. `font-display: swap` renders immediately with fallback, swaps when loaded (~100-200ms).

**Why JetBrains Mono:** Designed for code/data readability at small sizes. Crisper glyph differentiation than Courier New for tabular numbers (`33,422` vs `34,993`). Same monospace character, better optics. MIT-licensed.

---

## 4. Type Scale

Consistent +1px bump across the board. Floor moves from 10px to 11px. Footer stays at 10px.

| Role | Before | After |
|------|--------|-------|
| Hero KPI numbers | 24px (28px desktop) | 28px (32px desktop) |
| Section headers | 13px | 14px |
| Nav tabs | 10px | 11px |
| Filter labels | 10px | 11px |
| Chart panel labels | 10px | 11px |
| Chart panel unit | 10px | 11px |
| Body / table data / axes | 11px | 12px |
| Select dropdowns | 11px | 12px |
| Annotation findings | 12px | 13px |
| Annotation explanations | 11px | 12px |
| KPI label | 10px | 11px |
| KPI context | 10px | 11px |
| Legend strip | 10px | 11px |
| Methodology | 10px | 11px |
| Prev/next buttons | 10px | 11px |
| Sort indicator | 8px | 9px |
| Footer / metadata | 10px | 10px (unchanged) |

---

## 5. Spacing & Padding

Proportional +2-4px increase where text got bigger. Preserves dense terminal feel.

| Element | Before | After |
|---------|--------|-------|
| Header padding | `12px 16px` (14px desktop) | `14px 20px` (16px desktop) |
| Nav tab padding | `10px 16px` | `10px 18px` |
| View panel padding | `0 16px` | `0 20px` |
| KPI card padding | `16px` | `18px` |
| KPI mini chart canvas | `80×30px` | `100×36px` |
| KPI grid gap | `1px` | `1px` (unchanged) |
| DB card padding | `14px` | `16px` |
| Chart panel padding | `12px` | `14px` |
| Filter bar gap | `12px` | `14px` |
| Filter label-to-select gap | `3px` | `4px` |
| Select padding | `4px 6px` | `5px 8px` |
| Section label margin-bottom | `12px` | `14px` |
| Annotation section padding | `12px 0` | `14px 0` |
| Table cell padding | `6px 10px` | `7px 12px` |
| Table header padding | `8px 10px` | `9px 12px` |

**Unchanged:** 1px borders, 0 border-radius, grid gap (1px), animation timing, letter-spacing values.

---

## 6. KPI Cards

With wider cards (~350px vs ~290px), the icon and label move to the same line:

**Before:**
```
◎
INSERT PENALTY
−13.6% to −30%
UUIDv4 vs seq.
▪▪▪ (80×30)
```

**After:**
```
◎ INSERT PENALTY
−13.6% to −30%
UUIDv4 vs sequential
▪▪▪▪▪ (100×36)
```

- Saves one vertical line per card
- Context text has room for full labels
- Mini chart 25% larger — bars are actually distinguishable

---

## 7. Chart Heights

+10px per breakpoint to absorb larger axis labels (JetBrains Mono is slightly wider per-glyph than Courier New):

| Breakpoint | Before | After |
|------------|--------|-------|
| Mobile (<768px) | 240px | 250px |
| Tablet (768px+) | 260px | 270px |
| Desktop (1024px+) | 280px | 290px |
| Expanded | 350px / 400px | 360px / 400px |

---

## 8. Raw Data Table

At 1440px with ~10 columns, each column averages ~133px (vs ~110px before). Numbers like `33,422` and headers like `STDDEV` have more breathing room. All changes are the type scale and spacing cascades above — no structural table changes.

---

## 9. Mobile & Responsive

Breakpoints unchanged: 768px (tablet), 1024px (desktop).

**Fluid max-width handles everything naturally.** `min(1440px, calc(100vw - 48px))` adapts without extra breakpoints.

**Mobile-specific adjustment:** Below 400px viewport, KPI card icon+label stack vertically again (at ~158px card width, same-line would be too tight with 11px text):

```css
@media (max-width: 400px) {
  .kpi-card .kpi-icon { display: block; margin-bottom: 4px; }
}
```

Above 400px, icon+label stay on same line.

**Explorer sub-tabs:** 11px (from 10px), 9px padding (from 8px), 38px min-height (from 36px).

**No new breakpoints added.**

---

## 10. What Does NOT Change

- 1px borders, 0 border-radius, no shadows (the identity)
- Monochrome color system (base) + data-only color (key types, databases)
- 3-view structure (Summary / Explorer / Raw Data)
- 2×2 chart grid layout and expand/collapse
- Filter bar behavior (horizontal, cascading, wrapping on tablet)
- ES module architecture (9 files)
- Animation timing and easing
- Letter-spacing on uppercase labels
- Data files (data.json, annotations.json)
- URL state / deep linking
- Accessibility (ARIA, keyboard, reduced motion)
- Chart.js configuration (tooltips, error bars, dash patterns, point styles)
