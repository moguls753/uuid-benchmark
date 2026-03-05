# Calibrated Bump Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Apply UX refinements (wider max-width, JetBrains Mono font, +1px type scale, proportional spacing) to the existing dashboard without changing any logic or architecture.

**Architecture:** CSS-only changes in `style.css`, font loading in `index.html`, font/size constants in `charts.js`. No new files. No logic changes. No tests needed (visual CSS changes).

**Tech Stack:** CSS custom properties, Google Fonts CDN (JetBrains Mono), Chart.js font config.

**Design document:** `docs/plans/2026-03-04-dashboard-ux-calibrated-bump-design.md`

---

## Task 1: Add JetBrains Mono font loading to index.html

**Files:**
- Modify: `docs/index.html:4-9`

**Step 1: Add Google Fonts preconnect and stylesheet links**

In `docs/index.html`, add 3 lines after the viewport meta tag (line 5) and before the stylesheet link (line 7):

```html
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>UUID Benchmark</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
  <link rel="stylesheet" href="assets/style.css">
```

**Step 2: Update KPI chart canvas dimensions from 80×30 to 100×36**

In `docs/index.html`, find all 4 instances of:
```html
<canvas class="kpi-chart" width="80" height="30">
```
Replace with:
```html
<canvas class="kpi-chart" width="100" height="36">
```

There are exactly 4 occurrences (lines 59, 66, 73, 80).

**Step 3: Verify in browser**

Open `docs/index.html` in browser. JetBrains Mono should load after a brief flash of Courier New. KPI chart canvases should be slightly wider.

**Step 4: Commit**

```
feat(docs): add JetBrains Mono font loading and update KPI canvas sizes
```

---

## Task 2: Update CSS design tokens (font, max-width)

**Files:**
- Modify: `docs/assets/style.css:8-42`

**Step 1: Update custom properties**

Change these 2 lines in `:root` (lines 19-20):

Before:
```css
  --font-mono:      "Courier New", Courier, monospace;
  --max-width:      1200px;
```

After:
```css
  --font-mono:      "JetBrains Mono", "Courier New", Courier, monospace;
  --max-width:      min(1440px, calc(100vw - 48px));
```

**Step 2: Verify in browser**

- Text should render in JetBrains Mono
- Content should be wider (1440px max on large screens, fluid on smaller)
- All elements using `var(--max-width)` automatically pick up the change

**Step 3: Commit**

```
feat(docs): update font to JetBrains Mono and max-width to fluid 1440px
```

---

## Task 3: Update body and base font sizes

**Files:**
- Modify: `docs/assets/style.css:56-93`

**Step 1: Update body font-size**

Line 58: `font-size: 11px;` → `font-size: 12px;`

**Step 2: Update select font-size**

Line 79: `font-size: 11px;` → `font-size: 12px;`

**Step 3: Update select padding**

Line 84: `padding: 4px 6px;` → `padding: 5px 8px;`

**Step 4: Commit**

```
feat(docs): bump body font to 12px and select styling
```

---

## Task 4: Update page shell spacing (header, nav, footer, view panels)

**Files:**
- Modify: `docs/assets/style.css:103-205`

**Step 1: Update page-wrap padding**

Line 107: `padding: 0 16px;` → `padding: 0 20px;`

**Step 2: Update site-header**

Line 115: `padding: 12px 16px;` → `padding: 14px 20px;`

**Step 3: Update site-brand font-size**

Line 122: `font-size: 11px;` → `font-size: 12px;`

**Step 4: Update site-subtitle font-size**

Line 135: `font-size: 10px;` → `font-size: 11px;`

**Step 5: Update main-nav padding**

Line 149: `padding: 0 16px;` → `padding: 0 20px;`

**Step 6: Update nav-tab**

Line 153: `padding: 10px 16px;` → `padding: 10px 18px;`
Line 154: `font-size: 10px;` → `font-size: 11px;`

**Step 7: Update site-footer**

Line 180: `padding: 12px 16px;` → `padding: 12px 20px;`
(Footer font-size stays 10px — intentionally unchanged)

**Step 8: Update view-panel padding**

Line 200: `padding: 0 16px;` → `padding: 0 20px;`

**Step 9: Commit**

```
feat(docs): update page shell spacing for wider layout
```

---

## Task 5: Update Summary view font sizes and spacing

**Files:**
- Modify: `docs/assets/style.css:207-418`

**Step 1: Methodology banner**

Line 213: `font-size: 10px;` → `font-size: 11px;`

**Step 2: Methodology toggle**

Line 225: `font-size: 10px;` → `font-size: 11px;`

**Step 3: Methodology detail**

Line 239: `font-size: 11px;` → `font-size: 12px;`

**Step 4: Methodology detail h3**

Line 245: `font-size: 10px;` → `font-size: 11px;`

**Step 5: Methodology detail code**

Line 265: `font-size: 10px;` → `font-size: 11px;`

**Step 6: Section labels**

Line 270: `font-size: 13px;` → `font-size: 14px;`
Line 275: `margin-bottom: 12px;` → `margin-bottom: 14px;`

**Step 7: KPI card padding**

Line 292: `padding: 16px;` → `padding: 18px;`

**Step 8: KPI icon — merge onto same line as label**

Lines 307-311, change:
```css
.kpi-icon {
  font-size: 12px;
  color: var(--text-muted);
  margin-bottom: 4px;
}
```
To:
```css
.kpi-icon {
  font-size: 12px;
  color: var(--text-muted);
  display: inline;
  margin-right: 4px;
}
```

**Step 9: KPI label — inline to sit next to icon**

Lines 313-320, change:
```css
.kpi-label {
  font-size: 10px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 2px;
  color: var(--text-muted);
  margin-bottom: 6px;
}
```
To:
```css
.kpi-label {
  font-size: 11px;
  font-weight: 700;
  text-transform: uppercase;
  letter-spacing: 2px;
  color: var(--text-muted);
  margin-bottom: 6px;
  display: inline;
}
```

**Step 10: KPI hero**

Line 323: `font-size: 24px;` → `font-size: 28px;`

**Step 11: KPI context**

Line 330: `font-size: 10px;` → `font-size: 11px;`

**Step 12: KPI chart wrap**

Lines 336-337: `height: 30px; width: 80px;` → `height: 36px; width: 100px;`

**Step 13: DB card padding**

Line 355: `padding: 14px;` → `padding: 16px;`

**Step 14: DB name font-size**

Line 360: `font-size: 11px;` → `font-size: 12px;`

**Step 15: DB arch/tool font-size**

Line 372: `font-size: 10px;` → `font-size: 11px;`

**Step 16: DB explore font-size**

Line 378: `font-size: 10px;` → `font-size: 11px;`

**Step 17: Legend strip font-size**

Line 402: `font-size: 10px;` → `font-size: 11px;`

**Step 18: Commit**

```
feat(docs): update Summary view font sizes and spacing
```

---

## Task 6: Update Explorer view font sizes and spacing

**Files:**
- Modify: `docs/assets/style.css:420-698`

**Step 1: Explorer methodology**

Line 425: `font-size: 10px;` → `font-size: 11px;`

**Step 2: Filter bar gap**

Line 433: `gap: 12px;` → `gap: 14px;`

**Step 3: Filter group gap**

Line 442: `gap: 3px;` → `gap: 4px;`

**Step 4: Filter group label**

Line 450: `font-size: 10px;` → `font-size: 11px;`

**Step 5: Explorer tab**

Line 466: `padding: 8px 14px;` → `padding: 9px 14px;`
Line 467: `font-size: 10px;` → `font-size: 11px;`
Line 474: `min-height: 36px;` → `min-height: 38px;`

**Step 6: Comparability warning font-size**

Line 500: `font-size: 10px;` → `font-size: 11px;`

**Step 7: Chart panel padding**

Line 524: `padding: 12px;` → `padding: 14px;`

**Step 8: Panel label font-size**

Line 537: `font-size: 10px;` → `font-size: 11px;`

**Step 9: Panel unit font-size**

Line 546: `font-size: 10px;` → `font-size: 11px;`

**Step 10: Panel chart height (mobile default)**

Line 564: `height: 240px;` → `height: 250px;`

**Step 11: Expanded panel height**

Line 578: `height: 350px;` → `height: 360px;`

**Step 12: Panel N/A font-size**

Line 589: `font-size: 11px;` → `font-size: 12px;`

**Step 13: Show all metrics font-size**

Line 605: `font-size: 10px;` → `font-size: 11px;`

**Step 14: Annotation section padding**

Line 627: `padding: 12px 0;` → `padding: 14px 0;`

**Step 15: Annotation title font-size**

Line 649: `font-size: 10px;` → `font-size: 11px;`

**Step 16: Annotation progress font-size**

Line 655: `font-size: 10px;` → `font-size: 11px;`

**Step 17: Annotation prev/next font-size**

Line 669: `font-size: 10px;` → `font-size: 11px;`

**Step 18: Annotation finding font-size**

Line 682: `font-size: 12px;` → `font-size: 13px;`

**Step 19: Annotation explanation font-size**

Line 689: `font-size: 11px;` → `font-size: 12px;`

**Step 20: Annotation none font-size**

Line 695: `font-size: 11px;` → `font-size: 12px;`

**Step 21: No data font-size**

Line 704: `font-size: 11px;` → `font-size: 12px;`

**Step 22: Metric info popover font-size**

Line 720: `font-size: 10px;` → `font-size: 11px;`

**Step 23: Commit**

```
feat(docs): update Explorer view font sizes and spacing
```

---

## Task 7: Update Raw Data view font sizes and spacing

**Files:**
- Modify: `docs/assets/style.css:758-855`

**Step 1: Data table font-size**

Line 775: `font-size: 11px;` → `font-size: 12px;`

**Step 2: Table header padding**

Line 787: `padding: 8px 10px;` → `padding: 9px 12px;`

**Step 3: Table header font-size**

Line 789: `font-size: 10px;` → `font-size: 11px;`

**Step 4: Sort indicator font-size**

Line 809: `font-size: 8px;` → `font-size: 9px;`

**Step 5: Table cell padding**

Line 819: `padding: 6px 10px;` → `padding: 7px 12px;`

**Step 6: Table footer font-size**

Line 851: `font-size: 10px;` → (stays 10px — unchanged, metadata)

**Step 7: Commit**

```
feat(docs): update Raw Data table font sizes and spacing
```

---

## Task 8: Update responsive breakpoints

**Files:**
- Modify: `docs/assets/style.css:879-917`

**Step 1: Tablet chart height**

Line 891: `height: 260px;` → `height: 270px;`

**Step 2: Desktop header padding**

Line 907: `padding: 14px 16px;` → `padding: 16px 20px;`

**Step 3: Desktop chart height**

Line 911: `height: 280px;` → `height: 290px;`

**Step 4: Desktop KPI hero font-size**

Line 915: `font-size: 28px;` → `font-size: 32px;`

**Step 5: Add mobile KPI icon stacking below 400px**

After the reduced motion block (after line 927), add:

```css
/* --- 11. Responsive: Small Mobile (≤400px) ------------------------------- */

@media (max-width: 400px) {
  .kpi-icon {
    display: block;
    margin-right: 0;
    margin-bottom: 4px;
  }

  .kpi-label {
    display: block;
  }
}
```

**Step 6: Commit**

```
feat(docs): update responsive breakpoints and add small mobile override
```

---

## Task 9: Update Chart.js font configuration

**Files:**
- Modify: `docs/assets/charts.js:12,24-25,73,88,341`

**Step 1: Update CHART_FONT constant**

Line 12: `const CHART_FONT = '"Courier New", Courier, monospace';`
→ `const CHART_FONT = '"JetBrains Mono", "Courier New", Courier, monospace';`

**Step 2: Update tooltip font sizes**

Line 24: `titleFont: { family: CHART_FONT, size: 11, weight: '700' },`
→ `titleFont: { family: CHART_FONT, size: 12, weight: '700' },`

Line 25: `bodyFont: { family: CHART_FONT, size: 10 },`
→ `bodyFont: { family: CHART_FONT, size: 11 },`

**Step 3: Update axis tick font sizes**

Line 73: `font: { size: 10, family: CHART_FONT },`
→ `font: { size: 11, family: CHART_FONT },`

Line 88: `font: { size: 10, family: CHART_FONT },`
→ `font: { size: 11, family: CHART_FONT },`

**Step 4: Update scale chart label font**

Line 341: `font: { size: 10, weight: '700', family: CHART_FONT },`
→ `font: { size: 11, weight: '700', family: CHART_FONT },`

**Step 5: Verify in browser**

- Chart axis labels should render in JetBrains Mono at 11px
- Tooltips should render at 12px title / 11px body
- All charts should look correctly sized with the new font

**Step 6: Commit**

```
feat(docs): update Chart.js font to JetBrains Mono and bump sizes
```

---

## Task 10: Update CSS file header comment

**Files:**
- Modify: `docs/assets/style.css:1-5`

**Step 1: Update header comment to reflect new font**

```css
/* ==========================================================================
   UUID Benchmark Dashboard — Monochrome Terminal Aesthetic
   JetBrains Mono · 1px borders · No radius · No shadows · The wireframe IS the product.
   Only color comes from benchmark data.
   ========================================================================== */
```

**Step 2: Visual QA pass**

Open the dashboard in browser and verify:
- Summary view: KPI cards with icon+label on same line, hero numbers larger, mini charts wider
- Explorer view: filter bar readable, chart panels properly sized, annotations readable
- Raw Data view: table data clearly readable, numbers distinguishable
- Mobile: KPI cards stack icon/label below 400px, charts single-column
- Tab between Summary/Explorer/Raw Data — all look cohesive

**Step 3: Commit**

```
chore(docs): update CSS header comment for JetBrains Mono
```
