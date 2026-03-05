# FT-Inspired Full-Bleed Refactor Design

**Date:** 2026-03-05
**Inspiration:** ft.com editorial layout — full-width rules, background bands, serif/mono typography pairing
**Constraint:** Monochrome palette stays. No border-radius, no shadows. Data colors only.

## Typography System

**Display font:** IBM Plex Serif (Google Fonts, weights 400/600/700)
- Pairs with monospace by design (IBM Plex family)
- Used for: `.site-brand`, `.section-label`, `.kpi-hero`, `.db-name`, `.annotation-finding`

**Data font:** JetBrains Mono (unchanged)
- Used for: nav tabs, filter labels, data values, methodology text, table cells, all "system" UI

## Layout Structure

Core change: decoration bleeds to viewport edges, content stays constrained.

```
<section class="band band--off">           ← full-width bg + border
  <div class="band-content">               ← max-width: 1440px, centered
    ...content...
  </div>
</section>
```

Currently, `max-width` is applied directly to header/nav/main/footer — borders and backgrounds stop at the content edge. The refactor wraps each major section in a full-bleed container.

## Background Bands

| Section | Background | Border |
|---|---|---|
| Header | `--bg` | bottom: `--border-strong` |
| Nav | `--bg` | bottom: `--border` |
| Methodology | `--bg-off` | bottom: `--border` |
| KPI "Key Findings" | `--bg` | — |
| DB "Databases Tested" | `--bg-off` | top + bottom: `--border` |
| Legend | `--bg` | top: `--border` |
| Explorer filter bar | `--bg-off` | bottom: `--border` |
| Chart grid | `--bg` | — |
| Annotations | `--bg-off` | top: `--border` |
| Footer | `--bg` | top: `--border-strong` |

## Typography Assignments

| Element | Font | Size | Weight |
|---|---|---|---|
| `.site-brand` | IBM Plex Serif | 16-18px | 700 |
| `.section-label` | IBM Plex Serif | 18-20px | 600 |
| `.kpi-hero` | IBM Plex Serif | 32-36px | 700 |
| `.db-name` | IBM Plex Serif | 13-14px | 700 |
| `.annotation-finding` | IBM Plex Serif | 14-15px | 700 |
| Everything else | JetBrains Mono | unchanged | unchanged |

## What Doesn't Change

- Color system (monochrome + data colors)
- No border-radius, no shadows
- Grid gap 1px trick for card dividers
- Interactive patterns (filters, modals, tabs)
- Mobile responsive behavior
- All JS functionality
