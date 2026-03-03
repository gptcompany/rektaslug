# Implementation Plan: Liq-Map 1:1 Coinank Visual Match

**Branch**: `016-liqmap-1to1-coinank` | **Date**: 2026-03-03 | **Spec**: `specs/016-liqmap-1to1-coinank/spec.md`
**Input**: Feature specification from `/specs/016-liqmap-1to1-coinank/spec.md`

## Summary

Achieve a near 1:1 visual match between our `liq-map` page and Coinank's liquidation map.
Single file modification (`frontend/liq_map_1w.html`, 316 LOC) applying 8 sequential visual
transformations. Validation target: >= 95% on `/validate-liqmap` (32-element checklist).

## Technical Context

**Language/Version**: JavaScript (Plotly.js 2.26.0), HTML5
**Primary Dependencies**: Plotly.js CDN (already loaded)
**Storage**: N/A (frontend-only changes)
**Testing**: Visual validation via `scripts/validate_liqmap_visual.py` + `/validate-liqmap`
**Target Platform**: Browser (Chrome/Firefox)
**Project Type**: Single file frontend
**Performance Goals**: Chart renders in <2s, no regressions from current behavior
**Constraints**: Single file modification only, no backend changes, no new dependencies
**Scale/Scope**: BTC/USDT primary, ETH/USDT secondary (same code path, parametric via `?symbol=`)

## Constitution Check

| Gate | Status | Notes |
|------|--------|-------|
| Mathematical Correctness | N/A | No calculation changes, only visual |
| TDD | ADAPTED | Visual validation replaces unit tests (screenshot comparison) |
| Exchange Compatibility | N/A | No exchange formula changes |
| Performance Efficiency | PASS | No performance impact expected |
| Data Integrity | N/A | No data changes |
| Graceful Degradation | PASS | Fallback: current rendering works if JS fails |
| Progressive Enhancement | PASS | Each step is independently verifiable |
| Documentation Completeness | PASS | Spec + checklist fully documented |

## Project Structure

### Documentation (this feature)

```
specs/016-liqmap-1to1-coinank/
├── spec.md              # Canonical spec (8 steps, gap table, validation)
├── plan.md              # This file
└── tasks.md             # Task breakdown (created by /speckit.tasks)
```

### Source Code (single file)

```
frontend/
└── liq_map_1w.html      # ONLY file to modify (316 LOC -> ~320 LOC estimated)
```

**Structure Decision**: No new files needed. All changes are in-place modifications
to `frontend/liq_map_1w.html`.

## Implementation Phases

### Phase 1: Theme & Layout (Steps 1-2)

**Goal**: White background, clean grid, remove all titles.

| Task | Checklist IDs | Changes |
|------|---------------|---------|
| White background + light grid + font | T2-19, T2-21 | `paper_bgcolor`, `plot_bgcolor`, `font`, `gridcolor` |
| Remove chart title, axis titles | T2-12..16 | Delete `title` from xaxis/yaxis/yaxis2 |
| Hide DOM elements (pageTitle, currentPrice) | T2-16 | `display: none` on `#pageTitle`, `#currentPrice` |
| X-axis format: no comma separator | T2-14 | `tickformat: '.0f'` (was `',.0f'`) |
| Body style white | - | `<body style="background: #fff; margin: 0; padding: 0;">` |

### Phase 2: Data Grouping (Step 3)

**Goal**: Replace 5 individual leverage tiers with 3 grouped buckets.

| Task | Checklist IDs | Changes |
|------|---------------|---------|
| Define LEVERAGE_GROUPS constant | T2-02..04 | Replace `LEVERAGE_COLORS` with 3-group mapping |
| Aggregate groupByLeverage() | T2-02 | Sum volumes for tiers within each group |
| Update buildBarTraces() | T2-03 | Use 3 groups with new colors (Blue/Purple/Orange) |
| Remove/adapt buildAreaTraces() | - | Keep for `?chart=area` backward compat, but default is bar |

### Phase 3: Cumulative Fill Areas (Step 4)

**Goal**: Add filled areas under cumulative long/short curves.

| Task | Checklist IDs | Changes |
|------|---------------|---------|
| Add fill to cumulative long trace | T2-05, T2-06 | `fill: 'tozeroy'`, `fillcolor: 'rgba(232,104,74,0.12)'` |
| Add fill to cumulative short trace | T2-07, T2-08 | `fill: 'tozeroy'`, `fillcolor: 'rgba(90,216,166,0.12)'` |
| Hide cumulative from legend | T2-17 | `showlegend: false` on both |

### Phase 4: Current Price Annotation (Step 5)

**Goal**: Replace scatter trace with annotation + shape + bottom dot.

| Task | Checklist IDs | Changes |
|------|---------------|---------|
| Add dashed line shape (full height) | T2-09 | `layout.shapes` with `yref: 'paper'` |
| Add annotation label with arrow | T2-10 | `layout.annotations` with arrow + text |
| Add bottom red dot marker | T2-11 | Scatter trace `y: [0]`, `showlegend: false` |
| Remove old current price scatter trace | T2-09 | Delete the 2-point line trace |

### Phase 5: Legend & Range Slider (Steps 6-7)

**Goal**: 3-item centered legend + x-axis range slider.

| Task | Checklist IDs | Changes |
|------|---------------|---------|
| Center legend horizontally | T2-17, T2-18 | `xanchor: 'center', x: 0.5` |
| Limit legend to 3 leverage groups | T2-17 | All other traces: `showlegend: false` |
| Enable range slider | T2-20 | `xaxis.rangeslider: { visible: true, thickness: 0.05 }` |

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Plotly API incompatibility | LOW | MEDIUM | All features used are Plotly.js 2.x stable |
| ETH rendering breaks | LOW | LOW | Same code path, parametric; test both symbols |
| Range slider conflicts with stacked bars | MEDIUM | LOW | Test visual rendering; can adjust `thickness` |
| Data freshness skews validation | HIGH | MEDIUM | Run ingestion before validation (operational gate) |

## Validation Strategy

1. After each phase: visual inspection via browser at `localhost:8002`
2. After all phases: `scripts/validate_liqmap_visual.py` (automated screenshot + manifest)
3. Final gate: `/validate-liqmap` (alpha-visual element-by-element scoring, threshold >= 95%)
4. ETH verification: same pipeline with `--symbol ETHUSDT --coin ETH`

## Complexity Tracking

No constitution violations. Single-file, frontend-only change with no new dependencies.
