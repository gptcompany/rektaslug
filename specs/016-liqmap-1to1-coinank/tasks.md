# Tasks: Spec 016 - Liq-Map 1:1 Coinank Visual Match

**Spec**: `specs/016-liqmap-1to1-coinank/spec.md`
**Plan**: `specs/016-liqmap-1to1-coinank/plan.md`
**File**: `frontend/liq_map_1w.html` (ONLY)
**Validation**: `/validate-liqmap` (threshold >= 95%)

---

## Phase 1: Theme & Layout

- [ ] T001 [P1] White background + light grid + neutral font
  - Change `paper_bgcolor` and `plot_bgcolor` to `#ffffff`
  - Change `font.color` to `#333333`, add `family: 'Arial, sans-serif'`
  - Change `gridcolor` to `#f0f0f0` on both axes
  - Add `<body style="background: #fff; margin: 0; padding: 0;">`
  - Checklist: T2-19, T2-21

- [ ] T002 [P1] Remove chart title, axis titles, hide DOM metadata
  - Remove `title` property from `xaxis`, `yaxis`, `yaxis2`
  - Hide `#pageTitle` element (`display: none`)
  - Hide `#currentPrice` element (`display: none`)
  - Change `xaxis.tickformat` from `',.0f'` to `'.0f'` (no comma separator)
  - Add Y-axis tick formatting: `yaxis.tickformat: '.2s'` (shows M/B suffixes)
  - Add Y-axis2 tick formatting: `yaxis2.tickformat: '.2s'`
  - Checklist: T2-12..16

## Phase 2: Data Grouping

- [ ] T003 [P2] Replace LEVERAGE_COLORS with LEVERAGE_GROUPS
  - Define 3 groups: Low (5x,10x), Medium (25x,50x), High (100x)
  - Colors: Blue `#5B8FF9`, Purple `#B37FEB`, Orange `#FF9C6E`
  - Checklist: T2-02..04

- [ ] T004 [P2] Update groupByLeverage() for 3-group aggregation
  - New function: aggregate volumes across tiers within each group
  - Input: raw API data with individual leverage tiers
  - Output: { 'Low leverage': [...], 'Medium leverage': [...], 'High leverage': [...] }

- [ ] T005 [P2] Update buildBarTraces() for 3 groups
  - Iterate over LEVERAGE_GROUPS instead of individual tiers
  - Use group colors and group names
  - Keep `barmode: 'stack'` behavior
  - Deps: T003, T004

## Phase 3: Cumulative Fill Areas

- [ ] T006 [P3] Add fill to cumulative long trace
  - `fill: 'tozeroy'`
  - `fillcolor: 'rgba(232, 104, 74, 0.12)'`
  - `line.color: '#E8684A'`, `line.width: 2`
  - `showlegend: false`
  - Verify: Long curve descends L-to-R (cumulative from right)
  - Checklist: T2-05, T2-07

- [ ] T007 [P3] Add fill to cumulative short trace
  - `fill: 'tozeroy'`
  - `fillcolor: 'rgba(90, 216, 166, 0.12)'`
  - `line.color: '#5AD8A6'`, `line.width: 2`
  - `showlegend: false`
  - Verify: Short curve ascends L-to-R (cumulative from left)
  - Checklist: T2-06, T2-08

## Phase 4: Current Price Annotation

- [ ] T008 [P4] Replace current price scatter with annotation system
  - Remove the 2-point scatter trace for current price (lines 218-227)
  - Add `layout.shapes`: dashed vertical line, full-height (`yref: 'paper'`)
  - Add `layout.annotations`: label text with arrow, positioned above chart
  - Add scatter marker for bottom red dot: `y: [0]`, `marker.size: 8`
  - Remove current price from legend
  - Checklist: T2-09..11

## Phase 5: Legend & Range Slider

- [ ] T009 [P5] Center legend, limit to 3 items
  - `xanchor: 'center', x: 0.5`
  - Keep existing `yanchor: 'bottom', y: 1.02`
  - Ensure all non-group traces have `showlegend: false`
  - Adjust `margin.t` if needed for legend clearance
  - Checklist: T2-17, T2-18

- [ ] T010 [P5] Enable x-axis range slider
  - `xaxis.rangeslider: { visible: true, thickness: 0.05 }`
  - Adjust `margin.b` if slider gets clipped
  - Checklist: T2-20

## Validation

- [ ] T011 Visual verification: BTC/USDT 1W
  - Run `scripts/validate_liqmap_visual.py --exchange binance --coin BTC --coinank-timeframe 1w`
  - Run `/validate-liqmap`
  - Target: >= 95% score

- [ ] T012 Visual verification: ETH/USDT 1W
  - Same page with `?symbol=ETHUSDT`
  - Run validation with `--coin ETH --symbol ETHUSDT`
  - Target: >= 95% score

---

## Dependency Graph

```
T001 ──┐
       ├── T003 ── T004 ── T005 ──┐
T002 ──┘                           ├── T008 ── T009 ── T010 ── T011 ── T012
                    T006 ──────────┤
                    T007 ──────────┘
```

## Execution Order

1. T001 + T002 (parallel, independent layout changes)
2. T003 → T004 → T005 (sequential, data grouping)
3. T006 + T007 (parallel, cumulative fills)
4. T008 (current price annotation)
5. T009 + T010 (parallel, legend + slider)
6. T011 → T012 (sequential validation)
