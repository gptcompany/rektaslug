# Feature Specification: Liq-Map 1:1 Coinank Visual Match

**Feature ID**: 016
**Priority**: HIGH
**Status**: READY
**Canonical Source**: `specs/016-liqmap-1to1-coinank/spec.md`

## Purpose

This mirror exists only for `.specify` / `speckit` compatibility.

The source of truth remains:
- `specs/016-liqmap-1to1-coinank/spec.md`

The implementation goal is unchanged:
- achieve a near 1:1 visual match between our `liq-map` page and Coinank
- focus only on `liq-map`
- modify a single frontend file
- validate BTC first, then ETH on the same page

## Scope

- File to change: `frontend/liq_map_1w.html`
- Primary target: BTC/USDT 1W
- Same implementation path must also support ETH/USDT on the same page
- Target validation score: `>= 95%` on `/validate-liqmap`
- Reference checklist: `.claude/commands/validate-liqmap.md`

Out of scope for this spec:
- `liq-heat-map`
- backend API refactors
- multi-file frontend redesign

## Current Gaps vs Coinank

The active gaps to close are:

1. chart structure still differs (stacked bars vs current rendering)
2. leverage tiers are still shown as 5 raw buckets instead of 3 grouped buckets
3. cumulative curves are missing filled areas
4. current price marker differs from Coinank (annotation + arrow + bottom dot)
5. white background / light grid styling is missing
6. axis titles and chart title must disappear
7. legend must shrink to 3 centered items
8. bottom range slider must be visible

## Required Implementation Path

Only one file should be modified:
- `frontend/liq_map_1w.html`

The intended implementation remains the same 8-step flow defined in the canonical spec:

1. Switch layout to white background, light grid, neutral font.
2. Remove chart title, axis titles, and hide top metadata elements.
   Important: hide both `pageTitle` and the DOM `currentPrice` label to avoid duplicate price display outside the chart.
3. Replace raw leverage tiers with 3 grouped buckets:
   - Low leverage
   - Medium leverage
   - High leverage
4. Add filled cumulative areas for long and short curves.
5. Replace current-price trace with:
   - full-height dashed vertical line
   - annotation label above
   - bottom red dot marker
6. Center the legend horizontally and limit it to the 3 leverage groups only.
7. Enable Plotly range slider on the x-axis.
8. Ensure body/background styling is white and minimal.

## Freshness Requirement

The visual comparison requires recent DuckDB data.

Operational rule:
- data should be fresher than `5 minutes` before validation

Important:
- this is the required operational gate
- the canonical spec explains the exact commands
- the validator logic has been tightened, but this mirror still treats the canonical spec as the detailed reference

## Data Flow Assumption

`ccxt-data-pipeline` is upstream only.

It updates the Parquet catalog, not DuckDB directly.

`rektslug` is responsible for:
- bridging Parquet -> DuckDB
- serving the API
- exposing validation/dashboard endpoints

## Validation Flow

Use the canonical spec for the exact commands, but the expected process is:

1. ensure data freshness (`< 5 min`)
2. start the local API if not already running
3. compare:
   - local: `/chart/derivatives/liq-map/binance/btcusdt/1w`
   - remote: `https://coinank.com/chart/derivatives/liq-map/binance/btcusdt/1w`
4. run `scripts/validate_liqmap_visual.py`
5. run `/validate-liqmap`
6. repeat with ETH using the same page and `--symbol ETHUSDT`

## Canonical Working Links

- Local BTC 1W: `http://localhost:8002/chart/derivatives/liq-map/binance/btcusdt/1w`
- Local ETH 1W: `http://localhost:8002/chart/derivatives/liq-map/binance/ethusdt/1w`
- Coinank BTC 1W: `https://coinank.com/chart/derivatives/liq-map/binance/btcusdt/1w`
- Coinank ETH 1W: `https://coinank.com/chart/derivatives/liq-map/binance/ethusdt/1w`

## Session Entry

For direct work:
- `/pipeline:speckit 016`

If the pipeline still needs exact details, the canonical file to read is:
- `specs/016-liqmap-1to1-coinank/spec.md`
