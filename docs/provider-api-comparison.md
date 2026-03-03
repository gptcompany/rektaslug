# Provider API Comparison Workflow

## Scope

This runbook documents the current workflow for capturing and comparing raw
liquidation data across external providers:

- CoinAnk
- Coinglass
- Bitcoin CounterFlow

The goal is to compare raw API payloads before making assumptions about how any
provider computes or visualizes liquidation maps.

## Current State

Two scripts now cover the workflow:

- `scripts/capture_provider_api.py`
- `scripts/compare_provider_liquidations.py`
- `scripts/coinglass_decode_payload.js`

The first script captures raw JSON responses during page load with Playwright.
The second script reads one or more capture manifests, normalizes the most
relevant liquidation dataset per provider, and emits a comparison report.

A third script now orchestrates the full workflow:

- `scripts/run_provider_api_comparison.py`

For SQL-backed review of historical runs, there is also:

- `scripts/provider_comparison_sql_report.py`

This wrapper performs capture, normalization, comparison, report writing, and
optional DuckDB persistence in one command.

## Naming Convention

The repository uses `snake_case` for Python scripts under `scripts/`. The new
files follow that convention intentionally:

- `capture_provider_api.py`
- `compare_provider_liquidations.py`

Do not rename these to kebab-case unless the whole script naming convention in
the repo changes. Kebab-case is acceptable for Markdown docs, so this file uses
`provider-api-comparison.md`.

## Capture Workflow

### 1. Capture provider traffic

Examples:

```bash
python3 scripts/capture_provider_api.py \
  --provider bitcoincounterflow
```

```bash
python3 scripts/capture_provider_api.py \
  --provider both \
  --coin BTC \
  --timeframe 1w \
  --coinglass-url "https://www.coinglass.com/..."
```

```bash
python3 scripts/capture_provider_api.py \
  --provider all \
  --coin BTC \
  --timeframe 1w \
  --coinglass-url "https://www.coinglass.com/..."
```

Captures are written under:

`data/validation/raw_provider_api/<timestamp>/`

Each run contains:

- one subdirectory per provider
- `summary.json` per provider
- `manifest.json` for the whole run

Each captured response now also preserves the small header subset needed for
future provider-specific decoding:

- Coinglass response headers such as `user`, `time`, `v`, `encryption`
- the request header `cache-ts-v2` when present

### 2. Compare captured payloads

```bash
python3 scripts/compare_provider_liquidations.py \
  --manifest data/validation/raw_provider_api/<timestamp>/manifest.json
```

If no manifest is provided, the script uses the latest capture run.

Reports are written under:

`data/validation/provider_comparisons/`

### 2b. Query historical comparisons from DuckDB

```bash
python3 scripts/provider_comparison_sql_report.py
```

Use `--json` for machine-readable output or `--db-path` to point at a
non-default DuckDB.

### 3. Run the full workflow in one command

```bash
python3 scripts/run_provider_api_comparison.py \
  --provider all \
  --coin BTC \
  --timeframe 1w
```

By default, this command also persists normalized comparison rows into the
validation DuckDB (`data/validation/validation_results.duckdb`). Use
`--no-persist-db` if you want a file-only run.

## Provider-Specific Notes

### CoinAnk

- Capture is page-driven and reuses the existing CoinAnk login flow.
- `getAggLiqMap` is parsed explicitly.
- The remaining limitation is semantic, not structural: the endpoint exposes
  one magnitude array per exchange, so long/short are still inferred by
  splitting around `lastPrice`.
- A better CoinAnk source is still worth finding if it exposes separate
  long/short ladders.

### Coinglass

- Capture requires an explicit `--coinglass-url`.
- Login support is best-effort.
- The current public endpoints often return an encoded string in `data`.
- `scripts/compare_provider_liquidations.py` now has a header-aware decode path
  for Coinglass, but it only works when the capture includes the required
  response headers and a local `_app-*.js` bundle is available.
- `scripts/coinglass_decode_payload.js` mirrors the site decoder by loading the
  bundled frontend `CryptoJS` and `pako` modules directly from the saved
  Coinglass bundle.
- If the capture is missing `user`/`v` (or the bundle), the parser falls back
  to envelope metadata only.

### Bitcoin CounterFlow

- `scripts/capture_provider_api.py` explicitly triggers a fetch for
  `/api/liquidations`.
- The endpoint is not fully public. It returns `403` without an allowed
  `Origin`.
- It returns `200` when called from:
  - `https://bitcoincounterflow.com`
  - the extension origin
- The comparison script parses this payload explicitly as a USD-notional
  liquidation time-series.

Important distinction:

- `api/liquidations` is a raw time-series feed
- the website/extension `Liquidation Heatmap` view also has a client-side
  synthetic heatmap path built from proxied Binance klines

Do not assume these two views are the same dataset.

## What Is Still Missing

The workflow is usable, but it is not complete yet.

The main missing pieces are:

1. A better CoinAnk endpoint with explicit long/short separation
2. Stable Coinglass captures that include whatever headers are required for
   full numeric decode
3. Schema-level comparison once at least two providers have stable captured
   liquidation endpoints
4. Optional historical aggregation across many capture runs

Today, Bitcoin CounterFlow, CoinAnk, and Coinglass all have provider-specific
parsers.

CoinAnk now also has a provider-specific parser for `getAggLiqMap`, but its
long/short split is still inferred by dividing bins around `lastPrice` because
the endpoint exposes one magnitude array per exchange rather than separate long
and short arrays.

Coinglass now has a provider-specific parser for the current encoded response
envelope plus a header-aware decode path, but numeric decoding still depends on
capturing the provider headers that drive the site's decrypt flow.

## DuckDB: Needed or Not?

DuckDB is already part of this repository and already stores the core project
data.

The real question here is narrower:

- should the provider-comparison workflow stay file-based for now
- or should it also persist normalized comparison data into the existing DuckDB

At the current stage, a **new DuckDB-backed comparison layer is not required
yet**.

JSON files plus a normalized comparison report are enough while:

- the provider payload shapes are still being discovered
- the number of runs is low
- the main task is reverse engineering endpoints and units

The existing validation DuckDB becomes a good next step when you start doing
repeated, cross-run analysis such as:

- comparing many days of captures
- joining provider snapshots by rounded timestamp
- tracking drift in totals, peaks, and bucket counts over time
- building aggregate statistics across dozens or hundreds of runs

Recommended rule:

- Use JSON first for discovery and parser development
- Reuse the existing validation DuckDB after provider schemas are stable enough
  to justify a tabular model

When that threshold is reached, prefer adding comparison tables to the current
validation DuckDB instead of introducing a separate storage system.

## DuckDB Tables

When DuckDB persistence is enabled, the comparison workflow creates and updates
these tables in the validation DuckDB:

- `provider_comparison_runs`
- `provider_comparison_datasets`
- `provider_comparison_pairs`

These tables are intended for normalized provider-comparison metadata only.
They do not replace the raw JSON capture artifacts stored on disk.

## Recommended Next Steps

1. Capture one real CoinAnk run and one real Coinglass run for the same symbol
   and timeframe.
2. Re-run Coinglass capture with the updated manifest format so response
   headers are preserved for the decoder path.
3. Use `scripts/provider_comparison_sql_report.py` to watch drift across runs
   in `provider_comparison_*`.
