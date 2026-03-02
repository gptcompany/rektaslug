# Current Scope

This repository must be worked in strict sequence.

## Active Workstream

Current priority is **liq-map only**.

Within `liq-map`, the exchange is a **variant axis**:

- `binance`
- `bybit`
- `hyperliquid`

This means exchange selection must be treated as a parameter of the same product, not as a separate parallel project.

Operationally, the current baseline remains **Binance first** until the liq-map is aligned 1:1 with Coinank on the reference route. Only after that should work expand to the exchange variants.

Use these as the only primary references unless a task explicitly says otherwise:

- Canonical route:
  `http://localhost:8002/chart/derivatives/liq-map/<exchange>/<symbol>/<timeframe>`
- Primary frontend:
  `frontend/liq_map_1w.html`
- Primary validation script:
  `scripts/validate_liqmap_visual.py`
- Primary API payload:
  `/liquidations/levels`

Current reference matrix for the active `liq-map` workstream:

Coinank reference paths:

- `https://coinank.com/chart/derivatives/liq-map/binance/btcusdt/1d`
- `https://coinank.com/chart/derivatives/liq-map/binance/btcusdt/1w`
- `https://coinank.com/chart/derivatives/liq-map/binance/ethusdt/1d`
- `https://coinank.com/chart/derivatives/liq-map/binance/ethusdt/1w`

Local mirror paths:

- `http://localhost:8002/chart/derivatives/liq-map/binance/btcusdt/1d`
- `http://localhost:8002/chart/derivatives/liq-map/binance/btcusdt/1w`
- `http://localhost:8002/chart/derivatives/liq-map/binance/ethusdt/1d`
- `http://localhost:8002/chart/derivatives/liq-map/binance/ethusdt/1w`

The only active liq-map timeframes are:

- `1d`
- `1w`

## Deferred Workstream

`liq-heat-map` is explicitly **phase 2**.

Do not treat these as active implementation targets unless the task explicitly asks for heatmap work:

- `frontend/coinglass_heatmap.html`
- `scripts/validate_heatmap_visual.py`
- `/chart/derivatives/liq-heat-map/...`
- `/liquidations/heatmap-timeseries`

When heatmap work starts, the canonical route shape is:

- `http://localhost:8002/chart/derivatives/liq-heat-map/<symbol>/<timeframe>`

For heatmap, the active path axes are:

- `symbol` (for example `btcusdt`, `ethusdt`)
- `timeframe` (currently `1d`, `1w`)

There is no exchange segment in the canonical heatmap route.

## Legacy / Reference-Only

The following remain in the repo only as historical reference or compatibility surface:

- `frontend/heatmap.html`
- `frontend/heatmap_30d.html`
- `frontend/liquidation_map.html`
- `frontend/compare.html`
- `frontend/historical_liquidations.html`
- `/coinglass`
- `/heatmap_30d.html`
- `/liq_map_1w.html`

## Working Rule

When a new request is ambiguous, default to the active workstream above and ignore deferred or legacy references.

If a request mentions exchange variants without further detail:

- default to `binance`
- keep the work inside `liq-map`
- do not branch into `liq-heat-map`
