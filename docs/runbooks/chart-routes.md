# Chart Routes

This repo now has two canonical UI entrypoints for visual validation against Coinank.

## Canonical Routes

- Liquidation map:
  `http://localhost:8002/chart/derivatives/liq-map/<exchange>/<symbol>/<timeframe>`
- Liquidation heatmap:
  `http://localhost:8002/chart/derivatives/liq-heat-map/<symbol>/<timeframe>`

## Current Liq-Map Reference Matrix

These are the exact Coinank `liq-map` paths that define the current 1:1 validation scope:

- `https://coinank.com/chart/derivatives/liq-map/binance/btcusdt/1d`
- `https://coinank.com/chart/derivatives/liq-map/binance/btcusdt/1w`
- `https://coinank.com/chart/derivatives/liq-map/binance/ethusdt/1d`
- `https://coinank.com/chart/derivatives/liq-map/binance/ethusdt/1w`

These are the corresponding local mirror paths that must stay aligned:

- `http://localhost:8002/chart/derivatives/liq-map/binance/btcusdt/1d`
- `http://localhost:8002/chart/derivatives/liq-map/binance/btcusdt/1w`
- `http://localhost:8002/chart/derivatives/liq-map/binance/ethusdt/1d`
- `http://localhost:8002/chart/derivatives/liq-map/binance/ethusdt/1w`

Heatmap examples are documented separately and are not part of the active `liq-map` validation scope.

## Compatibility Aliases

These remain available, but should be considered legacy:

- `/coinglass`
- `/heatmap_30d.html`
- `/liq_map_1w.html`
- `/liq_map_1w/<symbol>`

## Timeframe Mapping

Only these chart timeframes are supported right now:

- `1d` -> `48h` (temporary closest internal window)
- `1w` -> `7d`

Any other chart timeframe should be treated as unsupported until the 1:1 validation baseline is stable.

## Validation Scripts

The visual validation scripts now target the canonical routes:

- `scripts/validate_liqmap_visual.py`
- `scripts/validate_heatmap_visual.py`

Use these routes for browser automation, screenshot diffing, and future 1:1 Coinank validation loops.
