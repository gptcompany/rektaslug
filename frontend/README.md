# Frontend Layout

Primary UI assets for current work:

- `coinglass_heatmap.html`: canonical heatmap UI, reached from `/chart/derivatives/liq-heat-map/...`
- `liq_map_1w.html`: canonical liq-map UI, reached from `/chart/derivatives/liq-map/...`

Compatibility wrappers kept in root:

- `heatmap.html`
- `heatmap_30d.html`
- `liquidation_map.html`
- `compare.html`
- `historical_liquidations.html`

Archived source files kept for historical context:

- `legacy/heatmap.html`
- `legacy/heatmap_30d.html`
- `legacy/liquidation_map.html`
- `legacy/compare.html`
- `legacy/historical_liquidations.html`

The root wrapper files exist only to prevent old URLs from silently executing outdated UI code. New automation and validation should target the canonical routes above.
