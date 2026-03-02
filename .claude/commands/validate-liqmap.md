# /validate-liqmap - Liquidation Map Coinank Validation

Validate our liquidation map (`/liquidations/levels`) against Coinank `liq-map` for a single exchange.

## Usage
```bash
/validate-liqmap [--exchange binance] [--timeframe 1w] [--headed]
```

## Repo Context

- **Local BTC page**: `http://localhost:8002/chart/derivatives/liq-map/binance/btcusdt/1w`
- **Local ETH page**: `http://localhost:8002/chart/derivatives/liq-map/binance/ethusdt/1w`
- **Frontend source**: `frontend/liq_map_1w.html` (reads `?symbol=` from query string)
- **Pipeline script**: `scripts/validate_liqmap_visual.py`
- **Coinank reference dependency**: `scripts/coinank_screenshot.py --product map`
- **Output directory**: `data/validation/liqmap/`
- **Manifest directory**: `data/validation/manifests/`
- **API endpoint**: `/liquidations/levels?symbol=BTCUSDT&model=openinterest&timeframe=7`

## Coinank Reference URLs

- **BTC liq-map 1w**: `https://coinank.com/chart/derivatives/liq-map/binance/btcusdt/1w`
- **ETH liq-map 1w**: `https://coinank.com/chart/derivatives/liq-map/binance/ethusdt/1w`

## Orchestration Flow

1. Run the full pipeline with dotenvx credentials injection:
   ```bash
   dotenvx run -f /media/sam/1TB/.env -- \
     uv run python scripts/validate_liqmap_visual.py \
     --exchange binance --coin BTC --coinank-timeframe 1w
   ```
2. The script will:
   - start FastAPI if needed
   - preflight `/liquidations/levels`
   - capture local screenshot (`/liq_map_1w.html`)
   - capture Coinank screenshot (native download when logged in, crop fallback)
   - write manifest JSON under `data/validation/manifests/`
3. Pass the two output screenshots to `alpha-visual` for comparison

## Alpha-Visual Delegation Prompt

```text
Compare these two liquidation map screenshots:
- Local map (Plotly): <ours_screenshot_path>
- Coinank reference: <coinank_screenshot_path>

Task:
1. Verify both images show valid liquidation map charts (bars + cumulative lines).
2. Compare structure:
   - X axis: price levels (USD) should cover similar range around current BTC price
   - Y axis: liquidation volume bars stacked by leverage tier
   - Leverage tiers: 5x (cyan), 10x (blue), 25x (dark blue), 50x (orange), 100x (pink)
   - Cumulative lines: red (long, descending) and green (short, ascending)
   - Current price marker (vertical dashed line)
3. Compare magnitudes:
   - Volume order of magnitude (should both be in billions USD range)
   - Long vs short distribution asymmetry
   - Which leverage tiers dominate
4. Score similarity 0-100.
5. Output PASS if >= 70 and no obvious render failure, otherwise FAIL.
6. List the top 5 mismatches with short annotations.

Important:
- Our map uses Binance-only data. Coinank liq-map for binance should match closely.
- Volume magnitudes may differ due to different OI sources/timing.
- Score >= 70 means structure matches even if exact values differ.
```

## Validation Metrics

| Metric | Expected | Tolerance |
|--------|----------|-----------|
| Leverage tiers | 5 tiers (5x,10x,25x,50x,100x) | Exact match |
| Price range | Within 5% of current BTC price | +/- 10% |
| Volume magnitude | Billions USD | Same order of magnitude |
| Long/Short split | Both present | Ratio within 2x |
| Cumulative lines | Both present, correct direction | Visual match |

## Result Format

```markdown
### Liquidation Map Validation Result
- Exchange: binance
- Timeframe: 1w
- Local screenshot: `<ours_screenshot_path>`
- Coinank screenshot: `<coinank_screenshot_path>`
- Similarity score: 0-100
- Decision: PASS | FAIL
- Mismatches:
  1. ...
  2. ...
```

## Notes

- The `/liquidations/levels` endpoint is marked DEPRECATED but still functional.
- MMR is now computed per-bucket using official Binance tiers (not flat 0.4%).
- Leverage weights are parameterized (default: 15/30/25/20/10%).
- Coinank liq-map is per-exchange, making direct comparison with our Binance data possible.
- Credentials for Coinank login are read from `COINANK_USER` and `COINANK_PASSWORD` via dotenvx.
