# /validate-heatmap - 30d Coinank Visual Validation

Validate the single-timeframe dashboard (`30d`) against Coinank (`1M`) using a reproducible screenshot pipeline.

## Usage
```bash
/validate-heatmap [--headed]
```

## Repo Context

- **Local page**: `http://localhost:8002/heatmap_30d.html`
- **Frontend source**: `frontend/heatmap_30d.html`
- **Pipeline script**: `scripts/validate_heatmap_visual.py`
- **Coinank reference script**: `scripts/coinank_screenshot.py`
- **Output directory**: `data/validation/`
- **API endpoint**: `/liquidations/heatmap-timeseries?symbol=BTCUSDT&time_window=30d&price_bin_size=500`

## Coinank Reference URLs

- **BTC heatmap 1w**: `https://coinank.com/chart/derivatives/liq-heat-map/btcusdt/1w`
- **ETH heatmap 1w**: `https://coinank.com/chart/derivatives/liq-heat-map/ethusdt/1w`

## Orchestration Flow

1. Run:
   `uv run python scripts/validate_heatmap_visual.py`
2. The script will:
   - start FastAPI if not already running
   - screenshot `http://localhost:8002/heatmap_30d.html`
   - screenshot Coinank `https://coinank.com/chart/derivatives/liq-heat-map/btcusdt/1M`
   - save both PNGs + manifest JSON in `data/validation/`
3. Pass the two screenshot paths to `alpha-visual` for comparison/scoring
4. Return PASS/FAIL with annotated differences

## Alpha-Visual Delegation Prompt

```text
Compare these two screenshots:
- Local heatmap (Plotly): <ours_screenshot_path>
- Coinank reference: <coinank_screenshot_path>

Task:
1. Verify both images are valid heatmap screenshots (not blank/error/loading overlays).
2. Compare overall structure:
   - X axis timeline coverage
   - Y axis price band layout
   - Heat intensity zones and major clusters
3. Score similarity 0-100.
4. Output PASS if >= 85 and no obvious render failure, otherwise FAIL.
5. List the top 3 mismatches with short annotations.

Important:
- The local page is a single-timeframe 30d view (BTCUSDT, bin=500).
- If local screenshot shows "No heatmap data" or error state, mark FAIL immediately.
```

## Expected Script Output

```text
ours_screenshot=data/validation/ours_heatmap_30d_<timestamp>.png
coinank_screenshot=data/validation/coinank_btc_1M_<timestamp>.png
manifest=data/validation/validation_manifest_<timestamp>.json
```

## Result Format

```markdown
### Validation Result
- Local screenshot: `data/validation/...`
- Coinank screenshot: `data/validation/...`
- Similarity score: 0-100
- Decision: PASS | FAIL
- Notes:
  - ...
```

## Notes

- If the script warns that the local API returned zero snapshots, the backend is reachable but the local DB has no data for the requested `30d` window. In that case the run is valid as a pipeline check, but visual validation should be marked `FAIL` (data unavailable).
