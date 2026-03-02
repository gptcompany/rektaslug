# 📊 Visual Verification Guide - OpenInterest Model

This guide helps you verify that the OpenInterest liquidation model matches industry standards (Coinglass).

## Pre-requisites

✅ Server running: `http://localhost:8888`
✅ Cache table created: `volume_profile_daily` (7,345 rows)
✅ OpenInterest model functional (tested via API)

## Step 1: Open Frontend

```bash
# Open liquidation map in your browser
firefox http://localhost:8888/frontend/liquidation_map.html
# or
chromium http://localhost:8888/frontend/liquidation_map.html
```

## Step 2: Configure Settings

In the frontend interface:

1. **Model Selection**: Choose "Open Interest (recommended)" from dropdown
2. **Timeframe**: Select "30 day"
3. **Symbol**: BTCUSDT (default)
4. **Click**: "Load Levels" button

## Step 3: Wait for Data

Expected loading time: **~52 seconds**

You'll see a loading indicator while the model calculates liquidation levels from 30 days of historical data.

## Step 4: Visual Comparison

### Expected Metrics (Coinglass Reference)

| Metric | Expected Value | Our Implementation |
|--------|----------------|-------------------|
| **Long Volume** | ~2.5-2.7 GB | 2.66 GB ✅ |
| **Short Volume** | ~3.7-4.2 GB | 4.18 GB ✅ |
| **Total Volume** | ~6.2-6.8 GB | 6.84 GB ✅ |
| **Long Bins** | ~100-150 | 131 ✅ |
| **Short Bins** | ~150-200 | 152 ✅ |

### Visual Checklist

Compare your frontend visualization with Coinglass (https://www.coinglass.com/LiquidationData):

- [ ] **Long liquidations** (red/pink bars) appear **below** current price
- [ ] **Short liquidations** (green bars) appear **above** current price
- [ ] **Bar heights** roughly match relative volumes
- [ ] **Leverage tiers** (5x, 10x, 25x, 50x, 100x) visible in stacked bars
- [ ] **Current price line** clearly marked
- [ ] **Volume axis** shows billions (B) units
- [ ] **Price axis** shows reasonable spread (~$90k-$120k range)

## Step 5: Test Different Timeframes

Try switching between timeframes to verify dynamic bin sizing:

### 7-day Timeframe
```
Model: Open Interest
Timeframe: 7 day
Expected: Smaller bins ($200), less historical data, faster load (~20s)
```

### 90-day Timeframe
```
Model: Open Interest
Timeframe: 90 day
Expected: Larger bins ($1500), more historical data, slower load (~90s)
```

## Step 6: Compare Models

Switch between models to see the difference:

### OpenInterest vs Binance Standard

| Model | Long Volume | Short Volume | Total Volume | Realism |
|-------|-------------|--------------|--------------|---------|
| **OpenInterest** | 2.66 GB | 4.18 GB | 6.84 GB | ✅ Matches industry |
| **Binance Standard** | ~50-60 GB | ~50-60 GB | ~110-120 GB | ⚠️ 17x overestimate |

**Expected**: OpenInterest should show dramatically lower (more realistic) volumes.

## Common Issues

### Issue 1: "Error loading data"
**Solution**: Check server logs:
```bash
ps aux | grep uvicorn  # Verify server running
tail -50 /tmp/liquidationheatmap_server.log  # Check for errors
```

### Issue 2: "Very slow loading (>2 minutes)"
**Solution**: Cache may not be created. Verify:
```bash
uv run python -c "
from src.liquidationheatmap.ingestion.db_service import DuckDBService
with DuckDBService() as db:
    result = db.conn.execute('SELECT COUNT(*) FROM volume_profile_daily').fetchone()
    print(f'Cache rows: {result[0]:,}')
"
```

Expected output: `Cache rows: 7,345`

If cache missing, recreate it:
```bash
uv run python scripts/create_volume_profile_cache.py
```

### Issue 3: "Volumes don't match Coinglass"
**Troubleshooting**:
1. Verify current price is similar (within 5% is OK)
2. Check timeframe selection (should be 30 days)
3. Ensure model is "Open Interest" not "Binance Standard"
4. Compare percentage distribution rather than absolute values

## Success Criteria

✅ **Pass**: If 3+ of these conditions are met:

1. Total volume between 6-7.5 GB
2. Long/Short ratio roughly 1:1.5 to 1:2
3. Bin count within 20% of expected
4. Loading time < 90 seconds
5. Visual distribution similar to Coinglass

⚠️ **Investigate**: If volumes differ by >30% or loading takes >2 minutes

❌ **Fail**: If volumes are >10 GB (indicates Binance Standard model used incorrectly)

## Advanced Verification

### Check API Response Directly

```bash
# Get raw data for manual inspection
curl -s "http://localhost:8888/liquidations/levels?symbol=BTCUSDT&model=openinterest&timeframe=30" \
  > /tmp/oi_verification.json

# Analyze volumes
cat /tmp/oi_verification.json | jq '{
  model: .model,
  current_price: .current_price,
  long_bins: (.long_liquidations | length),
  short_bins: (.short_liquidations | length),
  long_volume_GB: ([.long_liquidations[].volume | tonumber] | add / 1e9),
  short_volume_GB: ([.short_liquidations[].volume | tonumber] | add / 1e9)
}'
```

Expected output:
```json
{
  "model": "openinterest",
  "current_price": "105439.7",
  "long_bins": 131,
  "short_bins": 152,
  "long_volume_GB": 2.66,
  "short_volume_GB": 4.18
}
```

## Reporting Issues

If visual verification fails, include:

1. **Frontend screenshot**
2. **API response** (from advanced verification command above)
3. **Server logs**: `tail -100 /tmp/liquidationheatmap_server.log`
4. **Cache status**: `SELECT COUNT(*) FROM volume_profile_daily`
5. **Data range**: Oldest/newest trade dates in your database

## Next Steps

After successful verification:

1. ✅ Setup cron job for daily cache updates (see `scripts/setup_cache_cronjob.sh`)
2. ✅ Update any documentation with screenshots
3. ✅ Consider implementing further optimizations if 52s is too slow (see `SESSION_REPORT_13NOV2025.md`)

---

Generated: 2025-11-13
Model Version: OpenInterest v1.0 (with persistent cache)
