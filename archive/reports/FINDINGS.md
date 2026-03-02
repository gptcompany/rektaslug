# DuckDB aggTrades Loading - Root Cause Analysis

## Problem
`get_large_trades()` returns 0 rows despite CSV files existing with large trades.

## Root Cause Found ✅

### Issue: Inconsistent CSV Headers
- **Old files (2020-2024)**: NO header row, just data
- **New files (2025)**: WITH header row (`agg_trade_id,price,quantity,...`)

### DuckDB Behavior
When glob pattern `*-aggTrades-*.csv` matches BOTH old and new files:
- DuckDB detects inconsistent schemas
- Falls back to generic column names: `column0`, `column1`, `column2`, etc.
- SQL query using named columns (`price`, `quantity`) FAILS with "column not found"

### Evidence
```bash
# Old file (2020-01-01) - NO HEADER
$ head -1 /media/sam/3TB-WDC/.../BTCUSDT-aggTrades-2020-01-01.csv
18374167,7189.43,0.030,25247504,25247504,1577836801481,true

# New file (2025-10-28) - WITH HEADER
$ head -1 /media/sam/3TB-WDC/.../BTCUSDT-aggTrades-2025-10-28.csv
agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker
```

## Solutions

### Option 1: Use Only Recent Files (RECOMMENDED)
**Reason**: For liquidation map, only need recent data (last 30 days)

```python
# Current (BROKEN - scans 2128 files)
csv_path = f".../{symbol}-aggTrades-*.csv"

# Fix (28 files, 14 seconds)
csv_path = f".../{symbol}-aggTrades-2025-10-*.csv"
```

**Benefits**:
- ✅ Fast (14s vs >1 hour)
- ✅ Named columns work
- ✅ Recent data more relevant for liquidations
- ✅ No schema conflict

### Option 2: Use Generic Column Names
```sql
SELECT
    column1::DOUBLE as price,
    column2::DOUBLE as quantity,
    (column1::DOUBLE * column2::DOUBLE) as gross_value
FROM read_csv_auto('*-aggTrades-*.csv')
```

**Issues**:
- ❌ Slow (scans all 2128 files from 2020-2025)
- ❌ Fragile (column order must not change)
- ❌ Less readable

## Test Results ✅

### Successful Query (October 2025 only):
```
✅ Success in 14.10s! Rows: 10
                timestamp     price  quantity  side   gross_value
0 2025-10-01 00:00:01.145  113988.8     3.187   buy  3.632823e+05
1 2025-10-01 00:00:01.677  114000.0     2.639   buy  3.008460e+05
...
```

**Note**: Both BUY and SELL trades present → asymmetric distribution will work!

## Recommended Next Steps

1. **Update `get_large_trades()` to use October 2025 files only**
   - Change glob pattern from `*-*.csv` to `*-2025-10-*.csv`
   - Or better: dynamically construct based on `timeframe` parameter

2. **Make timeframe dynamic** (future enhancement)
   ```python
   from datetime import datetime, timedelta

   end_date = datetime.now()
   start_date = end_date - timedelta(days=timeframe)

   # Generate glob for specific month/year range
   csv_pattern = f"{symbol}-aggTrades-{start_date:%Y-%m}-*.csv"
   ```

3. **Test asymmetric distribution**
   - Once loading works, verify dashboard shows realistic buy/sell imbalance

## Related Files
- `src/liquidationheatmap/ingestion/db_service.py:222` - csv_path construction
- `tests/test_ingestion/test_db_service.py:65` - test (currently passes but doesn't verify data)
