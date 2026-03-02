# /refresh-heatmap - Regenerate Heatmap Cache

Run `scripts/generate_heatmap_cache.py` and optionally validate output.

## Usage
```
/refresh-heatmap [--symbol BTCUSDT] [--days 7] [--validate]
```

## Repo Context

- **Script**: `scripts/generate_heatmap_cache.py`
- **DB**: `/media/sam/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb`
- **Cache output**: `data/cache/`
- **Logs**: `logs/heatmap_cache.log`

## Pipeline Stages

```
STAGE 1: GENERATE CACHE
  │
  ├─► uv run python scripts/generate_heatmap_cache.py \
  │       --symbol {symbol} --days {days}
  ├─► Check exit code
  └─► Parse log for row counts
          │
          ▼ (if --validate)
STAGE 2: VISUAL VALIDATION
  │
  ├─► mcp__playwright__browser_navigate(localhost:8002/heatmap.html)
  ├─► mcp__playwright__browser_take_screenshot()
  ├─► Verify new data visible in heatmap
  └─► PASS | FAIL → retry
```

## Agent Delegation

```python
# Stage 1: Data refresh
Task(
    subagent_type="data-engineer",
    prompt="""
    Run heatmap cache generation:

    uv run python scripts/generate_heatmap_cache.py \\
        --symbol {symbol} --days {days}

    Verify:
    - Exit code 0
    - Log shows rows processed
    - Cache file updated in data/cache/
    """
)

# Stage 2: Visual validation (if --validate)
Task(
    subagent_type="alpha-visual",
    prompt="""
    Validate refreshed heatmap at http://localhost:8002/heatmap.html

    Check that new data is visible and no stale indicators.
    """
)
```

## Output

```markdown
### Cache Generation
- Symbol: BTCUSDT
- Days: 7
- Rows processed: 15,432
- Cache file: data/cache/heatmap_BTCUSDT.parquet
- Status: ✅

### Validation (optional)
- Heatmap renders: ✅
- New data visible: ✅
```
