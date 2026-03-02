# /dashboard-check - Full System Health Check

Validate FastAPI server, DuckDB connection, and heatmap visualization.

## Usage
```
/dashboard-check [--start-server] [--max-rounds 5]
```

## Repo Context

- **API**: `uv run uvicorn api.main:app --reload` (port 8000)
- **Endpoints**:
  - `/heatmap.html` - Plotly visualization (via static files)
  - `/health` - Health check endpoint
- **DB**: `/media/sam/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb`

## Orchestration Flow

```
┌──────────────────────────────────────────────────────────┐
│  INITIALIZATION                                          │
│  ─────────────────────────────────────────────────────── │
│  1. Check if server running (curl localhost:8002/health)  │
│  2. If --start-server: uv run uvicorn api.main:app       │
│  3. Verify DuckDB file exists and readable               │
└──────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────┐
│  COMPONENT CHECKS (parallel)                             │
│  ─────────────────────────────────────────────────────── │
│  API Health:                                             │
│    curl http://localhost:8002/health                     │
│                                                          │
│  Data Freshness:                                         │
│    Query DuckDB directly for latest timestamp            │
│                                                          │
│  Visual Render:                                          │
│    mcp__playwright__browser_navigate(/heatmap.html)      │
│    mcp__playwright__browser_take_screenshot()            │
└──────────────────────────────────────────────────────────┘
                          │
                          ▼
┌──────────────────────────────────────────────────────────┐
│  ITERATIVE FIX (if needed)                               │
│  ─────────────────────────────────────────────────────── │
│  ROUND N:                                                │
│    - Identify failed component                           │
│    - Attempt fix (restart server, regenerate cache)      │
│    - Re-validate                                         │
│    - Continue until all PASS or MAX_ROUNDS               │
└──────────────────────────────────────────────────────────┘
```

## Agent Delegation

```python
# API & Data check
Task(
    subagent_type="data-engineer",
    prompt="""
    Verify LiquidationHeatmap system health:

    1. Check API: curl http://localhost:8002/health
    2. Check DB: Test DuckDB connection at
       /media/sam/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb
    3. Query latest timestamp from DuckDB

    If server not running and --start-server:
       uv run uvicorn api.main:app --host 0.0.0.0 --port 8000
    """
)

# Visual validation
Task(
    subagent_type="alpha-visual",
    prompt="""
    Validate heatmap visualization health:

    MCP Tools:
    - mcp__playwright__browser_navigate
    - mcp__playwright__browser_take_screenshot
    - mcp__chrome-devtools__list_console_messages

    Check:
    1. http://localhost:8002/heatmap.html loads
    2. Plotly chart renders
    3. No JS errors in console
    4. Data populates (not empty/loading state)

    Iterate until PASS or max {max_rounds} rounds.
    """
)
```

## Output

```markdown
## Dashboard Health Report

### System Components
| Component | Status | Details |
|-----------|--------|---------|
| FastAPI Server | ✅ | Running on :8000 |
| DuckDB | ✅ | 2.3GB, 15M rows |
| API Health | ✅ | Response 200 in 45ms |

### Data Freshness
- Latest record: 2026-01-06 22:30:00
- Age: 3m 15s ✅

### Visual Validation (2 rounds)
| Round | Heatmap | Console | Score |
|-------|---------|---------|-------|
| 1 | Loading... | 0 errors | 60% |
| 2 | Rendered | 0 errors | 100% |

### Overall: HEALTHY
```
