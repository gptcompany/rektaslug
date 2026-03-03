# LiquidationHeatmap Architecture

> **Note**: Canonical architecture source. Auto-updated by architecture-validator.
> Last generated: 2026-03-03

## Overview

LiquidationHeatmap is a cryptocurrency liquidation analysis system that calculates and visualizes Binance futures liquidation levels using historical trade data. The system processes billions of aggregate trades to identify liquidation zones and potential market reactions, helping traders understand leverage dynamics and position concentration.

**Core Functionality**:
- Calculate liquidation price levels using Binance's tiered margin system
- Process large-scale historical trade data (1.9B+ trades) using DuckDB
- Cluster liquidation zones using DBSCAN algorithm
- Provide real-time Open Interest integration via Binance API
- Visualize heatmaps showing liquidation density by price and time

**Design Philosophy**:
- **KISS** (Keep It Simple, Stupid): Boring technology wins - Python + DuckDB + FastAPI
- **YAGNI** (You Ain't Gonna Need It): Build for today's problems, not hypothetical futures
- **Code Reuse First**: Leverage py-liquidation-map formulas instead of reinventing algorithms
- **Test-Driven Development**: Red-Green-Refactor discipline enforced by TDD guard

## Tech Stack

| Component | Technology | Version | Justification |
|-----------|------------|---------|---------------|
| **Database** | DuckDB | 0.9.0+ | Zero-copy CSV ingestion, columnar analytics, no server overhead |
| **Backend API** | FastAPI | 0.104.0+ | Async support, auto-generated docs, Pydantic validation |
| **Data Processing** | Pandas + NumPy | 2.1.0+ / 1.24.0+ | Industry-standard for numerical analysis |
| **Clustering** | scikit-learn | 1.3.0+ | DBSCAN implementation for liquidation zone detection |
| **Caching** | Redis | 5.0.0+ | In-memory cache for real-time streaming (future use) |
| **Visualization** | Plotly.js | 5.17.0+ | Interactive charts, no build step required |
| **HTTP Server** | Uvicorn | 0.24.0+ | ASGI server with WebSocket support |
| **Package Manager** | UV | Latest | 100x faster than pip, deterministic lockfiles |
| **Testing** | Pytest | 7.4.0+ | Property-based testing with Hypothesis |
| **Code Quality** | Ruff | 0.1.0+ | Fast Python linter and formatter |
| **Type Checking** | MyPy | 1.7.0+ | Static type analysis |

## Project Structure

```
LiquidationHeatmap/
├── src/                              # Core application code
│   ├── liquidationheatmap/           # Main package
│   │   ├── models/                   # Liquidation calculation models
│   │   │   ├── base.py              # Abstract model interface
│   │   │   ├── binance_standard.py  # Official Binance formula
│   │   │   ├── funding_adjusted.py  # Funding rate adjustments
│   │   │   ├── ensemble.py          # Weighted model combination
│   │   │   └── position.py          # Position data structures
│   │   ├── ingestion/               # Data pipeline
│   │   │   ├── db_service.py        # DuckDB connection manager (singleton)
│   │   │   ├── csv_loader.py        # CSV loading utilities
│   │   │   ├── validators.py        # Data quality checks
│   │   │   └── aggtrades_streaming.py # Streaming ingestion
│   │   ├── api/                     # FastAPI application
│   │   │   ├── main.py              # API server with caching layer
│   │   │   └── heatmap_models.py    # Response models
│   │   └── utils/                   # Shared utilities
│   │       ├── logging_config.py    # Structured logging
│   │       └── retry.py             # Exponential backoff
│   ├── services/                     # Business logic services
│   │   ├── tier_loader.py           # YAML config loader
│   │   ├── tier_validator.py        # Tier configuration validation
│   │   ├── margin_calculator.py     # Maintenance margin calculations
│   │   ├── maintenance_calculator.py # MA offset derivation
│   │   └── funding/                 # Funding rate services
│   │       ├── funding_fetcher.py   # Binance API client
│   │       ├── bias_calculator.py   # Long/short bias from funding
│   │       └── complete_calculator.py # End-to-end calculator
│   ├── clustering/                   # DBSCAN clustering
│   │   ├── service.py               # Clustering service with cache
│   │   ├── models.py                # Cluster data structures
│   │   └── cache.py                 # Cluster result caching
│   ├── models/                       # Domain models
│   │   ├── tier_config.py           # Margin tier configuration
│   │   └── funding/                 # Funding rate models
│   ├── validation/                   # Data validation system
│   │   ├── alerts/                  # Alert generation
│   │   ├── reports/                 # Quality reports
│   │   └── visualization/           # Validation charts
│   ├── api/                         # Legacy API (deprecated)
│   ├── db/                          # Database utilities
│   └── config/                      # Application config
├── tests/                           # Test suite (pytest)
│   ├── integration/                 # Integration tests
│   │   ├── test_binance_accuracy.py # Model accuracy validation
│   │   ├── test_heatmap_api.py      # API endpoint tests
│   │   ├── test_whale_positions.py  # Large position handling
│   │   └── funding/                 # Funding rate tests
│   ├── test_ingestion/              # Data pipeline tests
│   │   ├── test_db_service.py       # DuckDB service tests
│   │   ├── test_csv_loader.py       # CSV loading tests
│   │   └── test_validators.py       # Validation tests
│   └── ui/                          # Frontend tests
├── scripts/                         # Command-line utilities
│   ├── ingest_aggtrades.py          # Historical trade ingestion
│   ├── validate_aggtrades.py        # Data quality validation
│   ├── check_ingestion_ready.py     # Pre-flight checks
│   ├── generate_heatmap_cache.py    # Cache pre-computation
│   ├── create_volume_profile_cache.py # Daily volume aggregation
│   ├── daily_ingestion.py           # Automated daily updates
│   ├── init_database.py             # Database initialization
│   └── calculate_liquidations.py    # Manual calculation runner
├── frontend/                        # Active UI + compatibility wrappers
│   ├── liq_map_1w.html              # Active liq-map renderer (1d/1w via query params)
│   ├── coinglass_heatmap.html       # Heatmap renderer (implemented, phase 2 scope)
│   ├── validation_dashboard.html    # Screenshot/review support UI
│   ├── styles.css                   # Shared CSS
│   └── legacy/                      # Archived legacy implementations
├── data/                            # Data directory (gitignored)
│   ├── raw/                         # Symlink to 3TB-WDC Binance CSV
│   └── cache/                       # Temporary cache
# External Database (NVMe - fast I/O):
#   Host: /media/sam/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb
#   N8N:  /workspace/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb
├── config/                          # Configuration files
│   ├── tiers/                       # YAML tier configurations
│   ├── alert_settings.yaml          # Alert thresholds
│   ├── bias_settings.yaml           # Funding bias config
│   └── validation_thresholds.yaml   # Data quality thresholds
├── docs/                            # Documentation
│   ├── ARCHITECTURE.md              # This file
│   ├── DATA_VALIDATION.md           # Validation guide
│   ├── PRODUCTION_CHECKLIST.md      # Deployment guide
│   ├── mathematical_foundation.md   # Margin math proofs
│   ├── model_accuracy.md            # Model validation
│   ├── api_guide.md                 # API reference
│   └── troubleshooting.md           # Common issues
├── .claude/                         # Claude Code configuration
│   ├── agents/                      # Specialized subagents
│   ├── skills/                      # Template-driven automation
│   └── tdd-guard/                   # TDD enforcement
├── CLAUDE.md                        # Development guide for Claude Code
├── README.md                        # Public documentation
└── pyproject.toml                   # UV workspace configuration
```

## Core Components

### 1. Data Ingestion Layer

**Purpose**: Efficiently load and validate large-scale Binance historical data.

**Key Files**:
- `src/liquidationheatmap/ingestion/db_service.py` - Singleton DuckDB connection manager to avoid reopening 185GB database
- `src/liquidationheatmap/ingestion/csv_loader.py` - Zero-copy CSV loading via DuckDB's `COPY FROM`
- `src/liquidationheatmap/ingestion/validators.py` - Data quality checks (duplicates, gaps, sanity)
- `scripts/ingest_aggtrades.py` - CLI for historical trade ingestion

**Performance**:
- 10GB CSV loaded in ~5 seconds (zero-copy)
- 1.9B trades → 7K row cache (99.9996% reduction)
- Streaming ingestion prevents OOM on large datasets

**Database Schema**:
```sql
-- Raw aggregate trades (COMPOSITE PRIMARY KEY for multi-symbol/exchange support)
-- agg_trade_id is unique only within a symbol - NOT globally unique!
-- This composite PK prevents the critical bug where ETHUSDT data was rejected
-- as duplicates because BTCUSDT had overlapping agg_trade_ids.
CREATE TABLE aggtrades_history (
    agg_trade_id BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    exchange VARCHAR(20) NOT NULL DEFAULT 'binance',
    price DECIMAL(18, 8) NOT NULL,
    quantity DECIMAL(18, 8) NOT NULL,
    side VARCHAR(4) NOT NULL,  -- 'buy' or 'sell'
    gross_value DOUBLE NOT NULL,
    PRIMARY KEY (agg_trade_id, symbol, exchange)
);
-- Indexes for efficient queries
CREATE INDEX idx_aggtrades_timestamp_symbol ON aggtrades_history(timestamp, symbol);
CREATE INDEX idx_aggtrades_timestamp ON aggtrades_history(timestamp);
CREATE INDEX idx_aggtrades_symbol ON aggtrades_history(symbol);
CREATE INDEX idx_aggtrades_exchange ON aggtrades_history(exchange);

-- Pre-aggregated volume profile (cache)
CREATE TABLE volume_profile_daily (
    date DATE,
    price_bin DECIMAL(18,2),
    total_volume DECIMAL(18,8),
    buy_volume DECIMAL(18,8),
    sell_volume DECIMAL(18,8)
);
```

### 2. Liquidation Calculation Models

**Purpose**: Calculate liquidation price levels using Binance's tiered margin system.

**Model Types**:

#### BinanceStandardModel (`src/liquidationheatmap/models/binance_standard.py`)
- Uses official Binance liquidation formulas
- Supports 10 maintenance margin rate (MMR) tiers
- Handles both synthetic binning and real trade data
- **Formulas** (corrected 2026-03-03, matches Binance official docs):
  - Long: `liq_price = entry × (1 - 1/leverage + mmr)`
  - Short: `liq_price = entry × (1 + 1/leverage - mmr)`
  - Note: MMR is applied directly, NOT divided by leverage. Previous `mmr/leverage` was incorrect.

#### FundingAdjustedModel (`src/liquidationheatmap/models/funding_adjusted.py`)
- Adjusts positions based on funding rate bias
- Shifts long/short distribution when funding is extreme
- Experimental model for sentiment-aware calculations

#### EnsembleModel (`src/liquidationheatmap/models/ensemble.py`)
- Weighted combination of multiple models
- Confidence-based averaging
- Provides robustness against individual model errors

**Tiered Margin System** (see `docs/mathematical_foundation.md`):
- Continuous margin requirements via Maintenance Amount (MA) offset
- 10 tiers for BTC/USDT (0-50k → 300M-500M notional)
- MA derived mathematically: `MA[i] = MA[i-1] + boundary[i] × (rate[i] - rate[i-1])`

### 3. Clustering Service

**Purpose**: Group liquidation levels into zones using DBSCAN algorithm.

**Key Files**:
- `src/clustering/service.py` - DBSCAN clustering with auto-tuning
- `src/clustering/cache.py` - LRU cache for cluster results (5-minute TTL)
- `src/clustering/models.py` - Cluster data structures (Pydantic models)

**Algorithm**:
- DBSCAN (Density-Based Spatial Clustering)
- Auto-tunes `eps` parameter using k-distance graph
- Volume-weighted cluster centroids
- Noise point separation for outliers

**Configuration**:
```python
ClusterParameters(
    eps=0.005,           # Price distance threshold (0.5%)
    min_samples=5,       # Minimum points per cluster
    metric='euclidean',  # Distance metric
    algorithm='auto'     # scikit-learn optimizer
)
```

### 4. API Layer

**Purpose**: Expose liquidation data via REST endpoints with caching.

**Key Files**:
- `src/liquidationheatmap/api/main.py` - FastAPI application with HeatmapCache
- `src/api/main.py` - Legacy API (margin calculations)
- `src/api/schemas/` - Pydantic request/response models

**Endpoints**:
```
GET  /health                                                    # API health check
GET  /liquidations/levels?symbol=X&model=openinterest&timeframe=N  # Liquidation levels (OI-based)
GET  /liquidations/history                                      # Historical liquidations
GET  /liquidations/heatmap                                      # Time×price heatmap data
POST /api/margin/calculate                                      # Margin calculation
GET  /api/margin/tiers/{symbol}                                 # Tier configuration
GET  /api/v1/prepare-for-ingestion                              # Pre-ingestion API teardown
GET  /api/v1/refresh-connections                                # Post-ingestion reconnect
```

**Coinank-Style Frontend Routes** (served directly, no redirect):
```
GET  /chart/derivatives/liq-map/{exchange}/{symbol}/{timeframe}   # Liq-map page (1d, 1w)
GET  /chart/derivatives/liq-heat-map/{symbol}/{timeframe}         # Heatmap page
GET  /liq_map_1w.html                                             # Legacy direct (backward compat)
```
- URL pattern mirrors Coinank: `/chart/derivatives/liq-map/binance/btcusdt/1w`
- Frontend JS parses exchange/symbol/timeframe from pathname OR query string
- `FileResponse` serves `frontend/liq_map_1w.html` directly (HTTP 200, no 307 redirect)

**Caching Strategy**:
- In-memory cache with 5-minute TTL
- Cache key: `symbol:start:end:interval:bin_size:weights`
- Evicts oldest entries at max size (100 entries)
- Logs hit/miss ratio for monitoring

**DuckDB Ingestion Lock**:
- File-based lock at `/tmp/duckdb-ingestion.lock` (single-writer enforcement)
- Auto-expires after 30 minutes to prevent stale locks from crashed ingestion jobs
- API returns `IngestionLockError` (HTTP 503) when lock is active
- Implementation: `DuckDBService.is_ingestion_locked()` in `db_service.py`

### 5. Funding Rate Service

**Purpose**: Fetch and process Binance funding rates for bias adjustments.

**Key Files**:
- `src/services/funding/funding_fetcher.py` - Binance API client with retry logic
- `src/services/funding/bias_calculator.py` - Calculate long/short bias from funding
- `src/services/funding/smoothing.py` - EMA smoothing for noise reduction
- `src/services/funding/complete_calculator.py` - End-to-end calculator

**Features**:
- Exponential backoff retry (max 3 attempts)
- Caching with configurable TTL
- EMA smoothing to reduce noise
- Bias thresholds: extreme (>0.1%), high (>0.05%), neutral

### 6. Validation System

**Purpose**: Ensure data quality and detect anomalies.

**Key Files**:
- `src/validation/alerts/` - Alert generation for threshold violations
- `src/validation/reports/` - Quality report generation
- `src/validation/trends/` - Time-series anomaly detection
- `scripts/validate_aggtrades.py` - CLI validation runner

**Validation Checks**:
- Row count and date range
- Duplicate detection (by agg_trade_id)
- Invalid values (negative prices, NULL fields)
- Temporal continuity (gap detection)
- Sanity checks (price ranges, volume limits)

### 7. Frontend Visualizations

**Purpose**: Interactive charts for liquidation analysis (no build step).

**Visualizations**:
1. **Liq-Map Renderer** (`frontend/liq_map_1w.html`) - Canonical liq-map chart, 1:1 Coinank visual match target
2. **Heatmap Renderer** (`frontend/coinglass_heatmap.html`) - Liq-heat-map surface (phase 2 scope)
3. **Compatibility Wrappers** - Legacy entrypoints for backward compatibility
4. **Archived Legacy UIs** (`frontend/legacy/`) - Historical reference, not active

**Liq-Map Visual Spec** (targeting 1:1 Coinank match):
- **Chart type**: Stacked vertical bars (Plotly.js 2.26.0)
- **3 leverage groups**: Low (#5B8FF9 blue), Medium (#B37FEB purple), High (#FF9C6E orange)
- **Bar opacity**: 0.6 (semi-transparent bars, solid legend swatches)
- **Cumulative lines**: Red/pink (long, descending L→R) + Green/cyan (short, ascending L→R)
- **Cumulative fill**: Semi-transparent area under each curve
- **Current price**: Red dashed vertical line + arrow + text label ("Current Price：XXXXX")
- **Y-axis**: Left = liquidation volume (M/B suffix), Right = cumulative scale
- **X-axis**: Plain numbers (no comma/$ separator), range slider at bottom
- **Background**: White (#ffffff), horizontal grid only
- **Legend**: 3 items centered above chart, no axis title labels

**URL Routing**:
- Served directly at `/chart/derivatives/liq-map/{exchange}/{symbol}/{timeframe}` (HTTP 200)
- Frontend parses exchange/symbol/timeframe from `window.location.pathname`
- Falls back to `window.location.search` query params for backward compat
- No 307 redirect (changed 2026-03-03)

**Tech Stack**:
- Plotly.js 2.26.0 for interactive charts
- Vanilla JavaScript (no framework)
- Fetch API for REST calls
- Coinank color scheme: Low=#5B8FF9, Medium=#B37FEB, High=#FF9C6E

## Data Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  Binance Historical CSV (3TB-WDC)        Binance REST API               │
│  ┌───────────────────────┐               ┌──────────────────┐           │
│  │ trades/               │               │ Open Interest    │           │
│  │ bookDepth/            │               │ Funding Rate     │           │
│  │ fundingRate/          │               │ Ticker Price     │           │
│  │ metrics/ (OI)         │               └──────────────────┘           │
│  └───────────────────────┘                        │                     │
│           │                                        │                     │
└───────────┼────────────────────────────────────────┼─────────────────────┘
            │                                        │
            ▼                                        ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    INGESTION LAYER (DuckDB)                              │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌────────────────────┐       ┌─────────────────────┐                   │
│  │ CSV Loader         │       │ API Fetcher         │                   │
│  │ - Zero-copy COPY   │       │ - Retry logic       │                   │
│  │ - Streaming        │       │ - Cache manager     │                   │
│  └────────┬───────────┘       └──────────┬──────────┘                   │
│           │                              │                               │
│           ▼                              ▼                               │
│  ┌───────────────────────────────────────────────────┐                  │
│  │         DuckDB (liquidations.duckdb)              │                  │
│  │  ┌──────────────────┐  ┌────────────────────┐    │                  │
│  │  │ aggtrades        │  │ volume_profile_    │    │                  │
│  │  │ (1.9B rows)      │  │ daily (7K rows)    │    │                  │
│  │  └──────────────────┘  └────────────────────┘    │                  │
│  └───────────────────────────────────────────────────┘                  │
│           │                                                               │
└───────────┼───────────────────────────────────────────────────────────────┘
            │
            ▼
┌─────────────────────────────────────────────────────────────────────────┐
│              CALCULATION LAYER (FastAPI + Services)                      │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌────────────────────────────────────────────────────────────┐          │
│  │                 Liquidation Models                         │          │
│  │  ┌──────────────┐  ┌───────────────┐  ┌──────────────┐   │          │
│  │  │ Binance      │  │ Funding       │  │ Ensemble     │   │          │
│  │  │ Standard     │  │ Adjusted      │  │ Model        │   │          │
│  │  └──────┬───────┘  └───────┬───────┘  └──────┬───────┘   │          │
│  └─────────┼──────────────────┼──────────────────┼───────────┘          │
│            │                  │                  │                       │
│            └──────────────────┼──────────────────┘                       │
│                               ▼                                          │
│  ┌─────────────────────────────────────────────────────────┐            │
│  │            Clustering Service (DBSCAN)                  │            │
│  │  - Auto-tune eps parameter                              │            │
│  │  - Volume-weighted centroids                            │            │
│  │  - LRU cache (5-min TTL)                                │            │
│  └─────────────────────────┬───────────────────────────────┘            │
│                            ▼                                             │
│  ┌─────────────────────────────────────────────────────────┐            │
│  │         FastAPI Endpoints + HeatmapCache                │            │
│  │  - /liquidations/levels (with OI integration)           │            │
│  │  - /liquidations/history (time-series)                  │            │
│  │  - /liquidations/heatmap (clustered zones)              │            │
│  └─────────────────────────┬───────────────────────────────┘            │
│                            │                                             │
└────────────────────────────┼─────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   PRESENTATION LAYER (Plotly.js)                         │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                           │
│  ┌──────────────┐  ┌──────────────┐  ┌─────────────────────┐            │
│  │ Liquidation  │  │ Heatmap      │  │ Historical          │            │
│  │ Map          │  │ (time×price) │  │ Liquidations        │            │
│  │ (bar chart)  │  │ (density)    │  │ (time-series)       │            │
│  └──────────────┘  └──────────────┘  └─────────────────────┘            │
│                                                                           │
│  Interactive controls: timeframe, model selection, leverage filters      │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘
```

**Flow Summary**:
1. **Ingestion**: Historical CSV → DuckDB (zero-copy) + Binance API → in-memory cache
2. **Calculation**: DuckDB query → Liquidation models → Clustering → API response
3. **Presentation**: REST API → Plotly.js → Interactive charts

**Cache Strategy**:
- **Daily cache**: `volume_profile_daily` table (updated 00:05 UTC via cron)
- **API cache**: In-memory HeatmapCache (5-min TTL, 100 entry limit)
- **Cluster cache**: LRU cache in ClusteringService (5-min TTL)

## Ingestion Schedule

Two systemd timers manage data freshness:

### 1. CCXT Gap Fill (every 5 minutes)

Near-real-time bridge from ccxt-data-pipeline Parquet catalog into DuckDB (T-2 → T-0).

| Item | Value |
|------|-------|
| **Timer** | `scripts/systemd/lh-ccxt-gap-fill.timer` |
| **Service** | `scripts/systemd/lh-ccxt-gap-fill.service` |
| **Script** | `scripts/run-ccxt-gap-fill.sh` |
| **Schedule** | `OnCalendar=*:0/5` (every 5 minutes) |
| **Jitter** | `RandomizedDelaySec=30` |
| **Timeout** | 300 seconds |
| **Data source** | ccxt-data-pipeline Parquet files (`$CCXT_CATALOG`) |
| **Symbols** | `$SYMBOLS` (default: BTCUSDT ETHUSDT) |

**Flow**:
1. `POST /api/v1/prepare-for-ingestion` (API releases DuckDB read lock)
2. `cleanup_duckdb_locks.py` (clear stale WAL/lock files)
3. Test DuckDB write access (skip if busy)
4. `fill_gap_from_ccxt.py` (ingest latest Parquet into DuckDB)
5. `POST /api/v1/refresh-connections` (API reconnects read-only)

**Alternative**: `scripts/run-gapfill-daemon.sh` runs as continuous loop with `REKTSLUG_GAP_FILL_INTERVAL_SECONDS` (default: 300s).

### 2. Daily Historical Ingestion (03:30 UTC)

Bulk ingestion of full Binance historical CSV data.

| Item | Value |
|------|-------|
| **Timer** | `scripts/systemd/lh-ingestion.timer` |
| **Service** | `scripts/systemd/lh-ingestion.service` |
| **Script** | `scripts/run-ingestion.sh` |
| **Schedule** | Daily 03:30 UTC (30 min after binance-sync) |
| **Jitter** | `RandomizedDelaySec=300` (±5 min) |
| **Timeout** | 1 hour |
| **Data types** | aggTrades, fundingRate, Open Interest, klines (5m, 15m), metrics |

## Key Technical Decisions

### 1. DuckDB for Analytics Storage

**Decision**: Use DuckDB instead of PostgreSQL/MySQL.

**Rationale**:
- Zero-copy CSV ingestion (10GB in ~5 seconds)
- Columnar storage optimized for analytics queries
- No server required (embedded database)
- Excellent for read-heavy workloads (liquidation analysis)
- Native Parquet support for future optimization

**Trade-offs**:
- Limited concurrent write support (not an issue - batch ingestion only)
- Not suitable for OLTP (we only need OLAP)

### 2. Singleton DuckDBService

**Decision**: Use singleton pattern per database path.

**Rationale**:
- Avoid reopening 185GB database on every request (HDD I/O bottleneck)
- Thread-safe with lock for concurrent singleton creation
- Health-checked connection with auto-reconnect
- Per-path singletons allow tests to use separate databases

**Implementation**:
```python
# Singletons keyed by (resolved_path, read_only)
_instances: dict[tuple[str, bool], "DuckDBService"] = {}
```

### 3. Pre-aggregated Volume Profile

**Decision**: Create `volume_profile_daily` cache table (99.9996% reduction).

**Rationale**:
- Reduces query time from 52s to <1s for 30-day analysis
- Allows real-time API responses without database scan
- Daily cron job updates cache (no manual intervention)
- Minimal storage cost (~500KB for 7K rows)

**Trade-off**:
- Requires daily maintenance (automated via cron)
- Cache can be stale (max 24 hours old)

### 4. Tiered Margin System with MA Offset

**Decision**: Use Binance's exact formula with Maintenance Amount offset.

**Rationale**:
- Ensures mathematical continuity at tier boundaries (no sudden jumps)
- Matches Binance's official liquidation behavior (95% accuracy)
- Well-documented in academic literature (see `docs/mathematical_foundation.md`)

**Formula**:
```
Margin = N × rate[i] - MA[i]
MA[i] = MA[i-1] + boundary[i] × (rate[i] - rate[i-1])
```

### 5. DBSCAN Clustering for Liquidation Zones

**Decision**: Use DBSCAN instead of K-means or hierarchical clustering.

**Rationale**:
- Discovers arbitrary-shaped clusters (liquidation zones aren't always spherical)
- Handles noise points (outlier positions)
- No need to specify number of clusters in advance
- Density-based approach matches liquidation clustering behavior

**Trade-off**:
- Sensitive to `eps` parameter (mitigated by auto-tuning via k-distance graph)
- O(n log n) complexity (acceptable for <10K points per query)

### 6. FastAPI with In-Memory Cache

**Decision**: Use in-memory HeatmapCache instead of Redis for API caching.

**Rationale**:
- Simpler deployment (no Redis server required)
- Sufficient for single-server deployment
- 5-minute TTL balances freshness vs. performance
- LRU eviction prevents memory bloat

**When to migrate to Redis**:
- Multi-server deployment (shared cache)
- Cache size exceeds RAM limits
- Need for cache persistence across restarts

### 7. No Build Step for Frontend

**Decision**: Use vanilla JavaScript + Plotly.js instead of React/Vue.

**Rationale**:
- Faster development (no webpack/vite configuration)
- Easier debugging (no source maps)
- Smaller bundle size (no framework overhead)
- Matches UTXOracle pattern (proven simplicity)

**Trade-off**:
- Less suitable for complex UIs (acceptable for visualization-focused app)
- Manual DOM manipulation (mitigated by Plotly.js abstractions)

### 8. UV Package Manager

**Decision**: Use UV instead of pip/poetry.

**Rationale**:
- 100x faster dependency resolution
- Deterministic lockfiles (reproducible builds)
- Compatible with pyproject.toml standard
- Active development by Astral (Ruff creators)

**Migration**: Existing `pip install` → `uv sync`

## Configuration

### Environment Variables

Create `.env` from `.env.example`.

Primary runtime settings are centralized in `src/liquidationheatmap/settings.py`, while shell wrappers share the same contract through `scripts/lib/runtime_env.sh`.

```bash
# Core runtime
HEATMAP_HOST=0.0.0.0
HEATMAP_PORT=8002
HEATMAP_API_URL=http://localhost:8002
HEATMAP_PROJECT_ROOT=/media/sam/1TB/rektaslug
HEATMAP_DB_PATH=/media/sam/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb
HEATMAP_DATA_DIR=/media/sam/3TB-WDC/binance-history-data-downloader/data

# Optional container overrides
HEATMAP_CONTAINER_PROJECT_ROOT=/workspace/1TB/LiquidationHeatmap
HEATMAP_CONTAINER_DB_PATH=/workspace/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb
HEATMAP_CONTAINER_DATA_DIR=/workspace/3TB-WDC/binance-history-data-downloader/data
HEATMAP_CONTAINER_API_URL=http://host.docker.internal:8002
```

### YAML Configuration Files

#### Tier Configuration (`config/tiers/BTCUSDT.yaml`)
```yaml
symbol: BTCUSDT
version: binance-2025-v1
tiers:
  - tier_number: 1
    min_notional: 0
    max_notional: 50000
    margin_rate: 0.005
    max_leverage: 200
```

#### Alert Settings (`config/alert_settings.yaml`)
```yaml
thresholds:
  price_deviation_pct: 5.0
  volume_spike_multiplier: 3.0
  gap_hours: 6
```

#### Bias Settings (`config/bias_settings.yaml`)
```yaml
funding_thresholds:
  extreme: 0.001  # 0.1%
  high: 0.0005    # 0.05%
  neutral: 0.0002 # 0.02%
```

### Database Initialization

Run setup script:
```bash
# Initialize database schema
uv run python scripts/init_database.py

# Ingest historical data
uv run python scripts/ingest_aggtrades.py \
    --symbol BTCUSDT \
    --start-date 2025-01-01 \
    --end-date 2025-01-31 \
    --db /media/sam/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb \
    --data-dir /media/sam/3TB-WDC/binance-history-data-downloader/data

# Create volume profile cache
uv run python scripts/create_volume_profile_cache.py

# Validate data quality
uv run python scripts/validate_aggtrades.py
```

### Cache Maintenance

Setup daily cache updates:
```bash
# Automated setup (recommended)
bash scripts/setup_cache_cronjob.sh

# Manual cron entry (runs daily at 00:05 UTC)
5 0 * * * cd /path/to/LiquidationHeatmap && uv run python scripts/create_volume_profile_cache.py
```

### API Server

Run FastAPI server:
```bash
# Development (auto-reload)
uv run uvicorn liquidationheatmap.api.main:app --reload --port 8000

# Production (with workers)
uv run uvicorn liquidationheatmap.api.main:app --workers 4 --port 8000
```

### Testing

Run test suite:
```bash
# All tests
uv run pytest

# With coverage
uv run pytest --cov=src --cov-report=html

# Specific test
uv run pytest tests/integration/test_binance_accuracy.py -v

# Property-based tests (Hypothesis)
uv run pytest tests/ -v --hypothesis-show-statistics
```

**Test Coverage**: Target 80%+ (enforced by TDD guard)

### Logging

Structured logging to `logs/liquidationheatmap.log`:
```python
import logging
logger = logging.getLogger(__name__)
logger.info("Processing liquidations", extra={
    "symbol": "BTCUSDT",
    "model": "binance_standard",
    "timeframe_days": 30
})
```

**Log Rotation**: Configure via `src/liquidationheatmap/utils/logging_config.py`

---

## Related Documentation

- **Development Guide**: See `CLAUDE.md`
- **API Reference**: See `docs/api_guide.md`
- **Data Validation**: See `docs/DATA_VALIDATION.md`
- **Mathematical Foundation**: See `docs/mathematical_foundation.md`
- **Production Checklist**: See `docs/PRODUCTION_CHECKLIST.md`
- **Model Accuracy**: See `docs/model_accuracy.md`

---

**Maintained by**: Claude Code architecture-validator
**Last Updated**: 2026-03-03
**Version**: 1.2 (formula fix, ingestion schedule, Coinank-style routing)
