# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

**LiquidationHeatmap** calculates and visualizes cryptocurrency liquidation levels from Binance futures historical data. Uses DuckDB for fast CSV analytics, FastAPI for REST endpoints, and Redis pub/sub for real-time streaming. Leverages open-source models (py-liquidation-map) instead of reinventing algorithms.

**Key Principles**:
- KISS (Keep It Simple, Stupid) - Boring technology wins
- YAGNI (You Ain't Gonna Need It) - Build for today, not hypothetical futures
- Code Reuse First - Don't reinvent the wheel if >80% exists (py-liquidation-map formulas)
- Test-Driven Development - Red-Green-Refactor discipline

**🎯 Development Philosophy**:
→ The best code is no code. The second best is deleted code. The third best is simple code.

**Data Sources**:
- Binance historical CSV (3TB-WDC): trades, bookDepth, fundingRate, metrics (Open Interest)
- Real-time WebSocket (future): Binance Futures liquidation stream
- Target: BTC/USDT initially, expandable to other pairs

---

## Architecture Documentation

> **Canonical Source**: [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
>
> `docs/ARCHITECTURE.md` is the single source of truth for:
> - System architecture and component diagrams
> - Data flow and pipeline documentation
> - Technical decisions and rationale
>
> **Auto-validated** by architecture-validator hook on each commit.
> Below is a summary - see `docs/ARCHITECTURE.md` for complete details.

**3-Layer Design** (KISS approach - no custom binary parsing):

### Layer 1: Data Ingestion (DuckDB)
- **Raw Storage**: `data/raw/` (symlinked to Binance CSV on 3TB-WDC - read-only)
- **Processed Storage**: `/media/sam/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb` (NVMe for fast I/O)
- **N8N Container Path**: `/workspace/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb`
- **Pipeline**: Zero-copy CSV ingestion via `COPY FROM` (10GB in ~5 seconds)
- **Schema**: aggtrades_history, klines_5m_history, klines_1m_history, open_interest_history, funding_rate_history, heatmap_cache
- **Responsible Agent**: `data-engineer`

### Layer 2: Calculation & API (FastAPI + Redis)
- **Liquidation Models**: Binance-specific formulas (leverage py-liquidation-map)
  - Long liquidation: `entry_price * (1 - 1/leverage + maintenance_margin/leverage)`
  - Short liquidation: `entry_price * (1 + 1/leverage - maintenance_margin/leverage)`
- **Heatmap Generation**: Clustering algorithm from Open Interest + funding rates
- **REST API**: FastAPI endpoints from `src/liquidationheatmap/api/main.py`
- **Streaming**: Redis pub/sub for real-time liquidation events (Nautilus pattern)
- **Responsible Agent**: `quant-analyst`

### Layer 3: Visualization (Plotly.js)
- **Heatmap**: Plotly.js browser rendering for liq-map and liq-heat-map views
- **Frontend**: Active pages in `frontend/` with archived legacy implementations in `frontend/legacy/`
- **API Client**: Fetch API for REST endpoints
- **Real-time**: WebSocket client for Redis pub/sub (future)

**Data Flow**:
```
Binance CSV (3TB-WDC)
  → DuckDB ingestion (scripts/ingest_historical.py)
  → Liquidation calculation (src/models/liquidation.py)
  → Heatmap clustering (src/models/heatmap.py)
  → REST API (src/liquidationheatmap/api/main.py)
  → Canonical chart routes (/chart/derivatives/...)
```

### Tech Stack

| Layer | Technology | Justification |
|-------|------------|---------------|
| **Data Storage** | DuckDB | Zero-copy CSV, fast analytics, no server |
| **API Server** | FastAPI | Async, auto-docs, Pydantic integration |
| **Frontend** | Plotly.js | Lightweight, interactive visualizations |
| **Dependency Mgmt** | UV | 100x faster than pip, deterministic lockfiles |
| **Testing** | Pytest + TDD guard | Enforce test-first workflow |
| **Code Navigation** | Serena MCP | Fast codebase search |
| **Task Management** | SpecKit | Structured feature planning |

---

## Repository Organization

```
LiquidationHeatmap/
├── archive/               # Archived reports, notes, legacy config templates
├── src/                   # Core application code
├── tests/                 # Test suite (pytest)
├── scripts/               # Utilities and batch jobs
├── data/
│   ├── raw/               # Symlink to 3TB-WDC Binance CSV (read-only)
│   └── cache/             # Temporary cache (gitignored)
# Database: /media/sam/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb (external, NVMe)
├── frontend/              # Active UI pages + compatibility wrappers
│   └── legacy/            # Archived frontend implementations
├── .claude/               # Claude Code configuration
│   ├── agents/            # Specialized subagents
│   ├── skills/            # Template-driven automation
│   ├── commands/          # SpecKit slash commands
│   ├── prompts/           # Orchestration rules
│   └── tdd-guard/         # TDD enforcement
├── .specify/              # Task management
├── CLAUDE.md              # THIS FILE
├── README.md              # Public documentation
├── pyproject.toml         # UV workspace root
└── .gitignore             # Ignore patterns
```

### File Placement Conventions

**New backend code** → `src/`
**New frontend code** → `frontend/`
**Scripts/jobs** → `scripts/`
**Tests** → `tests/test_<module>.py`
**Documentation** → `README.md` (public) or `.claude/docs/` (meta)
**Agent specs** → `.claude/agents/`
**Skills** → `.claude/skills/`

### Immutable Patterns

- **Database location**: `/media/sam/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb` (NVMe, external)
- **N8N container path**: `/workspace/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb`
- **Never commit** `.env` (secrets)
- **Always use** `uv` for dependency management (not `pip`)
- **Always test** before committing (`uv run pytest`)

### Resource Safety (MANDATORY)

Il PC è spesso sotto stress (DuckDB su 6B+ righe, multi-agent, build parallele).
**Operazioni cautelative obbligatorie:**

- **MAI** lanciare query su aggtrades_history (6B righe) senza `WHERE` temporale o batch per anno
- **MAI** eseguire più ingestioni DuckDB in parallelo (single-writer lock)
- **SEMPRE** processare dati in batch incrementali (per anno, per settimana, per mese)
- **SEMPRE** aggiungere throttling (`time.sleep()`) tra batch pesanti
- **SEMPRE** verificare che nessun processo tenga il DuckDB lock prima di scrivere:
  ```bash
  lsof /media/sam/2TB-NVMe/liquidationheatmap_db/liquidations.duckdb 2>/dev/null
  ```
- **Volume profile cache**: usare `CREATE TABLE ... AS SELECT ... WHERE year=X` anno per anno, MAI full scan
- **Ingestioni CSV**: sequenziali, una alla volta, con I/O throttle (100ms minimo tra file)
- **Container memory**: il limite è 2GB (`docker-compose.yml`). DuckDB WAL checkpoint su un DB da 440GB richiede ~1GB+ RSS. Con 1GB di limite il kernel OOM killer termina silenziosamente il processo.
- **DuckDB write connections**: SEMPRE impostare `SET memory_limit='1GB'` per evitare che il WAL checkpoint esaurisca la RAM del container. Non fare warmup eager (apertura read-only) dopo gap-fill: il sync script chiama `/api/v1/refresh-connections`.

---

## 🛡️ Robustness & Error Handling

### DuckDB Lock & Fallback (rektaslug)
- **Lock Contention**: Il sistema gestisce i lock DuckDB tramite file (`.ingestion_lock`). Se occupato, l'API restituisce `503 Service Unavailable` con `Retry-After`.
- **Shell Fallback**: Gli script `run-ingestion.sh` e `run-ccxt-gap-fill.sh` riprovano l'accesso e passano alla CLI (`uv run`) se l'API è offline (404/000).
- **Non-fatal Skip**: Errori di lock durante i cicli di 5 minuti sono trattati come non fatali (exit 0, prefisso `SKIPPED_LOCK_CONTENTION:`) per evitare falsi allarmi.
- **Lifespan Hook**: All'avvio, il lifespan hook di FastAPI rimuove lock file stale (`/tmp/duckdb-ingestion.lock`) lasciati da crash precedenti.
- **Gap-fill timeout**: Il curl per `/api/v1/gap-fill` ha timeout 120s (il gap-fill impiega ~31s con cache warm a causa del metadata loading di DuckDB su 440GB).

### Parquet Storage (ccxt-data-pipeline)
- **Corruption Resilience**: La lettura dei file Parquet è isolata; file corrotti vengono loggati e saltati senza interrompere l'intera pipeline.
- **Schema Evolution**: Supporta l'unione di file con colonne aggiuntive tramite `promote_options="permissive"` in PyArrow.
- **Compatibility**: Il codice è mantenuto compatibile con **Python 3.10** (Workstation reference) gestendo manualmente `datetime.UTC` e `StrEnum`.

---

## 🔧 Development Workflow

### 🎯 KISS & YAGNI Blueprint (ALWAYS REMEMBER!)

#### **KISS** - Keep It Simple, Stupid
- **Choose boring technology**: Python, not Rust (until needed)
- **Avoid premature optimization**: Make it work, then make it fast
- **One module, one purpose**: Each file does ONE thing well
- **Minimize dependencies**: Every dependency is technical debt
- **Clear over clever**: Code that a junior can understand beats "smart" code

#### **YAGNI** - You Ain't Gonna Need It
- **Don't build for hypothetical futures**: Solve TODAY's problem
- **No unused abstractions**: 3 similar things ≠ need for abstraction yet
- **Delete dead code**: If unused for 2 weeks, remove it
- **Resist feature creep**: Solve the core problem first

#### **Code Reuse First** - Don't Reinvent the Wheel
- **NEVER write custom code if >80% can be reused**: Use existing libraries
- **Analyze before automating**: Verify existing solutions don't already solve it
- **Leverage open source**: Battle-tested code > custom implementation

---

## 🔧 Development Workflow

### TDD Implementation Flow

**Red-Green-Refactor** (enforced by TDD guard):

1. **🔴 RED**: Write failing test first
   ```bash
   uv run pytest tests/test_module.py::test_new_feature -v  # MUST fail
   git add tests/ && git commit -m "TDD RED: Add test for feature X"
   ```

2. **🟢 GREEN - BABY STEPS** (critical - TDD guard enforces this):

   **Step 2a**: Add MINIMAL stub (just method signature)
   ```python
   def new_method(self):
       """Stub - not implemented yet"""
       raise NotImplementedError
   ```
   Run test → Should fail differently

   **Step 2b**: Add MINIMAL implementation
   ```python
   def new_method(self):
       """Minimal implementation to pass test"""
       return []  # Simplest return value
   ```
   Run test → May still fail on assertions

   **Step 2c**: Iterate until GREEN
   ```bash
   uv run pytest tests/test_module.py::test_new_feature -v  # Should pass
   git add . && git commit -m "TDD GREEN: Implement feature X"
   ```

3. **♻️ REFACTOR**: Clean up with tests passing
   ```bash
   uv run pytest  # All tests still pass
   git add . && git commit -m "TDD REFACTOR: Clean up feature X"
   ```

**⚠️ TDD Guard Rules** (enforced automatically):
- ❌ **NEVER** implement without failing test first
- ❌ **NEVER** add multiple tests at once (one test at a time)
- ✅ **ALWAYS** run pytest immediately before AND after each edit
- ✅ **ALWAYS** implement smallest possible change

---

### When Stuck Protocol

**CRITICAL**: Maximum **3 attempts** per issue, then STOP.

#### After 3 Failed Attempts:

1. **Document failure** in issue/PR
2. **Research alternatives** (15min max) - Find 2-3 similar implementations
3. **Question fundamentals**: Is this the right approach? Can it be split?
4. **Try different angle OR ask for help**

**Never**: Keep trying the same approach >3 times.

---

### Decision Framework

When multiple valid approaches exist, choose based on **priority order**:

1. **Testability** → Can I easily test this?
2. **Simplicity** → Is this the simplest solution that works?
3. **Consistency** → Does this match existing project patterns?
4. **Readability** → Will someone understand this in 6 months?
5. **Reversibility** → How hard to change later?

---

### Error Handling Standards

**Principles**:
- **Fail fast** with descriptive messages
- **Include context** for debugging
- **Never silently swallow** exceptions

**Good Error Messages**:
```python
# ❌ Bad
raise ValueError("Invalid input")

# ✅ Good
raise ValueError(
    f"DuckDB connection failed: {db_path} "
    f"(check file exists and has read permissions)"
)
```

---

### Test Guidelines

**Principles**:
- Test **behavior**, not implementation
- **One assertion** per test when possible
- **Clear test names**: `test_<what>_<when>_<expected>`
- Tests must be **deterministic** (no random, no time dependencies)

**Good Test Structure**:
```python
def test_liquidation_calculation_returns_correct_price_when_leverage_10x():
    """Liquidation price should be 90% of entry for 10x long position."""
    # Arrange
    entry_price = 100.0
    leverage = 10

    # Act
    liq_price = calculate_liquidation_price(entry_price, leverage, "long")

    # Assert
    assert liq_price == 90.0
```

---

## Agent & Skill Architecture

### **Subagents** - Complex Reasoning
Specialized agents for deep domain expertise and multi-step workflows.

| Agent | Responsibility | Key Tasks |
|-------|---------------|-----------|
| **data-engineer** | DuckDB ingestion, ETL pipelines | CSV → DuckDB, schema design, data quality checks |
| **quant-analyst** | Liquidation models, heatmaps, backtesting | Formula implementation, clustering algorithms, strategy validation |

**Full specs**: See `.claude/agents/` directory

**Usage**: Invoke via Claude Code for complex implementation tasks.

### **Skills** - Template-Driven Automation
Lightweight templates for repetitive operations with 60-83% token savings.

| Skill | Purpose | Token Savings | Status |
|-------|---------|---------------|--------|
| pytest-test-generator | Auto-generate test boilerplate | 83% (3,000→500) | ✅ |
| github-workflow | PR/Issue/Commit templates | 79% (18,900→4,000) | ✅ |
| pydantic-model-generator | Pydantic data models with validators | 75% (2,000→500) | ✅ |

**Usage**: Automatically triggered by keywords (see `.claude/skills/*/SKILL.md`)

---

## Important Reminders

### ❌ **NEVER**:
- Use `--no-verify` to bypass commit hooks
- Disable tests instead of fixing them
- Commit code that doesn't compile/run
- Hardcode secrets/API keys (use `.env`)
- Commit without testing locally first

### ✅ **ALWAYS**:
- Run tests before committing (`uv run pytest`)
- Format/lint before committing (`ruff check . && ruff format .`)
- Write commit message explaining **WHY** (not just what)
- Update relevant docs when changing behavior
- Use `uv` for dependencies (not `pip`)

---

## 🧹 Task Completion Protocol

**Before marking ANY task complete or creating commit**:

### ✅ Pre-Commit Cleanup Checklist

1. **Remove temporary files**: `find . -name "*.tmp" -o -name "*.bak"`
2. **Clean Python cache**: `find . -type d -name "__pycache__" -exec rm -rf {} +`
3. **Remove debug code**: Check for `print()`, `console.log`, `import pdb`
4. **Run linter**: `ruff check . && ruff format .`
5. **Run tests**: `uv run pytest`
6. **Check git status**: No uncommitted temp files

### 🗑️ What to DELETE vs KEEP

**DELETE**:
- Temporary files (`.tmp`, `.bak`)
- Python cache (`__pycache__`, `.pyc`)
- Debug logs
- Commented code blocks >1 week old
- Unused imports

**KEEP**:
- Source code in `src/`, `tests/`, `scripts/`
- Configuration files
- Documentation (if referenced)
- `uv.lock` (dependency lockfile - COMMIT THIS!)

---

## Known Models & References

**Leverage existing open-source work** (KISS - don't reinvent):

1. **py-liquidation-map** (GitHub: aoki-h-jp/py-liquidation-map)
   - Liquidation clustering algorithm
   - Heatmap visualization approach
   - Supports Binance + Bybit data

2. **binance-liquidation-tracker** (GitHub: hgnx/binance-liquidation-tracker)
   - Real-time WebSocket tracking
   - Forced liquidation monitoring

3. **Coinglass formulas** (industry standard)
   - Liquidation heatmap calculations
   - Open Interest analysis

4. **Binance Official Docs**
   - Liquidation formula: https://www.binance.com/en/support/faq/liquidation
   - Maintenance margin rates (varies by leverage)

**Strategy**: Use py-liquidation-map formulas as reference implementation, adapt for DuckDB vectorized calculations.
