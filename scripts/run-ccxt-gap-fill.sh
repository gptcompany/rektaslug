#!/bin/bash
# Near-real-time bridge from ccxt-data-pipeline Parquet catalog into DuckDB.

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
. "$SCRIPT_DIR/lib/runtime_env.sh"
lh_load_runtime_env host

log() { echo "[$(date -Iseconds)] $1"; }

api_post() {
    local endpoint="$1"
    curl -s --max-time 10 -X POST "${API_URL}${endpoint}" >/dev/null 2>&1 || true
}

test_db_write_access() {
    uv run --project "$PROJECT_DIR" python -c "
import duckdb, sys
try:
    conn = duckdb.connect('${DB_PATH}', read_only=False)
    conn.close()
    sys.exit(0)
except Exception:
    sys.exit(1)
" 2>/dev/null
}

if [ ! -d "$CCXT_CATALOG" ]; then
    log "CCXT catalog not found at $CCXT_CATALOG, skipping"
    exit 0
fi

log "Starting ccxt gap fill"
log "Catalog: $CCXT_CATALOG"
log "DB: $DB_PATH"
log "Symbols: $SYMBOLS"

api_post "/api/v1/prepare-for-ingestion"
uv run --project "$PROJECT_DIR" python "$PROJECT_DIR/scripts/cleanup_duckdb_locks.py" "$DB_PATH" || true

if ! test_db_write_access; then
    log "DuckDB busy, skipping this run"
    api_post "/api/v1/refresh-connections"
    exit 0
fi

uv run --project "$PROJECT_DIR" python "$PROJECT_DIR/scripts/fill_gap_from_ccxt.py" \
    --symbols $SYMBOLS \
    --ccxt-catalog "$CCXT_CATALOG" \
    --db "$DB_PATH" || status=$?

api_post "/api/v1/refresh-connections"
status="${status:-0}"
if [ "$status" -ne 0 ]; then
    log "ccxt gap fill failed"
    exit "$status"
fi

log "ccxt gap fill complete"
