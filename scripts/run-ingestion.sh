#!/bin/bash
# Standalone DuckDB ingestion orchestrator
# Replaces the N8N workflow ingestion phase (PuSb49WQ8ypsOGEX + sub-workflow h4ZlTsDp5Fz1tMXR)
#
# Runs after binance-sync.service downloads fresh CSV data.
# Handles: aggTrades, funding rate, open interest, klines (5m, 15m)
# For both BTCUSDT and ETHUSDT.
#
# Usage:
#   ./run-ingestion.sh                # Daily incremental (default)
#   ./run-ingestion.sh --full         # Full history reload
#   ./run-ingestion.sh --dry-run      # Validate only, no writes
#
# Designed to run via systemd timer (lh-ingestion.timer) or manually.

set -euo pipefail

SCRIPT_DIR="$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)"
. "$SCRIPT_DIR/lib/runtime_env.sh"
lh_load_runtime_env host

# =============================================================================
# Configuration
# =============================================================================

# Retry settings
MAX_RETRIES=3
RETRY_DELAY=5

# Date calculations
TODAY=$(date +%Y-%m-%d)
YESTERDAY=$(date -d "yesterday" +%Y-%m-%d)
WEEK_AGO=$(date -d "7 days ago" +%Y-%m-%d)
CURRENT_MONTH=$(date +%Y-%m)
THREE_MONTHS_AGO=$(date -d "3 months ago" +%Y-%m)
FULL_START="2021-12-01"

# Parse arguments
MODE="daily"
for arg in "$@"; do
    case "$arg" in
        --full) MODE="full" ;;
        --dry-run) MODE="dry-run" ;;
        --help)
            echo "Usage: $0 [--full|--dry-run]"
            echo "  (default)   Daily incremental ingestion (last 7 days)"
            echo "  --full      Full history reload from 2021-12-01"
            echo "  --dry-run   Validate only, no writes"
            exit 0
            ;;
    esac
done

# =============================================================================
# Logging setup
# =============================================================================

mkdir -p "$LOG_DIR"
LOGFILE="${LOG_DIR}/ingestion_$(date +%Y%m%d_%H%M%S).log"

# Log to both stdout and file
exec > >(tee -a "$LOGFILE") 2>&1

log() { echo "[$(date -Iseconds)] $1"; }
log_section() {
    echo ""
    echo "=========================================="
    echo "$1"
    echo "=========================================="
}

# =============================================================================
# Discord notification
# =============================================================================

send_discord() {
    local title="$1"
    local description="$2"
    local color="$3"  # decimal: green=3066993, red=15158332, yellow=16776960

    # Load webhook URL from dotenvx (safe - value never printed)
    local webhook_url
    webhook_url=$(dotenvx get DISCORD_WEBHOOK_LIQUIDATION -f "$SHARED_ENV_FILE" 2>/dev/null || echo "")

    if [ -z "$webhook_url" ]; then
        log "WARN: DISCORD_WEBHOOK_URL not available, skipping notification"
        return 0
    fi

    local payload
    payload=$(cat <<EOJSON
{
  "embeds": [{
    "title": "${title}",
    "description": "${description}",
    "color": ${color},
    "footer": {"text": "LH Ingestion | $(hostname)"},
    "timestamp": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  }]
}
EOJSON
)

    curl -s -H "Content-Type: application/json" -d "$payload" "$webhook_url" > /dev/null 2>&1 || true
}

# =============================================================================
# DB lock management
# =============================================================================

test_db_write_access() {
    uv run --project "$PROJECT_DIR" python -c "
import duckdb, sys
try:
    conn = duckdb.connect('${DB_PATH}', read_only=False)
    conn.close()
    sys.exit(0)
except Exception as e:
    print(f'Lock test failed: {e}', file=sys.stderr)
    sys.exit(1)
" 2>/dev/null
    return $?
}

prepare_database() {
    log_section "Preparing database"

    # Step 1: Notify API to release connections (non-blocking if API is down)
    log "Notifying API to release DuckDB connections..."
    local token_header=""
    if [ -n "${REKTSLUG_INTERNAL_TOKEN:-}" ]; then
        token_header="-H X-Internal-Token:${REKTSLUG_INTERNAL_TOKEN}"
    fi
    local prep_result
    prep_result=$(curl -s --max-time 10 $token_header -X POST "${API_URL}/api/v1/prepare-for-ingestion" 2>/dev/null || echo '{"status":"api_unavailable"}')
    log "API preparation: ${prep_result}"

    # Step 2: Clean stale locks
    log "Cleaning stale DuckDB locks..."
    uv run --project "$PROJECT_DIR" python "${PROJECT_DIR}/scripts/cleanup_duckdb_locks.py" "${DB_PATH}" || true

    # Step 3: Wait for DB availability
    sleep 2
    local attempt=1
    while [ $attempt -le $MAX_RETRIES ]; do
        if test_db_write_access; then
            log "Database write access confirmed"
            return 0
        else
            if [ $attempt -eq $MAX_RETRIES ]; then
                log "SKIPPED_LOCK_CONTENTION: Cannot acquire database write lock after $MAX_RETRIES attempts"
                ps aux | grep -E "python.*duckdb|ingest" | grep -v grep || true
                return 1
            fi
            log "Database locked, waiting ${RETRY_DELAY}s (attempt $attempt/$MAX_RETRIES)..."
            sleep $RETRY_DELAY
            attempt=$((attempt + 1))
        fi
    done
}

refresh_api_connections() {
    log "Refreshing API connections..."
    local token_header=""
    if [ -n "${REKTSLUG_INTERNAL_TOKEN:-}" ]; then
        token_header="-H X-Internal-Token:${REKTSLUG_INTERNAL_TOKEN}"
    fi
    local result
    result=$(curl -s --max-time 10 $token_header -X POST "${API_URL}/api/v1/refresh-connections" 2>/dev/null || echo '{"status":"api_unavailable"}')
    log "API refresh: ${result}"
}

# =============================================================================
# Ingestion tasks
# =============================================================================

ingest_aggtrades() {
    log_section "AggTrades Ingestion"
    local failed=0

    for symbol in $SYMBOLS; do
        log "Ingesting ${symbol} aggTrades..."
        if [ "$MODE" = "full" ]; then
            uv run --project "$PROJECT_DIR" python "${PROJECT_DIR}/ingest_full_history_n8n.py" \
                --symbol "$symbol" \
                --data-dir "$DATA_DIR" \
                --db "$DB_PATH" \
                --mode full \
                --start-date "$FULL_START" \
                --end-date "$YESTERDAY" \
                --throttle-ms 200 || { log "FAILED: ${symbol} aggTrades"; failed=$((failed + 1)); }
        elif [ "$MODE" = "dry-run" ]; then
            uv run --project "$PROJECT_DIR" python "${PROJECT_DIR}/ingest_full_history_n8n.py" \
                --symbol "$symbol" \
                --data-dir "$DATA_DIR" \
                --db "$DB_PATH" \
                --mode dry-run \
                --throttle-ms 200 || { log "FAILED: ${symbol} aggTrades (dry-run)"; failed=$((failed + 1)); }
        else
            # Daily incremental: ingest recent window explicitly so trailing
            # days are imported even when there are no "internal" DB gaps.
            uv run --project "$PROJECT_DIR" python "${PROJECT_DIR}/ingest_full_history_n8n.py" \
                --symbol "$symbol" \
                --data-dir "$DATA_DIR" \
                --db "$DB_PATH" \
                --mode full \
                --start-date "$WEEK_AGO" \
                --end-date "$YESTERDAY" \
                --throttle-ms 200 || { log "FAILED: ${symbol} aggTrades"; failed=$((failed + 1)); }
        fi
    done

    return $failed
}

ingest_funding_rate() {
    log_section "Funding Rate Ingestion"
    local failed=0

    if [ "$MODE" = "dry-run" ]; then
        log "Skipping funding rate (dry-run mode)"
        return 0
    fi

    local start_month end_month
    if [ "$MODE" = "full" ]; then
        start_month="2020-01"
        end_month="$CURRENT_MONTH"
    else
        # Daily: last 3 months (idempotent, covers gaps)
        start_month="$THREE_MONTHS_AGO"
        end_month="$CURRENT_MONTH"
    fi

    for symbol in $SYMBOLS; do
        log "Ingesting ${symbol} funding rate ($start_month -> $end_month)..."
        uv run --project "$PROJECT_DIR" python "${PROJECT_DIR}/scripts/ingest_funding_rate.py" \
            --symbol "$symbol" \
            --start-month "$start_month" \
            --end-month "$end_month" \
            --data-dir "$DATA_DIR" \
            --db "$DB_PATH" \
            --throttle-ms 100 || { log "FAILED: ${symbol} funding rate"; failed=$((failed + 1)); }
    done

    return $failed
}

ingest_open_interest() {
    log_section "Open Interest Ingestion"
    local failed=0

    if [ "$MODE" = "dry-run" ]; then
        log "Skipping open interest (dry-run mode)"
        return 0
    fi

    local start_date end_date
    if [ "$MODE" = "full" ]; then
        start_date="$FULL_START"
        end_date="$YESTERDAY"
    else
        # Daily: last 7 days
        start_date="$WEEK_AGO"
        end_date="$YESTERDAY"
    fi

    for symbol in $SYMBOLS; do
        log "Ingesting ${symbol} Open Interest ($start_date -> $end_date)..."
        uv run --project "$PROJECT_DIR" python "${PROJECT_DIR}/scripts/ingest_oi.py" \
            --symbol "$symbol" \
            --start-date "$start_date" \
            --end-date "$end_date" \
            --data-dir "$DATA_DIR" \
            --db "$DB_PATH" \
            --throttle-ms 100 || { log "FAILED: ${symbol} OI"; failed=$((failed + 1)); }
    done

    return $failed
}

ingest_klines() {
    log_section "Klines Ingestion (5m + 15m)"
    local failed=0

    if [ "$MODE" = "dry-run" ]; then
        log "Skipping klines (dry-run mode)"
        return 0
    fi

    local start_date end_date
    if [ "$MODE" = "full" ]; then
        start_date="$FULL_START"
        end_date="$YESTERDAY"
    else
        # Daily: last 7 days
        start_date="$WEEK_AGO"
        end_date="$YESTERDAY"
    fi

    for symbol in $SYMBOLS; do
        for interval in 5m 15m; do
            log "Ingesting ${symbol} ${interval} klines ($start_date -> $end_date)..."
            uv run --project "$PROJECT_DIR" python "${PROJECT_DIR}/scripts/ingest_klines_15m.py" \
                --symbol "$symbol" \
                --start-date "$start_date" \
                --end-date "$end_date" \
                --interval "$interval" \
                --data-dir "$DATA_DIR" \
                --db "$DB_PATH" \
                --throttle-ms 200 || { log "FAILED: ${symbol} ${interval} klines"; failed=$((failed + 1)); }
        done
    done

    return $failed
}

ingest_metrics() {
    log_section "Metrics Ingestion (long/short ratio, taker volume)"
    local failed=0

    if [ "$MODE" = "dry-run" ]; then
        log "Skipping metrics (dry-run mode)"
        return 0
    fi

    local start_date end_date
    if [ "$MODE" = "full" ]; then
        start_date="$FULL_START"
        end_date="$YESTERDAY"
    else
        start_date="$WEEK_AGO"
        end_date="$YESTERDAY"
    fi

    for symbol in $SYMBOLS; do
        log "Ingesting ${symbol} metrics ($start_date -> $end_date)..."
        uv run --project "$PROJECT_DIR" python "${PROJECT_DIR}/scripts/ingest_metrics.py" \
            --symbol "$symbol" \
            --start-date "$start_date" \
            --end-date "$end_date" \
            --data-dir "$DATA_DIR" \
            --db "$DB_PATH" || { log "FAILED: ${symbol} metrics"; failed=$((failed + 1)); }
    done

    return $failed
}

fill_gap_from_ccxt() {
    log_section "Gap Fill from ccxt-data-pipeline (T-2 -> T-0)"

    local dry_run_param=""
    if [ "$MODE" = "dry-run" ]; then
        dry_run_param="?dry_run=true"
        log "Running gap fill in dry-run mode"
    fi

    run_gap_fill_cli_fallback() {
        local reason="$1"
        local -a symbol_args=()
        local -a cmd=()
        local symbol

        for symbol in $SYMBOLS; do
            symbol_args+=("$symbol")
        done

        log "Gap fill API unavailable (${reason}), falling back to direct CLI execution"
        cmd=(
            uv run --project "$PROJECT_DIR" python "${PROJECT_DIR}/scripts/fill_gap_from_ccxt.py"
            --symbols "${symbol_args[@]}"
            --ccxt-catalog "$CCXT_CATALOG"
            --db "$DB_PATH"
        )
        if [ "$MODE" = "dry-run" ]; then
            cmd+=(--dry-run)
        fi
        "${cmd[@]}"
    }

    # Delegate to in-process API endpoint (avoids DuckDB cross-process lock)
    local token_header=""
    if [ -n "${REKTSLUG_INTERNAL_TOKEN:-}" ]; then
        token_header="-H X-Internal-Token:${REKTSLUG_INTERNAL_TOKEN}"
    fi

    local response
    response=$(curl -s -w "\n%{http_code}" --max-time 120 \
        $token_header \
        -X POST "${API_URL}/api/v1/gap-fill${dry_run_param}" 2>/dev/null || true)

    local http_code body
    http_code=$(echo "$response" | tail -1)
    body=$(echo "$response" | sed '$d')

    if [ "$http_code" = "200" ]; then
        log "Gap fill via API: $body"
        return 0
    elif [ "$http_code" = "409" ]; then
        log "SKIPPED_LOCK_CONTENTION: Gap fill already in progress, skipping"
        return 0
    elif [ "$http_code" = "500" ] && echo "$body" | grep -qiE "Could not set lock|Conflicting lock"; then
        log "SKIPPED_LOCK_CONTENTION: Gap fill API lock contention, skipping"
        return 0
    elif [ "$http_code" = "404" ] || [ "$http_code" = "405" ] || [ "$http_code" = "000" ] || [ -z "$http_code" ]; then
        log "Gap fill endpoint not available (HTTP ${http_code}): ${body:-no response body}"
        if run_gap_fill_cli_fallback "http_${http_code:-unknown}"; then
            return 0
        fi
        log "FAILED: Gap fill fallback CLI failed"
        return 1
    else
        log "FAILED: Gap fill (HTTP ${http_code}): $body"
        return 1
    fi
}

# =============================================================================
# Main execution
# =============================================================================

START_TIME=$(date +%s)

log_section "LiquidationHeatmap DuckDB Ingestion"
log "Mode: ${MODE}"
log "Symbols: ${SYMBOLS}"
log "DB: ${DB_PATH}"
log "Data: ${DATA_DIR}"
log "Log: ${LOGFILE}"

# Send start notification
send_discord \
    "DuckDB Ingestion Started (${MODE})" \
    "Symbols: ${SYMBOLS}\nMode: ${MODE}" \
    16776960  # yellow

# Prepare database (lock cleanup, API coordination)
if ! prepare_database; then
    send_discord \
        "DuckDB Ingestion FAILED" \
        "Cannot acquire database lock" \
        15158332  # red
    exit 1
fi

# Track results
TOTAL_FAILED=0
RESULTS=""

# Phase 1: AggTrades
set +e
ingest_aggtrades
AT_FAILED=$?
TOTAL_FAILED=$((TOTAL_FAILED + AT_FAILED))
if [ $AT_FAILED -eq 0 ]; then
    RESULTS="${RESULTS}AggTrades: OK\n"
else
    RESULTS="${RESULTS}AggTrades: ${AT_FAILED} failed\n"
fi

# Phase 2: Funding Rate
ingest_funding_rate
FR_FAILED=$?
TOTAL_FAILED=$((TOTAL_FAILED + FR_FAILED))
if [ $FR_FAILED -eq 0 ]; then
    RESULTS="${RESULTS}Funding Rate: OK\n"
else
    RESULTS="${RESULTS}Funding Rate: ${FR_FAILED} failed\n"
fi

# Phase 3: Open Interest
ingest_open_interest
OI_FAILED=$?
TOTAL_FAILED=$((TOTAL_FAILED + OI_FAILED))
if [ $OI_FAILED -eq 0 ]; then
    RESULTS="${RESULTS}Open Interest: OK\n"
else
    RESULTS="${RESULTS}Open Interest: ${OI_FAILED} failed\n"
fi

# Phase 4: Klines
ingest_klines
KL_FAILED=$?
TOTAL_FAILED=$((TOTAL_FAILED + KL_FAILED))
if [ $KL_FAILED -eq 0 ]; then
    RESULTS="${RESULTS}Klines (5m+15m): OK\n"
else
    RESULTS="${RESULTS}Klines: ${KL_FAILED} failed\n"
fi

# Phase 5: Metrics (long/short ratio, taker volume)
ingest_metrics
MT_FAILED=$?
TOTAL_FAILED=$((TOTAL_FAILED + MT_FAILED))
if [ $MT_FAILED -eq 0 ]; then
    RESULTS="${RESULTS}Metrics: OK\n"
else
    RESULTS="${RESULTS}Metrics: ${MT_FAILED} failed\n"
fi
# Phase 6: Gap Fill from ccxt-data-pipeline
fill_gap_from_ccxt
GF_FAILED=$?
TOTAL_FAILED=$((TOTAL_FAILED + GF_FAILED))
if [ $GF_FAILED -eq 0 ]; then
    RESULTS="${RESULTS}Gap Fill (ccxt): OK\n"
else
    RESULTS="${RESULTS}Gap Fill (ccxt): FAILED\n"
fi
set -e

# Refresh API connections
refresh_api_connections

# Calculate duration
END_TIME=$(date +%s)
DURATION=$((END_TIME - START_TIME))
DURATION_MIN=$((DURATION / 60))
DURATION_SEC=$((DURATION % 60))

# Summary
log_section "Ingestion Summary"
echo -e "$RESULTS"
log "Duration: ${DURATION_MIN}m ${DURATION_SEC}s"
log "Total failures: ${TOTAL_FAILED}"

# Send completion notification
if [ $TOTAL_FAILED -eq 0 ]; then
    send_discord \
        "DuckDB Ingestion Complete (${MODE})" \
        "${RESULTS}Duration: ${DURATION_MIN}m ${DURATION_SEC}s" \
        3066993  # green
    log "All ingestion tasks completed successfully"
    exit 0
else
    send_discord \
        "DuckDB Ingestion Partial Failure (${MODE})" \
        "${RESULTS}Failures: ${TOTAL_FAILED}\nDuration: ${DURATION_MIN}m ${DURATION_SEC}s" \
        15158332  # red
    log "WARNING: ${TOTAL_FAILED} task(s) failed"
    exit 1
fi
