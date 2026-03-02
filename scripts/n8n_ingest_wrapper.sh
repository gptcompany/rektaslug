#!/bin/sh
# N8N wrapper script for DuckDB ingest
# Usage: ./n8n_ingest_wrapper.sh START_DATE END_DATE MODE
# Handles lock cleanup and API coordination

START_DATE="$1"
END_DATE="$2"
MODE="$3"

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
. "$PROJECT_DIR/scripts/lib/runtime_env.sh"
lh_load_runtime_env container
MAX_RETRIES=3
RETRY_DELAY=5

cd "$PROJECT_DIR"

echo ""
echo "=========================================="
echo "DuckDB AggTrades Ingestion Wrapper"
echo "=========================================="
echo "Date range: $START_DATE -> $END_DATE"
echo "Mode: $MODE"
echo ""

# Step 1: Notify API to release connections
echo "[Step 1/4] Preparing database for ingestion..."
PREP_RESULT=$(wget -q -O - --timeout=10 --post-data="" "${API_URL}/api/v1/prepare-for-ingestion" 2>/dev/null || echo '{"status":"api_unavailable"}')
echo "API preparation: ${PREP_RESULT}"

# Step 2: Clean any stale locks
echo ""
echo "[Step 2/4] Cleaning stale locks..."
python3 "${PROJECT_DIR}/scripts/cleanup_duckdb_locks.py" "${DB_PATH}" || true

# Step 3: Wait for database to be available
echo ""
echo "[Step 3/4] Checking database availability..."

# Small delay to let DuckDB release internal locks after API disconnects
sleep 2

# Function to test actual DuckDB write access
test_db_write_access() {
    python3 -c "
import duckdb
import sys
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

attempt=1
while [ $attempt -le $MAX_RETRIES ]; do
    if test_db_write_access; then
        echo "Database write access confirmed"
        break
    else
        if [ $attempt -eq $MAX_RETRIES ]; then
            echo "ERROR: Cannot acquire database write lock after $MAX_RETRIES attempts"
            echo "Checking for blocking processes..."
            ps aux | grep -E "python.*duckdb|ingest" | grep -v grep || true
            exit 1
        fi
        echo "Database locked, waiting ${RETRY_DELAY}s (attempt $attempt/$MAX_RETRIES)..."
        sleep $RETRY_DELAY
        attempt=$((attempt + 1))
    fi
done

echo "Database available, starting ingestion..."

# Track overall status
BTCUSDT_STATUS=0
ETHUSDT_STATUS=0

if [ "$MODE" = "PRODUCTION" ]; then
    echo ""
    echo "Ingesting BTCUSDT aggTrades..."
    python3 ingest_full_history_n8n.py \
        --symbol BTCUSDT \
        --data-dir "$DATA_DIR" \
        --db "$DB_PATH" \
        --mode full \
        --start-date "$START_DATE" \
        --end-date "$END_DATE" \
        --throttle-ms 200
    BTCUSDT_STATUS=$?

    echo ""
    echo "Ingesting ETHUSDT aggTrades..."
    python3 ingest_full_history_n8n.py \
        --symbol ETHUSDT \
        --data-dir "$DATA_DIR" \
        --db "$DB_PATH" \
        --mode full \
        --start-date "$START_DATE" \
        --end-date "$END_DATE" \
        --throttle-ms 200
    ETHUSDT_STATUS=$?
else
    echo ""
    echo "Ingesting BTCUSDT aggTrades (auto mode)..."
    python3 ingest_full_history_n8n.py \
        --symbol BTCUSDT \
        --data-dir "$DATA_DIR" \
        --db "$DB_PATH" \
        --mode auto \
        --throttle-ms 200
    BTCUSDT_STATUS=$?

    echo ""
    echo "Ingesting ETHUSDT aggTrades (auto mode)..."
    python3 ingest_full_history_n8n.py \
        --symbol ETHUSDT \
        --data-dir "$DATA_DIR" \
        --db "$DB_PATH" \
        --mode auto \
        --throttle-ms 200
    ETHUSDT_STATUS=$?
fi

# Step 4: Notify API to refresh connections
echo ""
echo "[Step 4/4] Refreshing API connections..."
REFRESH_RESULT=$(wget -q -O - --timeout=10 --post-data="" "${API_URL}/api/v1/refresh-connections" 2>/dev/null || echo '{"status":"api_unavailable"}')
echo "API refresh: ${REFRESH_RESULT}"

echo ""
echo "=========================================="
echo "AggTrades Ingestion Summary"
echo "=========================================="
if [ $BTCUSDT_STATUS -eq 0 ]; then
    echo "BTCUSDT: Success"
else
    echo "BTCUSDT: Failed (exit code: $BTCUSDT_STATUS)"
fi
if [ $ETHUSDT_STATUS -eq 0 ]; then
    echo "ETHUSDT: Success"
else
    echo "ETHUSDT: Failed (exit code: $ETHUSDT_STATUS)"
fi

# Exit with error if any symbol failed
if [ $BTCUSDT_STATUS -ne 0 ] || [ $ETHUSDT_STATUS -ne 0 ]; then
    exit 1
fi

echo ""
echo "Orchestration complete!"
