#!/bin/sh
# N8N wrapper script for Open Interest DuckDB ingest
# Usage: ./n8n_oi_wrapper.sh START_DATE END_DATE
# Handles lock cleanup and API coordination
# Ingests Open Interest for BTCUSDT and ETHUSDT

START_DATE="$1"
END_DATE="$2"

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
PROJECT_DIR=$(CDPATH= cd -- "$SCRIPT_DIR/.." && pwd)
. "$PROJECT_DIR/scripts/lib/runtime_env.sh"
lh_load_runtime_env container

cd "$PROJECT_DIR"

echo ""
echo "=========================================="
echo "Open Interest Ingestion Wrapper"
echo "=========================================="
echo "Date range: $START_DATE -> $END_DATE"
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
sleep 2

MAX_RETRIES=3
RETRY_DELAY=5

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
            ps aux | grep -E "python.*duckdb|ingest" | grep -v grep || true
            exit 1
        fi
        echo "Database locked, waiting ${RETRY_DELAY}s (attempt $attempt/$MAX_RETRIES)..."
        sleep $RETRY_DELAY
        attempt=$((attempt + 1))
    fi
done

echo ""
echo "[Step 4/4] Running Open Interest ingestion..."

# Track status
OI_BTC=0
OI_ETH=0

echo ""
echo "[1/2] Ingesting BTCUSDT Open Interest..."
python3 scripts/ingest_oi.py \
    --symbol BTCUSDT \
    --start-date "$START_DATE" \
    --end-date "$END_DATE" \
    --data-dir "$DATA_DIR" \
    --db "$DB_PATH" \
    --throttle-ms 100
OI_BTC=$?

echo ""
echo "[2/2] Ingesting ETHUSDT Open Interest..."
python3 scripts/ingest_oi.py \
    --symbol ETHUSDT \
    --start-date "$START_DATE" \
    --end-date "$END_DATE" \
    --data-dir "$DATA_DIR" \
    --db "$DB_PATH" \
    --throttle-ms 100
OI_ETH=$?

# Notify API to refresh connections
echo ""
echo "Refreshing API connections..."
REFRESH_RESULT=$(wget -q -O - --timeout=10 --post-data="" "${API_URL}/api/v1/refresh-connections" 2>/dev/null || echo '{"status":"api_unavailable"}')
echo "API refresh: ${REFRESH_RESULT}"

echo ""
echo "=========================================="
echo "Open Interest Ingestion Summary"
echo "=========================================="
FAILED=0

if [ $OI_BTC -eq 0 ]; then
    echo "BTCUSDT OI: Success"
else
    echo "BTCUSDT OI: Failed"
    FAILED=$((FAILED + 1))
fi

if [ $OI_ETH -eq 0 ]; then
    echo "ETHUSDT OI: Success"
else
    echo "ETHUSDT OI: Failed"
    FAILED=$((FAILED + 1))
fi

if [ $FAILED -gt 0 ]; then
    echo ""
    echo "$FAILED task(s) failed"
    exit 1
fi

echo ""
echo "Open Interest ingestion complete!"
